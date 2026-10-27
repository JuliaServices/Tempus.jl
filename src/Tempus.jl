"""
Tempus provides a cron-style job scheduling framework for Julia, inspired by Quartz in Java. 

## Features:
- Define jobs with cron-like scheduling expressions
- Supports job execution policies (overlap handling, retries, and failure strategies)
- Pluggable persistent state via AbstractStores.jl backends (memory, file, SQL, Redis)
- Concurrency-aware execution with configurable retry logic
- Supports disabling, enabling, and unscheduling jobs dynamically
- Thread-safe scheduling with a background execution loop
"""
module Tempus

using Dates, JSON, Logging, TimeZones
import AbstractStores

export withscheduler

include("cron.jl")

_some(x, y...) = x === nothing ? _some(y...) : x
_some(x) = x

# Job actions, retry checks, and timezone schedule lookups are stored behind one
# concrete type each, so the scheduler calls them without dynamic dispatch (which binaries compiled with
# `juliac --trim` do not have). A `Callback{C}` holds a `@cfunction` pointer
# specialized on the callable's type plus a pointer to the callable; calling it
# is a fixed-signature `ccall` that passes a concrete `C` record carrying the
# arguments and the result. Calls through the pointer run in the latest world.
#
# The type is mutable because Julia stores the `Ptr` fields of mutable objects
# as `C_NULL` in package images: a callback built during precompilation then
# fails with an error instead of calling through a stale pointer.
mutable struct Callback{C}
    ptr::Ptr{Cvoid}
    objptr::Ptr{Cvoid}
    const root::Any # keeps the callable alive; never dispatched on
end

struct CallbackWrapper{C} <: Function end

function (::CallbackWrapper{C})(f::F, callptr::Ptr{Cvoid}) where {C, F}
    invokecallback!(f, unsafe_pointer_to_objref(callptr)::C)
    return nothing
end

@generated function callbackptr(::Type{C}, ::Type{F}) where {C, F}
    return :(@cfunction($(CallbackWrapper{C}()), Cvoid, (Ref{$F}, Ptr{Cvoid})))
end

function Callback{C}(f::F) where {C, F}
    root = Base.cconvert(Ref{F}, f)
    objptr = Ptr{Cvoid}(Base.unsafe_convert(Ref{F}, root))
    return Callback{C}(callbackptr(C, F), objptr, root)
end

Callback{C}(f::Callback{C}) where {C} = f

function (f::Callback{C})(call::C) where {C}
    f.ptr == C_NULL && throw(ArgumentError(
        "this Tempus job or job option was created during precompilation; create it at run time"))
    GC.@preserve f call begin
        ccall(f.ptr, Cvoid, (Ptr{Cvoid}, Ptr{Cvoid}), f.objptr, pointer_from_objref(call))
    end
    return call
end

# a job action call: `action()`
mutable struct ActionCall
    result::Any
    ActionCall() = new(nothing)
end

invokecallback!(f, call::ActionCall) = (call.result = f(); nothing)

const JobAction = Callback{ActionCall}

(action::JobAction)() = action(ActionCall()).result

# a retry check call: `retry_check(state, exception)::Bool`, where `state` is
# the `Base.ExponentialBackOff` iteration state `Base.retry` passes
mutable struct RetryCheckCall
    const state::Tuple{Int, Float64}
    const exception::Any
    retry::Bool
    RetryCheckCall(state, exception) = new(state, exception, false)
end

invokecallback!(f, call::RetryCheckCall) = (call.retry = f(call.state, call.exception)::Bool; nothing)

# a timezone-aware schedule lookup: `getnext(cron, timezone, from)`. Kept behind
# a callback so TimeZones' database loading, which is not trim-safe, is only
# compiled into programs that create jobs with a `timezone`.
mutable struct NextTimeCall
    const cron::Cron
    const from::DateTime
    next::DateTime
    NextTimeCall(cron, from) = new(cron, from)
end

invokecallback!(f, call::NextTimeCall) = (call.next = f(call.cron, call.from)::DateTime; nothing)

function zonecallback(timezone::String)
    TimeZones.istimezone(timezone) || throw(ArgumentError("unknown timezone: $(repr(timezone))"))
    return Callback{NextTimeCall}((cron, from) -> getnext(cron, timezone, from))
end

"""
    JobOptions

Defines options for job execution behavior.

# Fields:
- `overlap_policy::Union{Symbol, Nothing}`: Determines job execution behavior when the same job is already running (`:skip`, `:queue`, `:concurrent`).
- `retries::Int`: Number of retries allowed on failure.
- `retry_delays::Union{Base.ExponentialBackOff, Nothing}`: Delay strategy for retries (defaults to exponential backoff if `retries > 0`).
- `retry_check`: Custom function `(state, exception) -> Bool` deciding whether a failed attempt is retried (the `check` argument of `Base.retry`); stored as an internal callback wrapper. In a binary compiled with `juliac --trim`, declare its exception argument `@nospecialize`.
- `max_failed_executions::Union{Int, Nothing}`: Number of consecutive failed executions that disables a job.
- `max_executions::Union{Int, Nothing}`: Maximum number of successful executions in the retained history.
- `expires_at::Union{DateTime, Nothing}`: Expiration time for a job.
- `timezone::Union{Nothing, String}`: IANA timezone name (e.g. `"America/Denver"`). When set, the job's cron schedule is interpreted in this timezone. `nothing` means UTC.
- `zone`: Internal callback computing the next UTC time of a schedule in `timezone`; `nothing` without a timezone, and for options read back from a store record (see [`restoreJobs!`](@ref)).
"""
struct JobOptions
    overlap_policy::Union{Symbol, Nothing} # :skip, :queue, :concurrent
    retries::Int
    retry_delays::Union{Base.ExponentialBackOff, Nothing} # see Base.ExponentialBackOff
    retry_check::Union{Nothing, Callback{RetryCheckCall}} # see Base.retry `check` keyword argument
    max_failed_executions::Union{Int, Nothing} # max number of failed executions allowed for a job before it will be disabled
    max_executions::Union{Int, Nothing} # max number of _successful_ executions job is allowed to run
    expires_at::Union{DateTime, Nothing} # expiration time for job
    timezone::Union{Nothing, String} # IANA timezone, e.g. "America/Denver"
    zone::Union{Nothing, Callback{NextTimeCall}}

    # validate at construction: a bad option caught here fails the Job/Scheduler
    # definition, rather than misbehaving inside the scheduler loop later (an
    # unrecognized overlap_policy would leave ready executions queued forever).
    # `@nospecialize` keeps one compiled method for every combination of the
    # union-typed options (for example when rebuilding options from a record).
    function JobOptions(@nospecialize(overlap_policy::Union{Symbol, Nothing}), retries::Int,
                        @nospecialize(retry_delays::Union{Base.ExponentialBackOff, Nothing}),
                        @nospecialize(retry_check::Union{Nothing, Callback{RetryCheckCall}}),
                        @nospecialize(max_failed_executions::Union{Int, Nothing}),
                        @nospecialize(max_executions::Union{Int, Nothing}),
                        @nospecialize(expires_at::Union{DateTime, Nothing}),
                        @nospecialize(timezone::Union{Nothing, String}),
                        @nospecialize(zone::Union{Nothing, Callback{NextTimeCall}}))
        overlap_policy === nothing || overlap_policy in (:skip, :queue, :concurrent) ||
            throw(ArgumentError("overlap_policy must be :skip, :queue, or :concurrent, got :$overlap_policy"))
        retries >= 0 || throw(ArgumentError("retries must be non-negative, got $retries"))
        max_failed_executions === nothing || max_failed_executions > 0 ||
            throw(ArgumentError("max_failed_executions must be positive, got $max_failed_executions"))
        max_executions === nothing || max_executions > 0 ||
            throw(ArgumentError("max_executions must be positive, got $max_executions"))
        return new(overlap_policy, retries, retry_delays, retry_check,
                   max_failed_executions, max_executions, expires_at, timezone, zone)
    end
end

# `retry_check::R`: a function argument the method only passes along would not
# be specialized on, leaving the callback construction dynamically dispatched
function JobOptions(; overlap_policy=nothing, retries=0,
        retry_delays=retries > 0 ? Base.ExponentialBackOff(; n=retries) : nothing,
        retry_check::R=nothing, max_failed_executions=nothing, max_executions=nothing,
        expires_at=nothing, timezone=nothing) where {R}
    check = retry_check === nothing ? nothing : Callback{RetryCheckCall}(retry_check)
    tz = timezone === nothing ? nothing : String(timezone)
    return JobOptions(convert(Union{Symbol, Nothing}, overlap_policy), convert(Int, retries),
                      convert(Union{Base.ExponentialBackOff, Nothing}, retry_delays), check,
                      convert(Union{Int, Nothing}, max_failed_executions),
                      convert(Union{Int, Nothing}, max_executions),
                      convert(Union{DateTime, Nothing}, expires_at),
                      tz, tz === nothing ? nothing : zonecallback(tz))
end

Base.show(io::IO, opts::JobOptions) = print(io, "Tempus.JobOptions(overlap_policy=$(opts.overlap_policy), retries=$(opts.retries), max_failed_executions=$(opts.max_failed_executions), max_executions=$(opts.max_executions), expires_at=$(opts.expires_at), timezone=$(opts.timezone))")

"""
    Job

Represents a single job/unit of work. Can be scheduled to repeat.

# Fields:
- `name::String`: Unique identifier for the job.
- `schedule::Union{Cron, Nothing}`: The cron-style schedule expression.
- `action`: The function to execute when the job runs, behind an internal callback wrapper (call it as `job.action()`); `nothing` for a job read from a store that was not added in this process (see [`addJob!`](@ref)).
- `action_ref::Union{Nothing, String}`: Fully qualified function reference (e.g. `"MyModule.my_handler"`), used by [`restoreJobs!`](@ref).
- `action_data::Union{Nothing, String}`: JSON-encoded `job_params`, passed to the action as keyword arguments: `action(; JSON.parse(action_data)...)`.
- `options::JobOptions`: Execution options for retries, failures, and overlap handling.
- `disabledAt::Union{DateTime, Nothing}`: Timestamp when the job was disabled (if applicable).

Create jobs at run time (for example in a package's `__init__`), not during
precompilation: the action wrapper is not valid in another Julia session.
`job_params` are converted to keyword arguments from JSON at every call, which
binaries compiled with `juliac --trim` cannot do; there, capture parameters in
the action instead (`Job(() -> handler(; value="x"), ...)`).
"""
mutable struct Job
    const lock::ReentrantLock
    const action::Union{Nothing, JobAction}
    const action_ref::Union{Nothing, String}
    const action_data::Union{Nothing, String}
    const name::String
    const schedule::Union{Cron, Nothing}
    const options::JobOptions
    # fields managed by scheduler
    disabledAt::Union{DateTime, Nothing}
end

function Job(action::F, name, schedule;
        job_params=nothing, action_ref::Union{Nothing,String}=nothing, kw...) where {F<:Function}
    schedule_parsed = schedule isa Cron ? schedule : parseCron(schedule)
    data = job_params === nothing ? nothing : JSON.json(job_params)
    return Job(action, string(name), schedule_parsed, action_ref, data, JobOptions(; kw...))
end

"""
    OneShotJob(action, name; kw...)

A [`Job`](@ref) with no cron schedule that runs once, as soon as the scheduler
picks it up, and is then disabled. A failed attempt is re-run (with the job's
retry options applied within each attempt) until it succeeds or reaches
`max_failed_executions`. Accepts the same keyword options as `Job`.
`max_executions` is always set to `1`.

Note the run-once bookkeeping is based on the job's stored execution history,
so re-adding a one-shot job whose name has already succeeded will not run it
again; use a fresh name (or [`purgeJob!`](@ref)) to re-run one.
"""
function OneShotJob(action::F, name;
        job_params=nothing, action_ref::Union{Nothing,String}=nothing, kw...) where {F<:Function}
    data = job_params === nothing ? nothing : JSON.json(job_params)
    return Job(action, string(name), nothing, action_ref, data, JobOptions(; kw..., max_executions=1))
end

function Job(f::F, name::String, schedule::Union{Cron, Nothing}, action_ref::Union{Nothing, String},
             data::Union{Nothing, String}, options::JobOptions) where {F<:Function}
    ref = action_ref !== nothing ? action_ref : _function_ref(f)
    action = data === nothing ? JobAction(f) : JobAction(() -> f(; JSON.parse(data)...))
    return Job(ReentrantLock(), action, ref, data, name, schedule, options, nothing)
end

"""Auto-extract fully qualified function reference string from a named function."""
function _function_ref(f::F) where {F<:Function}
    name = String(nameof(f))
    # anonymous functions cannot be resolved by name
    startswith(name, '#') && return nothing
    parts = [name]
    # module path, outermost first; `Main` is left out (bare names resolve there)
    m = parentmodule(f)
    while m !== Main
        pushfirst!(parts, String(nameof(m)))
        parent = parentmodule(m)
        parent === m && break
        m = parent
    end
    return join(parts, '.')
end

"""
    resolve_function(ref::String) -> Function

Resolve a function from its fully qualified reference string (e.g. `"MyModule.my_handler"`).
The module must be loaded before calling this function. This uses reflection,
so it is unavailable in binaries compiled with `juliac --trim`.
"""
function resolve_function(ref::String)
    parts = Symbol.(split(ref, '.'))
    if length(parts) == 1
        # Bare function name — look up in Main
        isdefined(Main, parts[1]) || error("Function $(ref) not found in Main")
        obj = getproperty(Main, parts[1])
        obj isa Function || error("$(ref) resolved to $(typeof(obj)), expected Function")
        return obj
    end
    obj = nothing
    for m in Base.loaded_modules_array()
        if nameof(m) == parts[1]
            obj = m
            break
        end
    end
    obj === nothing && error("Module $(parts[1]) not found in loaded modules. Ensure the module is loaded before restoring jobs.")
    for i in 2:length(parts)
        isdefined(obj, parts[i]) || error("$(join(parts[1:i], '.')) not found")
        obj = getproperty(obj, parts[i])
    end
    obj isa Function || error("$(ref) resolved to $(typeof(obj)), expected Function")
    return obj
end

function Base.show(io::IO, job::Job)
    println(io, "Job: $(job.name)")
    job.action_ref === nothing || println(io, "Action: $(job.action_ref)")
    job.schedule === nothing || println(io, "Schedule: $(job.schedule)")
    println(io, "Options: $(job.options)")
    job.disabledAt === nothing || println(io, "Disabled: $(job.disabledAt)")
    return
end

"""
    Store

Two typed `AbstractStore` views used by a scheduler, jobs by name
([`JobRecord`](@ref)s) and bounded execution history by job name
([`JobExecutionRecord`](@ref)s), plus the actions of the jobs added to this
`Store` object in this process. Applications choose the shared backend.

Actions are not persisted. A scheduler runs a stored job only when a job of
that name was added in this process with [`addJob!`](@ref) or `push!`; add each
job at startup, or call [`restoreJobs!`](@ref) to resolve stored jobs' actions
by name.
"""
struct Store{J<:AbstractStores.AbstractStore,E<:AbstractStores.AbstractStore}
    jobs::J
    executions::E
    history_limit::Int
    added::Dict{String, Job} # jobs added in this process, by name; they supply the actions
    lock::ReentrantLock # guards `added`
end

Store(jobs::J, executions::E, history_limit::Int) where {J, E} =
    Store{J, E}(jobs, executions, history_limit, Dict{String, Job}(), ReentrantLock())

# A view's values have its element type, but not every backend's `get` infers
# it (an untyped `MemoryStore` returns `Any`); assert it so code using the
# value stays statically dispatched.
_get(view::AbstractStores.AbstractStore{T}, key::AbstractString) where {T} =
    get(view, key, nothing)::Union{Nothing, T}

nextJobExecution(scheduler, job::Job) =
    nextJobExecution(
        scheduler.store,
        job,
        _some(job.options.max_failed_executions, scheduler.jobOptions.max_failed_executions),
        _some(job.options.max_executions, scheduler.jobOptions.max_executions),
        _some(job.options.expires_at, scheduler.jobOptions.expires_at);
        logging=scheduler.logging
    )

"""
    nextJobExecution(store::Store, job::Job) -> Union{JobExecution, Nothing}

For a `job` persisted in `store`, check the job's status and execution history
and return a `JobExecution` for the next time it should run, or `nothing` if
the job shouldn't be scheduled again. As a side effect, jobs that have expired
or reached their execution caps are disabled in the store.
"""
function nextJobExecution(store::Store, job::Job,
        @nospecialize(max_failed_executions::Union{Int, Nothing}=job.options.max_failed_executions),
        @nospecialize(max_executions::Union{Int, Nothing}=job.options.max_executions),
        @nospecialize(expires_at::Union{DateTime, Nothing}=job.options.expires_at); logging::Bool=true)
    # if job is already disabled, return nothing
    isdisabled(job) && return nothing
    # check if job has expired
    if expires_at !== nothing && expires_at < Dates.now(UTC)
        logging && @info "Disabling job $(job.name) due to expiration: $(expires_at)."
        disableJob!(store, job)
        return nothing
    end
    # pull job execution history for other checks
    nexecs = max_executions === nothing ? something(max_failed_executions, 0) : store.history_limit
    execs = getNMostRecentJobExecutions(store, job.name, nexecs)
    # check if max number of executions has been reached
    if max_executions !== nothing && count(e -> e.succeeded, execs) >= max_executions
        logging && @info "Disabling job $(job.name) after reaching maximum number of successful executions: $(max_executions)."
        disableJob!(store, job)
        return nothing
    end
    # check if max number of failed executions has been reached
    if max_failed_executions !== nothing
        execs = @view execs[1:min(max_failed_executions, length(execs))]
    end
    if max_failed_executions !== nothing && count(e -> !e.succeeded, execs) >= max_failed_executions
        logging && @info "Disabling job $(job.name) after reaching maximum number of failed executions: $(max_failed_executions)."
        disableJob!(store, job)
        return nothing
    end
    zone = job.options.zone
    time = if job.schedule === nothing
        Dates.now(UTC)
    elseif job.options.timezone === nothing
        getnext(job.schedule)
    elseif zone !== nothing
        zone(NextTimeCall(job.schedule, Dates.now(UTC))).next
    else
        throw(ArgumentError("job $(job.name) was read from a store record without its timezone schedule; add the job again or call `Tempus.restoreJobs!`"))
    end
    return JobExecution(job, time)
end

"""
    disable!(job::Job)

Disables a job, preventing it from being scheduled for execution.

This mutates the `Job` object only. A store keeps its own record of the job,
so use [`disableJob!`](@ref)`(store, job)` when the change must survive a
restart.
"""
disable!(job::Job) = (@lock job.lock (job.disabledAt = Dates.now(UTC)))

"""
    enable!(job::Job)

Enables a previously disabled job, allowing it to be scheduled again.

Like [`disable!`](@ref), this mutates the `Job` object only; call
[`addJob!`](@ref)`(store, job)` afterwards to write the re-enabled job back to a
persisting store.
"""
enable!(job::Job) = @lock job.lock (job.disabledAt = nothing)

"""
    isdisabled(job::Job) -> Bool

Returns `true` if the job is currently disabled.
"""
isdisabled(job::Job) = @lock job.lock job.disabledAt !== nothing

Base.hash(j::Job, h::UInt) = hash(j.name, h)

"""
    JobExecution

Represents an instance of a job execution.

# Fields:
- `jobExecutionId::String`: Unique identifier for this job execution.
- `job::Job`: The job being executed.
- `scheduledStart::DateTime`: When the job was scheduled to run.
- `runConcurrently::Bool`: Whether this execution is running concurrently with another.
- `actualStart::DateTime`: The actual start time.
- `finish::DateTime`: The completion time.
- `status::Symbol`: The execution result (`:succeeded`, `:failed`).
"""
mutable struct JobExecution
    const jobExecutionId::String
    const job::Job
    const scheduledStart::DateTime
    runConcurrently::Bool
    actualStart::DateTime
    finish::DateTime
    status::Symbol # :succeeded, :failed
    result::Any
    exception::Union{Exception, Nothing}
    # second precision: Julia 1.13's trim verifier rejects the millisecond
    # formatting `string(::DateTime)` would include
    JobExecution(job::Job, scheduledStart::DateTime) = new(
        string(job.name, '-', Dates.format(scheduledStart, dateformat"yyyy-mm-dd\THH:MM:SS")),
        job, scheduledStart, false)
end

Base.hash(je::JobExecution, h::UInt) = hash(je.jobExecutionId, h)

function Base.show(io::IO, je::JobExecution)
    println(io, "JobExecution: $(je.jobExecutionId)")
    println(io, "Job: $(je.job.name)")
    println(io, "Scheduled Start: $(je.scheduledStart)")
    if isdefined(je, :status)
        println(io, "Actual Start: $(je.actualStart)")
        println(io, "Finish: $(je.finish)")
        println(io, "Status: $(je.status)")
        println(io, "Result: $(je.result)")
        println(io, "Exception: $(je.exception)")
    end
    return
end

"""
    JobRecord

What a [`Store`](@ref) persists for a [`Job`](@ref): its definition and disabled
state, without the action, which exists only in the process that added the job.
`schedule` is the cron expression (`nothing` for one-shot jobs); the option
fields are those of [`JobOptions`](@ref) except `retry_check` and `zone`, with
`overlap_policy` as a string.
"""
struct JobRecord
    name::String
    schedule::Union{Nothing, String}
    action_ref::Union{Nothing, String}
    action_data::Union{Nothing, String}
    overlap_policy::Union{Nothing, String}
    retries::Int
    retry_delays::Union{Nothing, Base.ExponentialBackOff}
    max_failed_executions::Union{Nothing, Int}
    max_executions::Union{Nothing, Int}
    expires_at::Union{Nothing, DateTime}
    timezone::Union{Nothing, String}
    disabledAt::Union{Nothing, DateTime}
end

function JobRecord(job::Job)
    disabledAt = @lock job.lock job.disabledAt
    o = job.options
    schedule = job.schedule === nothing ? nothing : cronstring(job.schedule)
    policy = o.overlap_policy === nothing ? nothing : String(o.overlap_policy)
    return JobRecord(job.name, schedule, job.action_ref, job.action_data, policy,
                     o.retries, o.retry_delays, o.max_failed_executions, o.max_executions,
                     o.expires_at, o.timezone, disabledAt)
end

"""
    JobExecutionRecord

What a [`Store`](@ref) persists for a finished [`JobExecution`](@ref): its id,
timing, and whether it `succeeded` (its `status` was `:succeeded`). The value
an action returned and the exception it threw are kept only on the in-process
`JobExecution`.
"""
struct JobExecutionRecord
    jobExecutionId::String
    scheduledStart::DateTime
    runConcurrently::Bool
    actualStart::DateTime
    finish::DateTime
    succeeded::Bool
end

isdisabled(r::JobRecord) = r.disabledAt !== nothing

JobExecutionRecord(e::JobExecution) = JobExecutionRecord(e.jobExecutionId, e.scheduledStart,
    e.runConcurrently, e.actualStart, e.finish, e.status == :succeeded)

"""
    Store(backend; prefix="tempus/", history_limit=100)

Create job and execution-history views over one `AbstractStore` backend.
Namespacing lets Tempus share a physical store with other libraries and
application state.

The backend must accept both `JobRecord` and `Vector{JobExecutionRecord}`
values, i.e. be an `AbstractStore{Any}`. A serializing backend can use
`SerializedCodec` or `JSONCodec`; JSON needs AbstractStores 0.2 or later, which
decodes through the views' concrete types.

`history_limit` bounds the execution history kept per job. Note that
`max_executions`/`max_failed_executions` are evaluated against that history, so a
limit below either of them means the corresponding cap can never be reached.
"""
function Store(
    backend::AbstractStores.AbstractStore;
    prefix::AbstractString="tempus/",
    history_limit::Int=100,
)
    history_limit > 0 || throw(ArgumentError("history_limit must be positive"))
    (JobRecord <: eltype(backend) && Vector{JobExecutionRecord} <: eltype(backend)) || throw(ArgumentError(
        "a Tempus store needs a backend holding both `Tempus.JobRecord` and " *
        "`Vector{Tempus.JobExecutionRecord}` values, but `eltype(backend)` is $(eltype(backend)). " *
        "Use an `AbstractStore{Any}` (`MemoryStore()`, `FileStore(dir)`, `SQLStore{Any}(conn)`), " *
        "or pass separately typed stores to `Tempus.Store(jobs, executions)`."))
    AbstractStores.checkstore(backend; listing=true)
    return Store(
        AbstractStores.PrefixedStore{JobRecord}(backend, string(prefix, "jobs/")),
        AbstractStores.PrefixedStore{Vector{JobExecutionRecord}}(
            backend,
            string(prefix, "executions/"),
        ),
        history_limit,
    )
end

"""
    Store(jobs, executions; history_limit=100)

Create a store from separate typed job and execution-history backends.
"""
function Store(
    jobs::AbstractStores.AbstractStore{JobRecord},
    executions::AbstractStores.AbstractStore{Vector{JobExecutionRecord}};
    history_limit::Int=100,
)
    history_limit > 0 || throw(ArgumentError("history_limit must be positive"))
    AbstractStores.checkstore(jobs; listing=true)
    return Store(jobs, executions, history_limit)
end

"""Create a process-local Tempus store."""
InMemoryStore(; kw...) = Store(AbstractStores.MemoryStore(); kw...)

"""
    FileStore(directory; kw...)

Create a Tempus store backed by an `AbstractStores.FileStore`. `directory` is a
directory, not the single JSON file used by Tempus 2.
"""
FileStore(directory::AbstractString; kw...) =
    Store(AbstractStores.FileStore(directory); kw...)

"""
    SQLiteStore(connection; table="tempus_state", kw...)

Compatibility constructor for a Tempus store backed by one
`AbstractStores.SQLStore` table. The same `Store(SQLStore(...))` form works for
SQLite, Postgres, and other DBInterface drivers.
"""
function SQLiteStore(connection; table::AbstractString="tempus_state", kw...)
    backend = AbstractStores.SQLStore{Any}(connection; table)
    return Store(backend; kw...)
end

# The job a stored record describes: the job added under its name in this
# process, with the record's disabled state, or else a job rebuilt from the
# record without an action.
function Job(store::Store, record::JobRecord)
    added = @lock store.lock get(store.added, record.name, nothing)
    added === nothing || return Job(ReentrantLock(), added.action, added.action_ref,
        added.action_data, added.name, added.schedule, added.options, record.disabledAt)
    schedule = record.schedule === nothing ? nothing : parseCron(record.schedule)
    policy = record.overlap_policy === nothing ? nothing : Symbol(record.overlap_policy)
    options = JobOptions(policy, record.retries, record.retry_delays, nothing,
        record.max_failed_executions, record.max_executions, record.expires_at, record.timezone, nothing)
    return Job(ReentrantLock(), nothing, record.action_ref, record.action_data,
        record.name, schedule, options, record.disabledAt)
end

"""
    getJobs(store) -> Vector{Job}

Return every stored job, regardless of disabled status. A job added to `store`
in this process carries its action; any other stored job has `action === nothing`.
"""
function getJobs(store::Store)
    jobs = Job[]
    for name in keys(store.jobs)
        # a concurrent `purgeJob!` can remove a key between listing and fetching
        record = _get(store.jobs, name)
        record === nothing || push!(jobs, Job(store, record))
    end
    return jobs
end

"""
    addJob!(store, job)

Add or replace a job by name. The job's definition is persisted; its action is
kept in `store` for this process, so add every job again after a restart (or
call [`restoreJobs!`](@ref)) for a scheduler to run it.
"""
function addJob!(store::Store, job::Job)
    put!(store.jobs, job.name, JobRecord(job))
    job.action === nothing || @lock store.lock (store.added[job.name] = job)
    return job
end

"""
    restoreJobs!(store) -> Vector{Job}

Give stored jobs without an action in this process their action back by
resolving each job's `action_ref` with [`resolve_function`](@ref), so a
scheduler runs jobs a previous process stored without adding them again. Jobs
without an `action_ref` (anonymous functions) are skipped. Returns the
restored jobs.

This resolves functions by name through reflection, so it is unavailable in
binaries compiled with `juliac --trim`; add every job with [`addJob!`](@ref)
at startup there.
"""
function restoreJobs!(store::Store)
    restored = Job[]
    for stored in getJobs(store)
        (stored.action === nothing && stored.action_ref !== nothing) || continue
        f = resolve_function(stored.action_ref)
        o = stored.options
        options = o.timezone === nothing ? o : JobOptions(o.overlap_policy, o.retries, o.retry_delays,
            o.retry_check, o.max_failed_executions, o.max_executions, o.expires_at, o.timezone,
            zonecallback(o.timezone))
        job = Job(f, stored.name, stored.schedule, stored.action_ref, stored.action_data, options)
        job.disabledAt = stored.disabledAt
        @lock store.lock (store.added[job.name] = job)
        push!(restored, job)
    end
    return restored
end

"""Remove a job and all execution history for that job."""
function purgeJob!(store::Store, job::Union{Job,AbstractString})
    name = job isa Job ? job.name : String(job)
    delete!(store.jobs, name)
    delete!(store.executions, name)
    @lock store.lock delete!(store.added, name)
    return nothing
end

"""
    disableJob!(store, job; at=Dates.now(UTC))

Disable a stored job by reference or name. The update uses the backend atomic
read-modify-write operation.
"""
function disableJob!(
    store::Store,
    job::Union{Job,AbstractString};
    at::DateTime=Dates.now(UTC),
)
    name = job isa Job ? job.name : String(job)
    # `@nospecialize`: backends that do not infer the view's type pass `Any`
    updated = AbstractStores.modify!(store.jobs, name) do @nospecialize(current)
        current === nothing ? nothing : disabled(current::JobRecord, at)
    end::Union{Nothing, JobRecord}
    if job isa Job && updated !== nothing
        @lock job.lock job.disabledAt = at
    end
    return updated === nothing ? nothing : Job(store, updated)
end

disabled(r::JobRecord, at::DateTime) = JobRecord(r.name, r.schedule, r.action_ref, r.action_data,
    r.overlap_policy, r.retries, r.retry_delays, r.max_failed_executions, r.max_executions,
    r.expires_at, r.timezone, at)

"""
    storeJobExecution!(store, execution)

Prepend one execution's [`JobExecutionRecord`](@ref) to the job history and keep
at most `history_limit` records. The update is atomic when the selected backend
provides atomic `modify!`.
"""
function storeJobExecution!(store::Store, execution::JobExecution)
    record = JobExecutionRecord(execution)
    # `@nospecialize`: backends that do not infer the view's type pass `Any`
    AbstractStores.modify!(store.executions, execution.job.name) do @nospecialize(current)
        history = current === nothing ? JobExecutionRecord[] : copy(current::Vector{JobExecutionRecord})
        pushfirst!(history, record)
        resize!(history, min(length(history), store.history_limit))
        return history
    end
    return execution
end

"""Return at most `n` [`JobExecutionRecord`](@ref)s, newest first."""
function getNMostRecentJobExecutions(
    store::Store,
    job_name::String,
    n::Int,
)
    n <= 0 && return JobExecutionRecord[]
    history = _get(store.executions, job_name)
    history === nothing && return JobExecutionRecord[]
    return history[1:min(n, length(history))]
end

"""
    Scheduler

The main scheduling engine that executes jobs according to their schedules.

# Fields:
- `lock::ReentrantLock`: Ensures thread-safe access.
- `jobExecutions::Vector{JobExecution}`: List of scheduled job executions.
- `store::S`: Job storage backend (a [`Store`](@ref)); the scheduler type is parameterized on it so store calls are statically dispatched.
- `jobExecutionFinished::Threads.Event`: Signals all job executions have finished when shutting down.
- `executingJobExecutions::Set{JobExecution}`: Tracks currently executing jobs.
- `running::Bool`: Scheduler state (running/stopped).
- `loopActive::Bool`: Whether the dispatch-loop task has finished stopping.
- `jobOptions::JobOptions`: Default job execution options.
- `max_concurrent_executions::Int`: Limit on how many total executions can be running concurrently for this scheduler, defaults to `Threads.nthreads()`
- `logging::Bool`: Whether to emit log messages during scheduler operations, defaults to `true`.
"""
mutable struct Scheduler{S<:Store}
    const lock::ReentrantLock
    const jobExecutions::Vector{JobExecution}
    const store::S
    const jobExecutionFinished::Threads.Event
    const executingJobExecutions::Set{JobExecution}
    running::Bool
    loopActive::Bool
    const jobOptions::JobOptions
    const max_concurrent_executions::Int
    const logging::Bool
    Scheduler(
        store::S=InMemoryStore();
        overlap_policy::Symbol=:skip,
        retries::Int = 3,
        retry_delays::Union{Base.ExponentialBackOff, Nothing}=retries == 0 ? nothing : Base.ExponentialBackOff(; n=retries),
        retry_check::R=nothing,
        max_failed_executions::Union{Int, Nothing}=3,
        max_executions::Union{Int, Nothing}=nothing,
        expires_at::Union{DateTime, Nothing}=nothing,
        max_concurrent_executions::Int=Threads.nthreads(),
        logging::Bool=true,
    ) where {S<:Store, R} = begin
        max_concurrent_executions >= 1 || throw(ArgumentError("max_concurrent_executions must be at least 1, got $max_concurrent_executions"))
        new{S}(ReentrantLock(), JobExecution[], store, Threads.Event(), Set{JobExecution}(), false, false, JobOptions(; overlap_policy, retries, retry_delays, retry_check, max_failed_executions, max_executions, expires_at), max_concurrent_executions, logging)
    end
end

Scheduler(backend::AbstractStores.AbstractStore; kw...) =
    Scheduler(Store(backend); kw...)

function Base.show(io::IO, scheduler::Scheduler)
    println(io, "Scheduler:")
    println(io, "  Jobs: $(length(scheduler.jobExecutions))")
    println(io, "  Running: $(scheduler.running)")
    return
end

"""
    run!(scheduler::Scheduler; close_when_no_jobs::Bool=false)

Starts the scheduler, executing jobs at their scheduled times. The dispatch
loop runs on a background task; `run!` returns the scheduler immediately.
With `close_when_no_jobs=true`, the loop shuts down on its own once no
executions are queued or running (see [`runJobs!`](@ref)). Throws if the
scheduler is already running or a previous timed-out close still has loop or
job tasks in flight.

Only stored jobs with an action in this process run (see [`addJob!`](@ref));
others are skipped with a warning.

A dispatch error stops the loop without canceling jobs that already started.
Selected executions whose tasks were not started remain queued. Calling
`run!` again rebuilds the queue from the stored jobs and their execution history.
"""
function run!(scheduler::Scheduler; close_when_no_jobs::Bool=false)
    readyToExecute = Tuple{Int, Bool, JobExecution}[]
    scheduler.logging && @info "Starting scheduler and all jobs."
    @lock scheduler.lock begin
        scheduler.running && throw(ArgumentError("scheduler is already running; close it before calling run! again"))
        scheduler.loopActive &&
            throw(ArgumentError("the previous scheduler loop is still stopping"))
        isempty(scheduler.executingJobExecutions) ||
            throw(ArgumentError("the scheduler still has in-flight job executions"))

        # Build the initial queue before changing lifecycle state. A store error
        # here must leave the scheduler stopped and safe to retry.
        initial_executions = JobExecution[]
        for job in getJobs(scheduler.store)
            if job.action === nothing
                scheduler.logging && @warn "Skipping stored job $(job.name): it has no action in this process. Add it with `Tempus.addJob!` or `push!` at startup, or call `Tempus.restoreJobs!`."
                continue
            end
            je = nextJobExecution(scheduler, job)
            je === nothing || push!(initial_executions, je)
        end
        sort!(initial_executions, by=je->je.scheduledStart)

        reset(scheduler.jobExecutionFinished)
        empty!(scheduler.jobExecutions)
        append!(scheduler.jobExecutions, initial_executions)
        scheduler.running = true
        scheduler.loopActive = true
    end
    # start scheduler job execution task
    Threads.@spawn :interactive try
        while true
            empty!(readyToExecute)
            now = trunc(Dates.now(UTC), Second)
            @lock scheduler.lock begin
                scheduler.running || break
                if close_when_no_jobs && isempty(scheduler.jobExecutions) && isempty(scheduler.executingJobExecutions)
                    # in-flight executions count: a finishing one-shot may still
                    # schedule a retry, and waiting also lets `wait(scheduler)`
                    # callers observe every execution's completion
                    scheduler.logging && @info "No jobs left to execute, closing scheduler."
                    break
                end
                # check for jobs that are ready to execute
                resort = false
                for (i, je) in enumerate(scheduler.jobExecutions)
                    if je.scheduledStart <= now
                        if isdisabled(je.job)
                            push!(readyToExecute, (i, true, je))
                        elseif length(scheduler.executingJobExecutions) >= scheduler.max_concurrent_executions
                            # scheduler is already executing at limit; leave the
                            # execution queued, it is dispatched (and the job's
                            # following execution scheduled) once capacity frees up
                        elseif any(j -> j.job.name == je.job.name, scheduler.executingJobExecutions)
                            overlap_policy = _some(je.job.options.overlap_policy, scheduler.jobOptions.overlap_policy)
                            if overlap_policy == :skip
                                push!(readyToExecute, (i, true, je))
                            elseif overlap_policy == :concurrent
                                push!(readyToExecute, (i, false, je))
                                push!(scheduler.executingJobExecutions, je)
                                je.runConcurrently = true
                            elseif overlap_policy == :queue
                                next = scheduleNextExecution!(scheduler, je.job)
                                if next !== nothing
                                    resort = true
                                    if scheduler.logging
                                        nexecs = count(j -> j.job.name == je.job.name, scheduler.jobExecutions)
                                        @warn "Job $(je.job.name) already executing, keeping scheduled execution queued until current execution finishes. There are $nexecs queued for this job."
                                    end
                                end
                            end
                        else
                            push!(readyToExecute, (i, false, je))
                            push!(scheduler.executingJobExecutions, je)
                        end
                    else
                        # scheduler.jobExecutions is sorted by scheduledStart
                        break
                    end
                end
                # remove job executions that are ready or to be skipped while holding the lock and schedule next execution
                if !isempty(readyToExecute)
                    # remove from highest index first so earlier deletes don't shift later indices
                    sort!(readyToExecute, by=x -> x[1], rev=true)
                    for (ready_index, (i, toSkip, je)) in enumerate(readyToExecute)
                        deleteat!(scheduler.jobExecutions, i)
                        next = scheduleNextExecution!(scheduler, je.job)
                        # The eligibility check can disable a job that expired while queued.
                        if !toSkip && isdisabled(je.job)
                            delete!(scheduler.executingJobExecutions, je)
                            toSkip = true
                            readyToExecute[ready_index] = (i, toSkip, je)
                        end
                        resort |= next !== nothing
                        if scheduler.logging
                            if isdisabled(je.job)
                                if next !== nothing
                                    @info "[$(je.jobExecutionId)]: Skipping disabled job $(je.job.name) (disabled at $(je.job.disabledAt)) execution at $(now), next scheduled at $(next.scheduledStart)"
                                else
                                    @info "[$(je.jobExecutionId)]: Skipping disabled job $(je.job.name) (disabled at $(je.job.disabledAt)) execution at $(now), no next execution scheduled"
                                end
                            elseif toSkip
                                if next !== nothing
                                    @info "[$(je.jobExecutionId)]: Skipping job $(je.job.name) execution at $(now), next scheduled at $(next.scheduledStart)"
                                else
                                    @info "[$(je.jobExecutionId)]: Skipping job $(je.job.name) execution at $(now), no next execution scheduled"
                                end
                            else
                                if next !== nothing
                                    @info "[$(je.jobExecutionId)]: Job $(je.job.name) execution scheduled at $(next.scheduledStart)"
                                else
                                    @info "[$(je.jobExecutionId)]: Job $(je.job.name) execution scheduled at $(now), no next execution scheduled"
                                end
                            end
                        end
                    end
                end
                # restore scheduledStart order after any scheduling this pass
                resort && sort!(scheduler.jobExecutions, by=je->je.scheduledStart)
            end
            filter!(x -> !x[2], readyToExecute)
            if !isempty(readyToExecute)
                _launch_ready!(executeJob!, scheduler, readyToExecute)
            else
                # @info "No jobs to execute, sleeping 500ms then checking again."
                sleep(0.5)
            end
        end
    catch e
        # nothing waits on this task, so report the failure here
        @error "Tempus scheduler loop failed" exception=(e, catch_backtrace())
        rethrow()
    finally
        # the loop is the scheduler's liveness; however it exits — including a
        # store error thrown mid-iteration — the scheduler is no longer running
        # and finishing executions must be able to observe that (otherwise
        # `wait(scheduler)` never returns)
        @lock scheduler.lock begin
            # Entries remain in the batch until task handoff succeeds. Return
            # unlaunched work to the queue without releasing any running task.
            if !isempty(readyToExecute)
                for (_, toSkip, je) in readyToExecute
                    toSkip && continue
                    delete!(scheduler.executingJobExecutions, je)
                    any(queued -> queued === je, scheduler.jobExecutions) ||
                        push!(scheduler.jobExecutions, je)
                end
                sort!(scheduler.jobExecutions, by=je->je.scheduledStart)
            end
            scheduler.running = false
            scheduler.loopActive = false
            isempty(scheduler.executingJobExecutions) && notify(scheduler.jobExecutionFinished)
        end
    end
    return scheduler
end

# Preserve the batch's dispatch order while removing entries only after the
# launcher has accepted them. On failure, the remaining entries still belong
# to the dispatch loop and its cleanup can release their reservations.
function _launch_ready!(launch::F, scheduler::Scheduler, readyToExecute) where {F}
    reverse!(readyToExecute)
    while !isempty(readyToExecute)
        _, toSkip, je = last(readyToExecute)
        toSkip || launch(scheduler, je)
        pop!(readyToExecute)
    end
    return nothing
end

"""
    scheduleNextExecution!(scheduler::Scheduler, job::Job)

Schedule `job`'s next execution, returning it, or `nothing` when no execution
was scheduled: the job is done (see [`nextJobExecution`](@ref)), it has been
removed from the store, it has no action in this process, an execution at the
same time is already queued, or —
for one-shot jobs, which are rescheduled only when an attempt fails — an
execution is already queued or running. `scheduler.lock` must be held.

The new execution is appended without re-sorting (so callers iterating
`scheduler.jobExecutions` or holding indexes into it stay valid); callers must
restore scheduledStart order before the scheduler loop scans the list again.
"""
function scheduleNextExecution!(scheduler::Scheduler, job::Job)
    job.action === nothing && return nothing
    # a job removed from the store (e.g. via `purgeJob!`) must not be revived
    haskey(scheduler.store.jobs, job.name) || return nothing
    next = nextJobExecution(scheduler, job)
    next === nothing && return nothing
    if job.schedule === nothing
        # one-shot jobs have no future occurrences: an already queued or
        # currently running execution means this attempt is already covered
        (any(je -> je.job.name == job.name, scheduler.jobExecutions) ||
            any(je -> je.job.name == job.name, scheduler.executingJobExecutions)) && return nothing
    elseif any(je -> je.job.name == job.name && je.scheduledStart == next.scheduledStart, scheduler.jobExecutions)
        return nothing
    end
    push!(scheduler.jobExecutions, next)
    return next
end

function executeJob!(scheduler::Scheduler, jobExecution::JobExecution)
    Threads.@spawn try
        now = Dates.now(UTC)
        if scheduler.logging
            if jobExecution.runConcurrently
                @info "[$(jobExecution.jobExecutionId)]: Executing job $(jobExecution.job.name) concurrently at $(now)"
            else
                @info "[$(jobExecution.jobExecutionId)]: Executing job $(jobExecution.job.name) at $(now)"
            end
        end
        retry_check = _some(jobExecution.job.options.retry_check, scheduler.jobOptions.retry_check)
        check = (eb, @nospecialize(e)) -> begin
            if isdisabled(jobExecution.job)
                scheduler.logging && @info "[$(jobExecution.jobExecutionId)]: Skipping job $(jobExecution.job.name) retry due to job being disabled"
                return false
            end
            should_retry = retry_check === nothing || retry_check(RetryCheckCall(eb, e)).retry
            if should_retry
                scheduler.logging && @info "[$(jobExecution.jobExecutionId)]: Job $(jobExecution.job.name) execution failed, retrying"
            end
            return should_retry
        end
        # only jobs with an action are scheduled (see scheduleNextExecution!)
        action = jobExecution.job.action::JobAction
        base_action = () -> action()
        retry_delays = _some(jobExecution.job.options.retry_delays, scheduler.jobOptions.retry_delays)
        f = retry_delays !== nothing ?
            Base.retry(base_action; delays=retry_delays, check=check) :
            base_action
        jobExecution.actualStart = now
        try
            jobExecution.result = f()
            jobExecution.status = :succeeded
            jobExecution.exception = nothing
        catch e
            jobExecution.result = nothing
            jobExecution.exception = e
            jobExecution.status = :failed
            scheduler.logging && @error "[$(jobExecution.jobExecutionId)]: Job $(jobExecution.job.name) execution failed" exception=(e, catch_backtrace())
        finally
            jobExecution.finish = Dates.now(UTC)
            scheduler.logging && @info "[$(jobExecution.jobExecutionId)]: Job $(jobExecution.job.name) execution finished at $(jobExecution.finish)"
        end
        # Serialize completion bookkeeping with push!/unschedule!. Otherwise a
        # finishing execution can recreate history after unschedule! purges it,
        # or apply its old options to a same-name replacement.
        @lock scheduler.lock begin
            delete!(scheduler.executingJobExecutions, jobExecution)
            try
                record = _get(scheduler.store.jobs, jobExecution.job.name)
                current_job = record === nothing ? nothing : Job(scheduler.store, record)
                if current_job === nothing
                    # The job was removed while this execution was running. Do
                    # not recreate its history or leave stale queued executions.
                    filter!(je -> je.job.name != jobExecution.job.name, scheduler.jobExecutions)
                else
                    # A store failure must not take the scheduler's
                    # bookkeeping down with it.
                    try
                        storeJobExecution!(scheduler.store, jobExecution)
                    catch e
                        scheduler.logging && @error "[$(jobExecution.jobExecutionId)]: Failed to store execution of job $(jobExecution.job.name); its execution history is now incomplete" exception=(e, catch_backtrace())
                    end

                    if current_job.schedule === nothing
                        # One-shot jobs schedule a follow-up attempt (nothing
                        # when history disqualifies the current stored job) only
                        # after the in-flight execution has finished.
                        next = scheduleNextExecution!(scheduler, current_job)
                        next === nothing || sort!(scheduler.jobExecutions, by=je->je.scheduledStart)
                    else
                        next = nextJobExecution(scheduler, current_job)
                    end
                    if next === nothing
                        # If the current job should not be scheduled again, drop
                        # any queued executions for its name.
                        filter!(je -> je.job.name != current_job.name, scheduler.jobExecutions)
                    end
                end
            catch e
                # a store error here must not skip the notify below (that would
                # hang wait/close); leave queued executions alone — the job's
                # eligibility is re-checked at every dispatch anyway
                scheduler.logging && @error "[$(jobExecution.jobExecutionId)]: Failed to determine job $(jobExecution.job.name)'s next execution" exception=(e, catch_backtrace())
            end
            !scheduler.running && !scheduler.loopActive &&
                isempty(scheduler.executingJobExecutions) && notify(scheduler.jobExecutionFinished)
        end
    catch e
        # nothing waits on this task, so report the failure here
        @error "Tempus job execution task failed" exception=(e, catch_backtrace())
        rethrow()
    end
    return
end

"""
    close(scheduler::Scheduler; timeout::Real=5)

Closes the scheduler, stopping job execution; waits up to `timeout` seconds
(5 by default) for any currently executing jobs to finish before returning.
"""
function Base.close(scheduler::Scheduler; timeout::Real=5)
    isfinite(timeout) && timeout >= 0 ||
        throw(ArgumentError("timeout must be a finite non-negative number, got $timeout"))
    scheduler.logging && @info "Closing scheduler and waiting $(timeout)s for job executions to stop."
    @lock scheduler.lock begin
        scheduler.running = false
        if !scheduler.loopActive && isempty(scheduler.executingJobExecutions)
            notify(scheduler.jobExecutionFinished)
        end
    end

    # Do not notify jobExecutionFinished on timeout. That event means the loop
    # and every execution are actually finished; using it as a timeout signal
    # makes wait(scheduler) lie and permits an unsafe restart of active work.
    status = Base.timedwait(
        () -> (@lock scheduler.lock begin
            !scheduler.loopActive && isempty(scheduler.executingJobExecutions)
        end),
        timeout,
    )
    if status == :timed_out
        scheduler.logging && @warn "Scheduler closing timeout reached, returning without waiting for job executions to finish."
    else
        scheduler.logging && @info "Scheduler closed and job execution stopped."
    end
    return
end

"""
    wait(scheduler::Scheduler)

Waits until the dispatch loop and all started job tasks have stopped. Close the
scheduler explicitly, or pass `close_when_no_jobs=true` to `run!` to stop the
loop automatically when no executions are queued or running.

Returning does not mean every queued job ran: closing the scheduler or a
dispatch error can leave unstarted executions queued. Background task errors
are logged with `@error`; `wait(scheduler)` does not rethrow them.
"""
Base.wait(scheduler::Scheduler) = wait(scheduler.jobExecutionFinished)

"""
    push!(scheduler::Scheduler, job::Job)

Adds a job to the scheduler and underlying Store, scheduling its next execution based on its cron schedule.
Pushing a job whose name is already scheduled replaces the stored job and any queued (not yet running) executions.
"""
function Base.push!(scheduler::Scheduler, job::Job)
    @lock scheduler.lock begin
        addJob!(scheduler.store, job)
        # drop queued executions from a previous version of this job so the
        # schedule reflects the job as just pushed
        filter!(je -> je.job.name != job.name, scheduler.jobExecutions)
        next = scheduleNextExecution!(scheduler, job)
        if next === nothing
            return job
        end
        scheduler.logging && @info "[$(next.jobExecutionId)]: Adding job $(job.name) to scheduler and scheduling next execution at $(next.scheduledStart)."
        sort!(scheduler.jobExecutions, by=je->je.scheduledStart)
    end
    return job
end

"""
    unschedule!(scheduler::Scheduler, job::Union{Job, AbstractString})

Removes a job (by reference or name) from the scheduler and the underlying
store, canceling any queued executions and deleting the job's execution
history. An already running execution finishes but is not rescheduled.

See [`disable!`](@ref)/[`disableJob!`](@ref) to keep a job (and its history)
around while preventing it from running.
"""
function unschedule!(scheduler::Scheduler, job::Union{Job, AbstractString})
    name = job isa Job ? job.name : String(job)
    @lock scheduler.lock begin
        filter!(je -> je.job.name != name, scheduler.jobExecutions)
        purgeJob!(scheduler.store, name)
    end
    return
end

"""
    withscheduler(f, args...; kw...)

Creates a scheduler, runs a function `f` with it, then calls `close`.
"""
function withscheduler(f, args...; kw...)
    scheduler = Scheduler(args...; kw...)
    try
        run!(scheduler)
        f(scheduler)
    finally
        close(scheduler)
    end
end

"""
    runJobs!(store::Store, jobs; kw...)

Add each job in `jobs` to `store`, run a scheduler with `kw` options,
wait for all jobs to finish, then close the scheduler.

"""
function runJobs!(store::Store, jobs; kw...)
    for job in jobs
        addJob!(store, job)
    end
    scheduler = Scheduler(store; kw...)
    try
        run!(scheduler; close_when_no_jobs=true)
        wait(scheduler)
    finally
        close(scheduler)
    end
    return jobs
end

runJobs!(backend::AbstractStores.AbstractStore, jobs; kw...) =
    runJobs!(Store(backend), jobs; kw...)

end # module
