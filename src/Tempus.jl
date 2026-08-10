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

"""
    Store

Two typed `AbstractStore` views used by a scheduler: jobs by name and bounded
execution history by job name. Applications choose the shared backend.
"""
struct Store{J<:AbstractStores.AbstractStore,E<:AbstractStores.AbstractStore}
    jobs::J
    executions::E
    history_limit::Int
end

"""
    JobOptions

Defines options for job execution behavior.

# Fields:
- `overlap_policy::Union{Symbol, Nothing}`: Determines job execution behavior when the same job is already running (`:skip`, `:queue`, `:concurrent`).
- `retries::Int`: Number of retries allowed on failure.
- `retry_delays::Union{Base.ExponentialBackOff, Nothing}`: Delay strategy for retries (defaults to exponential backoff if `retries > 0`).
- `retry_check`: Custom function to determine retry behavior (`check` argument from `Base.retry`).
- `max_failed_executions::Union{Int, Nothing}`: Maximum number of failed executions allowed for a job before it will be disabled.
- `max_executions::Union{Int, Nothing}`: Maximum number of executions allowed for a job.
- `expires_at::Union{DateTime, Nothing}`: Expiration time for a job.
- `timezone::Union{Nothing, String}`: IANA timezone name (e.g. `"America/Denver"`). When set, the job's cron schedule is interpreted in this timezone. `nothing` means UTC.
"""
@kwdef struct JobOptions
    overlap_policy::Union{Symbol, Nothing} = nothing # :skip, :queue, :concurrent
    retries::Int = 0
    retry_delays::Union{Base.ExponentialBackOff, Nothing} = retries > 0 ? Base.ExponentialBackOff(; n=retries) : nothing # see Base.ExponentialBackOff
    retry_check = nothing # see Base.retry `check` keyword argument
    max_failed_executions::Union{Int, Nothing} = nothing # max number of failed executions allowed for a job before it will be disabled
    max_executions::Union{Int, Nothing} = nothing # max number of _successful_ executions job is allowed to run
    expires_at::Union{DateTime, Nothing} = nothing # expiration time for job
    timezone::Union{Nothing, String} = nothing # IANA timezone, e.g. "America/Denver"

    # validate at construction: a bad option caught here fails the Job/Scheduler
    # definition, rather than misbehaving inside the scheduler loop later (an
    # unrecognized overlap_policy would leave ready executions queued forever,
    # and a bad timezone would throw while computing the next execution)
    function JobOptions(overlap_policy, retries, retry_delays, retry_check,
                        max_failed_executions, max_executions, expires_at, timezone)
        overlap_policy === nothing || overlap_policy in (:skip, :queue, :concurrent) ||
            throw(ArgumentError("overlap_policy must be :skip, :queue, or :concurrent, got $(repr(overlap_policy))"))
        retries >= 0 || throw(ArgumentError("retries must be non-negative, got $retries"))
        max_failed_executions === nothing || max_failed_executions > 0 ||
            throw(ArgumentError("max_failed_executions must be positive, got $max_failed_executions"))
        max_executions === nothing || max_executions > 0 ||
            throw(ArgumentError("max_executions must be positive, got $max_executions"))
        timezone === nothing || TimeZones.istimezone(timezone) ||
            throw(ArgumentError("unknown timezone: $(repr(timezone))"))
        return new(overlap_policy, retries, retry_delays, retry_check,
                   max_failed_executions, max_executions, expires_at, timezone)
    end
end

Base.show(io::IO, opts::JobOptions) = print(io, "Tempus.JobOptions(overlap_policy=$(opts.overlap_policy), retries=$(opts.retries), max_failed_executions=$(opts.max_failed_executions), max_executions=$(opts.max_executions), expires_at=$(opts.expires_at), timezone=$(opts.timezone))")

"""
    Job

Represents a single job/unit of work. Can be scheduled to repeat.

# Fields:
- `name::String`: Unique identifier for the job.
- `schedule::Union{Cron, Nothing}`: The cron-style schedule expression.
- `action::Function`: The function to execute when the job runs.
- `action_ref::Union{Nothing, String}`: Fully qualified function reference for persistence (e.g. `"MyModule.my_handler"`).
- `action_data::Union{Nothing, String}`: JSON-encoded parameters for persistence. Splatted as kwargs: `action(; JSON.parse(action_data)...)`.
- `options::JobOptions`: Execution options for retries, failures, and overlap handling.
- `disabledAt::Union{DateTime, Nothing}`: Timestamp when the job was disabled (if applicable).
"""
mutable struct Job
    const lock::ReentrantLock
    const action::Function
    const action_ref::Union{Nothing, String}
    const action_data::Union{Nothing, String}
    const name::String
    const schedule::Union{Cron, Nothing}
    const options::JobOptions
    # fields managed by scheduler
    disabledAt::Union{DateTime, Nothing}
end

function Job(action::Function, name, schedule;
        job_params=nothing, action_ref::Union{Nothing,String}=nothing, kw...)
    ref = action_ref !== nothing ? action_ref : _function_ref(action)
    data = job_params === nothing ? nothing : JSON.json(job_params)
    schedule_parsed = schedule isa Cron ? schedule : parseCron(schedule)
    Job(ReentrantLock(), action, ref, data, string(name), schedule_parsed, JobOptions(; kw...), nothing)
end

"""
    OneShotJob(action, name; kw...)

A [`Job`](@ref) with no cron schedule that runs once, as soon as the scheduler
picks it up, and is then disabled. A failed attempt is re-run (with the job's
retry options applied within each attempt) until it succeeds or reaches
`max_failed_executions`. Accepts the same keyword options as `Job`.

Note the run-once bookkeeping is based on the job's stored execution history,
so re-adding a one-shot job whose name has already succeeded will not run it
again; use a fresh name (or [`purgeJob!`](@ref)) to re-run one.
"""
function OneShotJob(action::Function, name;
        job_params=nothing, action_ref::Union{Nothing,String}=nothing, kw...)
    ref = action_ref !== nothing ? action_ref : _function_ref(action)
    data = job_params === nothing ? nothing : JSON.json(job_params)
    Job(ReentrantLock(), action, ref, data, string(name), nothing, JobOptions(; max_executions=1, kw...), nothing)
end

"""Auto-extract fully qualified function reference string from a named function."""
function _function_ref(f::Function)
    fname = nameof(f)
    # Detect anonymous functions (can't be persisted)
    startswith(string(fname), '#') && return nothing
    mod_path = Base.fullname(parentmodule(f))
    parts = mod_path[1] == :Main ? mod_path[2:end] : mod_path
    isempty(parts) && return string(fname)
    return join([parts..., fname], '.')
end

"""
    resolve_function(ref::String) -> Function

Resolve a function from its fully qualified reference string (e.g. `"MyModule.my_handler"`).
The module must be loaded before calling this function.
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
function nextJobExecution(store::Store, job::Job, max_failed_executions=job.options.max_failed_executions, max_executions=job.options.max_executions, expires_at=job.options.expires_at; logging::Bool=true)
    # if job is already disabled, return nothing
    isdisabled(job) && return nothing
    # check if job has expired
    if expires_at !== nothing && expires_at < Dates.now(UTC)
        logging && @info "Disabling job $(job.name) due to expiration: $(expires_at)."
        disableJob!(store, job)
        return nothing
    end
    # pull job execution history for other checks
    nexecs = max(0, something(max_failed_executions, 0), something(max_executions, 0))
    execs = getNMostRecentJobExecutions(store, job.name, nexecs)
    # check if max number of executions has been reached
    if max_executions !== nothing && count(e -> e.status == :succeeded, execs) >= max_executions
        logging && @info "Disabling job $(job.name) after reaching maximum number of successful executions: $(max_executions)."
        disableJob!(store, job)
        return nothing
    end
    # check if max number of failed executions has been reached
    if max_failed_executions !== nothing && max_executions !== nothing && max_failed_executions < max_executions
        execs = @view execs[1:min(max_failed_executions, length(execs))]
    end
    if max_failed_executions !== nothing && count(e -> e.status == :failed, execs) >= max_failed_executions
        logging && @info "Disabling job $(job.name) after reaching maximum number of failed executions: $(max_failed_executions)."
        disableJob!(store, job)
        return nothing
    end
    tz = job.options.timezone
    time = if job.schedule === nothing
        Dates.now(UTC)
    elseif tz !== nothing
        getnext(job.schedule, tz)
    else
        getnext(job.schedule)
    end
    return JobExecution(job, time)
end

"""
    disable!(job::Job)

Disables a job, preventing it from being scheduled for execution.

This mutates the `Job` object only. A persisting store holds a *copy* of the job,
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
    JobExecution(job::Job, scheduledStart::DateTime) = new("$(job.name)-$scheduledStart", job, scheduledStart, false)
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
    Store(backend; prefix="tempus/", history_limit=100)

Create job and execution-history views over one `AbstractStore` backend.
Namespacing lets Tempus share a physical store with other libraries and
application state.

The backend must accept both `Job` and `Vector{JobExecution}` values — i.e. be an
`AbstractStore{Any}` — and it must return them *as those types*. That rules out
codecs that decode at the backend's own `eltype`: `FileStore{Any}(dir;
codec=JSONCodec())` hands back `Dict{String,Any}`, not a `Job`. Use a
type-preserving backend (`MemoryStore()`, or any store with the default
`SerializedCodec`), or pass separately typed stores to `Store(jobs, executions)`.

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
    (Job <: eltype(backend) && Vector{JobExecution} <: eltype(backend)) || throw(ArgumentError(
        "a Tempus store needs a backend holding both `Tempus.Job` and " *
        "`Vector{Tempus.JobExecution}` values, but `eltype(backend)` is $(eltype(backend)). " *
        "Use an `AbstractStore{Any}` (`MemoryStore()`, `FileStore(dir)`, `SQLStore{Any}(conn)`), " *
        "or pass separately typed stores to `Tempus.Store(jobs, executions)`."))
    AbstractStores.checkstore(backend; listing=true)
    return Store(
        AbstractStores.PrefixedStore{Job}(backend, string(prefix, "jobs/")),
        AbstractStores.PrefixedStore{Vector{JobExecution}}(
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
    jobs::AbstractStores.AbstractStore{Job},
    executions::AbstractStores.AbstractStore{Vector{JobExecution}};
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

"""Return every stored job, regardless of disabled status."""
function getJobs(store::Store)
    jobs = Job[]
    for name in keys(store.jobs)
        # a concurrent `purgeJob!` can remove a key between listing and fetching
        job = get(store.jobs, name, nothing)
        job === nothing || push!(jobs, job)
    end
    return jobs
end

"""Add or replace a job by name."""
function addJob!(store::Store, job::Job)
    put!(store.jobs, job.name, job)
    return job
end

"""Remove a job and all execution history for that job."""
function purgeJob!(store::Store, job::Union{Job,AbstractString})
    name = job isa Job ? job.name : String(job)
    delete!(store.jobs, name)
    delete!(store.executions, name)
    return nothing
end

function disabled_copy(job::Job, at::DateTime)
    return Job(
        ReentrantLock(),
        job.action,
        job.action_ref,
        job.action_data,
        job.name,
        job.schedule,
        job.options,
        at,
    )
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
    updated = AbstractStores.modify!(store.jobs, name) do current
        current === nothing ? nothing : disabled_copy(current, at)
    end
    if job isa Job && updated !== nothing
        @lock job.lock job.disabledAt = at
    end
    return updated
end

"""
    storeJobExecution!(store, execution)

Prepend one execution to the job history and keep at most `history_limit`
records. The update is atomic when the selected backend provides atomic
`modify!`.

A serializing backend encodes the whole `JobExecution`, including the value the
job returned and any exception it threw, so those must be encodable by the
backend's codec.
"""
function storeJobExecution!(store::Store, execution::JobExecution)
    AbstractStores.modify!(store.executions, execution.job.name) do current
        history = current === nothing ? JobExecution[] : copy(current)
        pushfirst!(history, execution)
        resize!(history, min(length(history), store.history_limit))
        return history
    end
    return execution
end

"""Return at most `n` execution records, newest first."""
function getNMostRecentJobExecutions(
    store::Store,
    job_name::String,
    n::Int,
)
    n <= 0 && return JobExecution[]
    history = get(store.executions, job_name, nothing)
    history === nothing && return JobExecution[]
    return history[1:min(n, length(history))]
end

"""
    Scheduler

The main scheduling engine that executes jobs according to their schedules.

# Fields:
- `lock::ReentrantLock`: Ensures thread-safe access.
- `jobExecutions::Vector{JobExecution}`: List of scheduled job executions.
- `store::Store`: Job storage backend.
- `jobExecutionFinished::Threads.Event`: Signals all job executions have finished when shutting down.
- `executingJobExecutions::Set{JobExecution}`: Tracks currently executing jobs.
- `running::Bool`: Scheduler state (running/stopped).
- `loopActive::Bool`: Whether the dispatch-loop task has finished stopping.
- `jobOptions::JobOptions`: Default job execution options.
- `max_concurrent_executions::Int`: Limit on how many total executions can be running concurrently for this scheduler, defaults to `Threads.nthreads()`
- `logging::Bool`: Whether to emit log messages during scheduler operations, defaults to `true`.
"""
mutable struct Scheduler
    const lock::ReentrantLock
    const jobExecutions::Vector{JobExecution}
    const store::Store
    const jobExecutionFinished::Threads.Event
    const executingJobExecutions::Set{JobExecution}
    running::Bool
    loopActive::Bool
    const jobOptions::JobOptions
    const max_concurrent_executions::Int
    const logging::Bool
    Scheduler(
        store::Store=InMemoryStore();
        overlap_policy::Symbol=:skip,
        retries::Int = 3,
        retry_delays::Union{Base.ExponentialBackOff, Nothing}=retries == 0 ? nothing : Base.ExponentialBackOff(; n=retries),
        retry_check=nothing,
        max_failed_executions::Union{Int, Nothing}=3,
        max_executions::Union{Int, Nothing}=nothing,
        expires_at::Union{DateTime, Nothing}=nothing,
        max_concurrent_executions::Int=Threads.nthreads(),
        logging::Bool=true,
    ) = begin
        max_concurrent_executions >= 1 || throw(ArgumentError("max_concurrent_executions must be at least 1, got $max_concurrent_executions"))
        new(ReentrantLock(), JobExecution[], store, Threads.Event(), Set{JobExecution}(), false, false, JobOptions(; overlap_policy, retries, retry_delays, retry_check, max_failed_executions, max_executions, expires_at), max_concurrent_executions, logging)
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
"""
function run!(scheduler::Scheduler; close_when_no_jobs::Bool=false)
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
    errormonitor(Threads.@spawn :interactive try
        readyToExecute = Tuple{Int, Bool, JobExecution}[]
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
                    for (i, toSkip, je) in readyToExecute
                        deleteat!(scheduler.jobExecutions, i)
                        next = scheduleNextExecution!(scheduler, je.job)
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
                for (_, _, je) in readyToExecute
                    # we're ready to execute a job!
                    executeJob!(scheduler, je)
                end
            else
                # @info "No jobs to execute, sleeping 500ms then checking again."
                sleep(0.5)
            end
        end
    finally
        # the loop is the scheduler's liveness; however it exits — including a
        # store error thrown mid-iteration — the scheduler is no longer running
        # and finishing executions must be able to observe that (otherwise
        # `wait(scheduler)` never returns)
        @lock scheduler.lock begin
            scheduler.running = false
            scheduler.loopActive = false
            isempty(scheduler.executingJobExecutions) && notify(scheduler.jobExecutionFinished)
        end
    end)
    return scheduler
end

"""
    scheduleNextExecution!(scheduler::Scheduler, job::Job)

Schedule `job`'s next execution, returning it, or `nothing` when no execution
was scheduled: the job is done (see [`nextJobExecution`](@ref)), it has been
removed from the store, an execution at the same time is already queued, or —
for one-shot jobs, which are rescheduled only when an attempt fails — an
execution is already queued or running. `scheduler.lock` must be held.

The new execution is appended without re-sorting (so callers iterating
`scheduler.jobExecutions` or holding indexes into it stay valid); callers must
restore scheduledStart order before the scheduler loop scans the list again.
"""
function scheduleNextExecution!(scheduler::Scheduler, job::Job)
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
    errormonitor(Threads.@spawn begin
        now = Dates.now(UTC)
        if scheduler.logging
            if jobExecution.runConcurrently
                @info "[$(jobExecution.jobExecutionId)]: Executing job $(jobExecution.job.name) concurrently at $(now)"
            else
                @info "[$(jobExecution.jobExecutionId)]: Executing job $(jobExecution.job.name) at $(now)"
            end
        end
        retry_check = _some(jobExecution.job.options.retry_check, scheduler.jobOptions.retry_check)
        check = (eb, e) -> begin
            if isdisabled(jobExecution.job)
                scheduler.logging && @info "[$(jobExecution.jobExecutionId)]: Skipping job $(jobExecution.job.name) retry due to job being disabled"
                return false
            end
            should_retry = retry_check !== nothing ? retry_check(eb, e) : true
            if should_retry
                scheduler.logging && @info "[$(jobExecution.jobExecutionId)]: Job $(jobExecution.job.name) execution failed, retrying"
            end
            return should_retry
        end
        data = jobExecution.job.action_data
        base_action = data !== nothing ?
            () -> jobExecution.job.action(; JSON.parse(data)...) :
            jobExecution.job.action
        f = _some(jobExecution.job.options.retry_delays, scheduler.jobOptions.retry_delays) !== nothing ?
            Base.retry(base_action; delays=_some(jobExecution.job.options.retry_delays, scheduler.jobOptions.retry_delays), check=check) :
            base_action
        jobExecution.actualStart = now
        try
            jobExecution.result = Base.invokelatest(f)
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
                current_job = get(scheduler.store.jobs, jobExecution.job.name, nothing)
                if current_job === nothing
                    # The job was removed while this execution was running. Do
                    # not recreate its history or leave stale queued executions.
                    filter!(je -> je.job.name != jobExecution.job.name, scheduler.jobExecutions)
                else
                    # A persisting store serializes the execution, including
                    # whatever the job returned or threw. An unencodable value
                    # must not take the scheduler's bookkeeping down with it.
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
    end)
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

Waits for the scheduler to finish executing all jobs.
Note the scheduler must be explicitly closed to stop the scheduler loop
or pass `close_when_no_jobs=true` to `run!` to automatically close the scheduler when no jobs are left.
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
