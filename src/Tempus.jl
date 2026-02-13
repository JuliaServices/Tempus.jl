"""
Tempus provides a cron-style job scheduling framework for Julia, inspired by Quartz in Java. 

## Features:
- Define jobs with cron-like scheduling expressions
- Supports job execution policies (overlap handling, retries, and failure strategies)
- Multiple job stores and JobStore interface (in-memory, file-based persistence)
- Concurrency-aware execution with configurable retry logic
- Supports disabling, enabling, and unscheduling jobs dynamically
- Thread-safe scheduling with a background execution loop
"""
module Tempus

using Dates, Logging, Serialization

export withscheduler

include("cron.jl")

_some(x, y...) = x === nothing ? _some(y...) : x
_some(x) = x

"""
    Store

Defines an interface for job storage backends.
"""
abstract type Store end

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
"""
@kwdef struct JobOptions
    overlap_policy::Union{Symbol, Nothing} = nothing # :skip, :queue, :concurrent
    retries::Int = 0
    retry_delays::Union{Base.ExponentialBackOff, Nothing} = retries > 0 ? Base.ExponentialBackOff(; n=retries) : nothing # see Base.ExponentialBackOff
    retry_check = nothing # see Base.retry `check` keyword argument
    max_failed_executions::Union{Int, Nothing} = nothing # max number of failed executions allowed for a job before it will be disabled
    max_executions::Union{Int, Nothing} = nothing # max number of _successful_ executions job is allowed to run
    expires_at::Union{DateTime, Nothing} = nothing # expiration time for job
end

Base.show(io::IO, opts::JobOptions) = print(io, "Tempus.JobOptions(overlap_policy=$(opts.overlap_policy), retries=$(opts.retries), retry_delays=$(opts.retry_delays), retry_check=$(opts.retry_check), max_failed_executions=$(opts.max_failed_executions), max_executions=$(opts.max_executions), expires_at=$(opts.expires_at))")

"""
    Job

Represents a single job/unit of work. Can be scheduled to repeat.

# Fields:
- `name::String`: Unique identifier for the job.
- `schedule::Cron`: The cron-style schedule expression.
- `action::Function`: The function to execute when the job runs.
- `options::JobOptions`: Execution options for retries, failures, and overlap handling.
- `disabledAt::Union{DateTime, Nothing}`: Timestamp when the job was disabled (if applicable).
"""
mutable struct Job
    const lock::ReentrantLock
    const action::Function
    const name::String
    const schedule::Union{Cron, Nothing}
    const options::JobOptions
    # fields managed by scheduler
    disabledAt::Union{DateTime, Nothing}
end

Job(action::Function, name, schedule; kw...) = Job(ReentrantLock(), action, string(name), schedule isa Cron ? schedule : parseCron(schedule), JobOptions(; kw...), nothing)
OneShotJob(action::Function, name; kw...) = Job(ReentrantLock(), action, string(name), nothing, JobOptions(; max_executions=1, kw...), nothing)

function Base.show(io::IO, job::Job)
    println(io, "Job: $(job.name)")
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

# for a given `job`` persisted in `store`, check status of job and return the next DateTime when it should be executed
# `nothing` is returned if the job shouldn't be scheduled again
function nextJobExecution(store::Store, job::Job, max_failed_executions=job.options.max_failed_executions, max_executions=job.options.max_executions, expires_at=job.options.expires_at; logging::Bool=true)
    # if job is already disabled, return nothing
    isdisabled(job) && return nothing
    # check if job has expired
    if expires_at !== nothing && expires_at < Dates.now(UTC)
        logging && @info "Disabling job $(job.name) due to expiration: $(expires_at)."
        disable!(job)
        return nothing
    end
    # pull job execution history for other checks
    nexecs = max(0, something(max_failed_executions, 0), something(max_executions, 0))
    execs = getNMostRecentJobExecutions(store, job.name, nexecs)
    # check if max number of executions has been reached
    if max_executions !== nothing && count(e -> e.status == :succeeded, execs) >= max_executions
        logging && @info "Disabling job $(job.name) after reaching maximum number of successful executions: $(max_executions)."
        disable!(job)
        return nothing
    end
    # check if max number of failed executions has been reached
    if something(max_failed_executions, 0) < something(max_executions, 0)
        execs = execs[1:max_failed_executions]
    end
    if max_failed_executions !== nothing && count(e -> e.status == :failed, execs) >= max_failed_executions
        logging && @info "Disabling job $(job.name) after reaching maximum number of failed executions: $(max_failed_executions)."
        disable!(job)
        return nothing
    end
    time = job.schedule === nothing ? Dates.now(UTC) : getnext(job.schedule)
    return JobExecution(job, time)
end

"""
    disable!(job::Job)

Disables a job, preventing it from being scheduled for execution.
"""
disable!(job::Job) = (@lock job.lock (job.disabledAt = Dates.now(UTC)))

"""
    enable!(job::Job)

Enables a previously disabled job, allowing it to be scheduled again.
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

# interface for Stores
"""
    getJobs(store::Store) -> Collection{Job}

Retrieve all jobs stored in `store`, regardless of disabled status.
"""
function getJobs end

"""
    addJob!(store::Store, job::Job)

Add a new `job` to `store`.
"""
function addJob! end

"""
    purgeJob!(store::Store, job::Union{Job, String})

Remove a `job` from `store` by reference or name.
All job execution history will also be removed.
"""
function purgeJob! end

"""
    storeJobExecution!(store::Store, jobExecution::JobExecution)

Store `jobExecution` in `store`.
"""
function storeJobExecution!(store::Store, jobExecution::JobExecution) end

"""
    getNMostRecentJobExecutions(store::Store, jobName::String, n::Int) -> Vector{JobExecution}

Get the `n` most recent job executions for a job persisted in `store`.
"""
function getNMostRecentJobExecutions(store::Store, jobName::String, n::Int) end

# fallback for purging job by name
function purgeJob!(store::Store, jobName::String)
    jobs = getJobs(store)
    for job in jobs
        if job.name == jobName
            purgeJob!(store, job)
            return
        end
    end
    return
end

"""
    disableJob!(store::Store, job::Union{Job, String})
    
Disable a `job` in `store` by reference or name.
"""
function disableJob!(store::Store, job::Union{Job, String})
    jobName = job isa Job ? job.name : job
    jobs = getJobs(store)
    for j in jobs
        if j.name == jobName
            disable!(j)
            return
        end
    end
    return
end

"""
    InMemoryStore <: Store

An in-memory job storage backend.

# Fields:
- `jobs::Set{Job}`: Stores active jobs.
- `jobExecutions::Dict{String, Vector{JobExecution}}`: Stores execution history for each job.
"""
struct InMemoryStore <: Store
    lock::ReentrantLock
    jobs::Set{Job}
    jobExecutions::Dict{String, Vector{JobExecution}} # job executions stored most recent first
end

InMemoryStore() = InMemoryStore(ReentrantLock(), Set{Job}(), Dict{String, Vector{JobExecution}}())

addJob!(store::InMemoryStore, job::Job) = @lock store.lock push!(store.jobs, job)

function purgeJob!(store::InMemoryStore, job::Job)
    @lock store.lock begin
        delete!(store.jobs, job)
        delete!(store.jobExecutions, job.name)
    end
end

getJobs(store::InMemoryStore) = store.jobs

function getNMostRecentJobExecutions(store::InMemoryStore, jobName::String, n::Int)
    n == 0 && return JobExecution[]
    execs = @lock store.lock get(() -> JobExecution[], store.jobExecutions, jobName)
    return @view execs[1:min(n, length(execs))]
end

function storeJobExecution!(store::InMemoryStore, jobExecution::JobExecution)
    @lock store.lock begin
        execs = get!(() -> JobExecution[], store.jobExecutions, jobExecution.job.name)
        pushfirst!(execs, jobExecution)
    end
    return
end

"""
    FileStore <: Store

A file-based job storage backend that persists jobs and execution history to disk.

# Fields:
- `filepath::String`: The file path where jobs are serialized.
- `store::InMemoryStore`: In-memory store that handles operations before syncing to disk.
"""
struct FileStore <: Store
    lock::ReentrantLock
    filepath::String
    store::InMemoryStore
end

function FileStore(filepath::String)
    store = isfile(filepath) && filesize(filepath) > 0 ? deserialize(filepath) : InMemoryStore()
    return FileStore(ReentrantLock(), filepath, store)
end

function addJob!(store::FileStore, job::Job)
    @lock store.lock begin
        addJob!(store.store, job)
        serialize(store.filepath, store.store)
    end
end

function purgeJob!(store::FileStore, job::Job)
    @lock store.lock begin
        purgeJob!(store.store, job)
        serialize(store.filepath, store.store)
    end
end

getJobs(store::FileStore) = getJobs(store.store)
getNMostRecentJobExecutions(store::FileStore, jobName::String, n::Int) = getNMostRecentJobExecutions(store.store, jobName, n)

function storeJobExecution!(store::FileStore, jobExecution::JobExecution)
    @lock store.lock begin
        storeJobExecution!(store.store, jobExecution)
        serialize(store.filepath, store.store)
    end
    return
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
    ) = new(ReentrantLock(), JobExecution[], store, Threads.Event(), Set{JobExecution}(), false, JobOptions(; overlap_policy, retries, retry_delays, retry_check, max_failed_executions, max_executions, expires_at), max_concurrent_executions, logging)
end

function Base.show(io::IO, scheduler::Scheduler)
    println(io, "Scheduler:")
    println(io, "  Jobs: $(length(scheduler.jobExecutions))")
    println(io, "  Running: $(scheduler.running)")
    return
end

"""
    run!(scheduler::Scheduler)

Starts the scheduler, executing jobs at their scheduled times.
"""
function run!(scheduler::Scheduler; close_when_no_jobs::Bool=false)
    scheduler.logging && @info "Starting scheduler and all jobs."
    reset(scheduler.jobExecutionFinished)
    jobs = getJobs(scheduler.store)
    # generate initial JobExecution list
    @lock scheduler.lock begin
        scheduler.running = true
        empty!(scheduler.executingJobExecutions)
        empty!(scheduler.jobExecutions)
        for job in jobs
            # get next job execution for each job
            je = nextJobExecution(scheduler, job)
            if je !== nothing
                push!(scheduler.jobExecutions, je)
            end
        end
        sort!(scheduler.jobExecutions, by=je->je.scheduledStart)
    end
    # start scheduler job execution task
    errormonitor(Threads.@spawn :interactive begin
        readyToExecute = Tuple{Int, Bool, JobExecution}[]
        while true
            empty!(readyToExecute)
            now = trunc(Dates.now(UTC), Second)
            @lock scheduler.lock begin
                scheduler.running || break
                if isempty(scheduler.jobExecutions) && close_when_no_jobs
                    scheduler.logging && @info "No jobs left to execute, closing scheduler."
                    break
                end
                # check for jobs that are ready to execute
                for (i, je) in enumerate(scheduler.jobExecutions)
                    if je.scheduledStart <= now
                        if isdisabled(je.job)
                            push!(readyToExecute, (i, true, je))
                        elseif length(scheduler.executingJobExecutions) >= scheduler.max_concurrent_executions
                            # scheduler is already executing at limit, keep execution queued, but check to schedule next execution
                            next = nextJobExecution(scheduler, je.job)
                            if next !== nothing && !any(j -> j.job.name == je.job.name && j.scheduledStart == next, scheduler.jobExecutions)
                                push!(scheduler.jobExecutions, next)
                            end
                        elseif any(j -> j.job.name == je.job.name, scheduler.executingJobExecutions)
                            if _some(je.job.options.overlap_policy, scheduler.jobOptions.overlap_policy) == :skip
                                push!(readyToExecute, (i, true, je))
                            elseif _some(je.job.options.overlap_policy, scheduler.jobOptions.overlap_policy) == :concurrent
                                push!(readyToExecute, (i, false, je))
                                push!(scheduler.executingJobExecutions, je)
                                je.runConcurrently = true
                            elseif _some(je.job.options.overlap_policy, scheduler.jobOptions.overlap_policy) == :queue
                                nexecs = count(j -> j.job.name == je.job.name, scheduler.jobExecutions)
                                scheduler.logging && @warn "Job $(je.job.name) already executing, keeping scheduled execution queued until current execution finishes. There are $nexecs queued for this job."
                                next = nextJobExecution(scheduler, je.job)
                                if next !== nothing && !any(j -> j.job.name == je.job.name && j.scheduledStart == next, scheduler.jobExecutions)
                                    push!(scheduler.jobExecutions, next)
                                end
                            end
                        else
                            push!(readyToExecute, (i, false, je))
                            push!(scheduler.executingJobExecutions, je)
                        end
                    elseif je.scheduledStart > now
                        break
                    end
                end
                # remove job executions that are ready or to be skipped while holding the lock and schedule next execution
                if !isempty(readyToExecute)
                    for (i, toSkip, je) in readyToExecute
                        deleteat!(scheduler.jobExecutions, i)
                        next = nextJobExecution(scheduler, je.job)
                        if next !== nothing && !any(j -> j.job.name == je.job.name && j.scheduledStart == next, scheduler.jobExecutions)
                            push!(scheduler.jobExecutions, next)
                        end
                        if scheduler.logging
                            if isdisabled(je.job) !== nothing
                                @info "[$(je.jobExecutionId)]: Skipping disabled job $(je.job.name) (disabled at $(je.job.disabledAt)) execution at $(now), next scheduled at $(next.scheduledStart)"
                            elseif toSkip
                                @info "[$(je.jobExecutionId)]: Skipping job $(je.job.name) execution at $(now), next scheduled at $(next.scheduledStart)"
                            else
                                @info "[$(je.jobExecutionId)]: Job $(je.job.name) execution scheduled at $(next.scheduledStart)"
                            end
                        end
                    end
                    sort!(scheduler.jobExecutions, by=je->je.scheduledStart)
                end
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
        @lock scheduler.lock begin
            isempty(scheduler.executingJobExecutions) && notify(scheduler.jobExecutionFinished)
        end
    end)
    return scheduler
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
        f = _some(jobExecution.job.options.retry_delays, scheduler.jobOptions.retry_delays) !== nothing ?
            Base.retry(jobExecution.job.action; delays=_some(jobExecution.job.options.retry_delays, scheduler.jobOptions.retry_delays), check=check) :
            jobExecution.job.action
        jobExecution.actualStart = now
        try
            jobExecution.result = Base.invokelatest(f)
            jobExecution.status = :succeeded
            jobExecution.exception = nothing
        catch e
            jobExecution.exception = e
            jobExecution.status = :failed
            scheduler.logging && @error "[$(jobExecution.jobExecutionId)]: Job $(jobExecution.job.name) execution failed" exception=(e, catch_backtrace())
        finally
            jobExecution.finish = Dates.now(UTC)
            scheduler.logging && @info "[$(jobExecution.jobExecutionId)]: Job $(jobExecution.job.name) execution finished at $(jobExecution.finish)"
        end
        # store the job execution
        storeJobExecution!(scheduler.store, jobExecution)
        # check if job should be disabled or unscheduled based on on_fail_policy
        if jobExecution.status == :failed
            # getting the next job execution checks if it should be disabled
            nextJobExecution(scheduler, jobExecution.job)
        end
        @lock scheduler.lock begin
            delete!(scheduler.executingJobExecutions, jobExecution)
            !scheduler.running && isempty(scheduler.executingJobExecutions) && notify(scheduler.jobExecutionFinished)
        end
    end)
    return
end

"""
    close(scheduler::Scheduler)

Closes the scheduler, stopping job execution; waits for any currently executing jobs to finish.
Will wait `timeout` seconds (5 by default) for any currently executing jobs to finish before returning.
"""
function Base.close(scheduler::Scheduler; timeout::Real=5)
    scheduler.logging && @info "Closing scheduler and waiting $(timeout)s for job executions to stop."
    @lock scheduler.lock begin
        scheduler.running = false
    end
    # we use a Timer here to notify jobExecutionFinished ourself if the scheduler
    # or last executing job doesn't do it themselves in time
    Timer(timeout) do t
        if isopen(t)
            scheduler.logging && @warn "Scheduler closing timeout reached, returning without waiting for job executions to finish."
            notify(scheduler.jobExecutionFinished)
        end
    end
    wait(scheduler.jobExecutionFinished)
    scheduler.logging && @info "Scheduler closed and job execution stopped."
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
"""
function Base.push!(scheduler::Scheduler, job::Job)
    @lock scheduler.lock begin
        addJob!(scheduler.store, job)
        next = nextJobExecution(scheduler, job)
        if next === nothing
            return job
        end
        push!(scheduler.jobExecutions, next)
        scheduler.logging && @info "[$(next.jobExecutionId)]: Adding job $(job.name) to scheduler and scheduling next execution at $(next.scheduledStart)."
        sort!(scheduler.jobExecutions, by=je->je.scheduledStart)
    end
    return job
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
    withscheduler(store; kw...) do sched
        run!(sched; close_when_no_jobs=true)
        wait(sched)
    end
    return jobs
end

"""
    SQLiteStore <: Store

A SQLite-backed job storage that uses an InMemoryStore as cache.
The `db` field holds the SQLite.DB (typed as `Any` to avoid hard dependency).
The extension `TempusSQLiteExt` provides the constructor and all store methods.
"""
struct SQLiteStore <: Store
    lock::ReentrantLock
    db::Any  # SQLite.DB — typed Any to avoid hard dep
    cache::InMemoryStore
end

end # module
