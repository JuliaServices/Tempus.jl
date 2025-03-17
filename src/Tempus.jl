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
    JobOptions

Defines options for job execution behavior.

# Fields:
- `overlap_policy::Union{Symbol, Nothing}`: Determines job execution behavior when the same job is already running (`:skip`, `:queue`, `:concurrent`).
- `retries::Int`: Number of retries allowed on failure.
- `retry_delays::Union{Base.ExponentialBackOff, Nothing}`: Delay strategy for retries (defaults to exponential backoff if `retries > 0`).
- `retry_check`: Custom function to determine retry behavior (`check` argument from `Base.retry`).
- `on_fail_policy::Union{Tuple{Symbol, Int}, Nothing}`: Determines job failure handling (`:ignore`, `:disable`, `:unschedule`), with a threshold for consecutive failures.
- `max_executions::Union{Int, Nothing}`: Maximum number of executions allowed for a job.
- `expires_at::Union{DateTime, Nothing}`: Expiration time for a job.
"""
@kwdef struct JobOptions
    overlap_policy::Union{Symbol, Nothing} = nothing # :skip, :queue, :concurrent
    retries::Int = 0
    retry_delays::Union{Base.ExponentialBackOff, Nothing} = retries > 0 ? Base.ExponentialBackOff(; n=retries) : nothing # see Base.ExponentialBackOff
    retry_check = nothing # see Base.retry `check` keyword argument
    on_fail_policy::Union{Tuple{Symbol, Int}, Nothing} = nothing # :ignore, :disable, :unschedule + max consecutive failures for disable/unschedule
    max_executions::Union{Int, Nothing} = nothing # max number of executions job is allowed to run
    expires_at::Union{DateTime, Nothing} = nothing # expiration time for job
end

"""
    Job

Represents a scheduled job in the Tempus scheduler.

# Fields:
- `name::String`: Unique identifier for the job.
- `schedule::Cron`: The cron-style schedule expression.
- `action::Function`: The function to execute when the job runs.
- `options::JobOptions`: Execution options for retries, failures, and overlap handling.
- `disabledAt::Union{DateTime, Nothing}`: Timestamp when the job was disabled (if applicable).
"""
mutable struct Job
    const action::Function
    const name::String
    const schedule::Cron
    const options::JobOptions
    # fields managed by scheduler
    disabledAt::Union{DateTime, Nothing}
end

Job(action::Function, name, schedule; kw...) = Job(action, string(name), schedule isa Cron ? schedule : parseCron(schedule), JobOptions(kw...), nothing)

"""
    disable!(job::Job)

Disables a job, preventing it from being scheduled for execution.
"""
disable!(job::Job) = (job.disabledAt = Dates.now(UTC))

"""
    enable!(job::Job)

Enables a previously disabled job, allowing it to be scheduled again.
"""
enable!(job::Job) = (job.disabledAt = nothing)

"""
    isdisabled(job::Job) -> Bool

Returns `true` if the job is currently disabled.
"""
isdisabled(job::Job) = job.disabledAt !== nothing

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
    JobExecution(job::Job, scheduledStart::DateTime) = new("$(job.name)-$scheduledStart", job, scheduledStart, false)
end

Base.hash(je::JobExecution, h::UInt) = hash(je.jobExecutionId, h)

"""
    Store

Defines an interface for job storage backends.
"""
abstract type Store end

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
    removeJob!(store::Store, job::Union{Job, String})

Remove a `job` from `store` by reference or name.
Store implementations may choose to remove job execution history as well.
"""
function removeJob! end

# fallback for removing job by name
function removeJob!(store::Store, jobName::String)
    jobs = getJobs(store)
    job = findfirst(j -> j.name == jobName, jobs)
    job === nothing && return
    removeJob!(store, job)
    return
end

"""
    disableJob!(store::Store, job::Union{Job, String})
    
Disable a `job` in `store` by reference or name.
"""
function disableJob!(store::Store, job::Union{Job, String})
    jobName = job isa Job ? job.name : job
    jobs = getJobs(store)
    job = findfirst(j -> j.name == jobName, jobs)
    job === nothing && return
    disable!(job)
    return
end

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

"""
    removeJobExecutions!(store::Store, jobName::String)

Remove all job execution history for `jobName` from `store`.
"""
function removeJobExecutions! end

"""
    InMemoryStore <: Store

An in-memory job storage backend.

# Fields:
- `jobs::Set{Job}`: Stores active jobs.
- `jobExecutions::Dict{String, Vector{JobExecution}}`: Stores execution history for each job.
"""
struct InMemoryStore <: Store
    jobs::Set{Job}
    jobExecutions::Dict{String, Vector{JobExecution}} # job executions stored most recent first
end

InMemoryStore() = InMemoryStore(Set{Job}(), Dict{String, Vector{JobExecution}}())

addJob!(store::InMemoryStore, job::Job) = push!(store.jobs, job)
removeJob!(store::InMemoryStore, job::Job) = delete!(store.jobs, job)
getJobs(store::InMemoryStore) = store.jobs
function getNMostRecentJobExecutions(store::InMemoryStore, jobName::String, n::Int)
    execs = get(() -> JobExecution[], store.jobExecutions, jobName)
    return @view execs[1:min(n, length(execs))]
end

function storeJobExecution!(store::InMemoryStore, jobExecution::JobExecution)
    execs = get!(() -> JobExecution[], store.jobExecutions, jobExecution.job.name)
    pushfirst!(execs, jobExecution)
    return
end

function removeJobExecutions!(store::InMemoryStore, jobName::String)
    delete!(store.jobExecutions, jobName)
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
    filepath::String
    store::InMemoryStore
end

function FileStore(filepath::String)
    store = isfile(filepath) && filesize(filepath) > 0 ? deserialize(filepath) : InMemoryStore()
    return FileStore(filepath, store)
end

addJob!(store::FileStore, job::Job) = addJob!(store.store, job)
removeJob!(store::FileStore, job::Job) = removeJob!(store.store, job)
getJobs(store::FileStore) = getJobs(store.store)
getNMostRecentJobExecutions(store::FileStore, jobName::String, n::Int) = getNMostRecentJobExecutions(store.store, jobName, n)

function storeJobExecution!(store::FileStore, jobExecution::JobExecution)
    storeJobExecution!(store.store, jobExecution)
    serialize(store.filepath, store.store)
    return
end

removeJobExecutions!(store::FileStore, jobName::String) = removeJobExecutions!(store.store, jobName)

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
"""
mutable struct Scheduler
    const lock::ReentrantLock
    const jobExecutions::Vector{JobExecution}
    const store::Store
    const jobExecutionFinished::Threads.Event
    const executingJobExecutions::Set{JobExecution}
    running::Bool
    const jobOptions::JobOptions
    Scheduler(
        store::Store=InMemoryStore();
        overlap_policy::Symbol=:skip,
        retries::Int = 3,
        retry_delays::Union{Base.ExponentialBackOff, Nothing}=retries == 0 ? nothing : Base.ExponentialBackOff(; n=retries),
        retry_check=nothing,
        on_fail_policy::Union{Tuple{Symbol, Int}, Nothing}=(:ignore, 0),
    ) = new(ReentrantLock(), JobExecution[], store, Threads.Event(), Set{JobExecution}(), false, JobOptions(; overlap_policy, retries, retry_delays, retry_check, on_fail_policy))
end

"""
    removeJob!(scheduler::Scheduler, job::Union{Job, String})

Removes a job from the scheduler, preventing it from being executed in the future.
This function does *not* remove the job from the scheduler's Store.
"""
function removeJob!(scheduler::Scheduler, job::Union{Job, String})
    jobName = job isa Job ? job.name : job
    @lock scheduler.lock begin
        # find job executions for job to remove
        inds = findall(je -> je.job.name == jobName, scheduler.jobExecutions)
        # remove job executions
        deleteat!(scheduler.jobExecutions, inds)
    end
    return
end

# check if job should be disabled based on on_fail_policy and job execution history
function checkDisable!(scheduler::Scheduler, store::Store, job::Job, on_fail_policy::Union{Tuple{Symbol, Int}, Nothing})
    (on_fail_policy === nothing || on_fail_policy[1] === :ignore) && return
    execs = getNMostRecentJobExecutions(store, job.name, on_fail_policy === nothing ? 0 : on_fail_policy[2])
    if all(e -> e.status == :failed, execs)
        if on_fail_policy[1] === :disable
            disable!(job)
            n = on_fail_policy[2]
            @info "Disabling job $(job.name) after $(n) $(n == 1 ? "failure." : "consecutive failures.")"
            return :disabled
        elseif on_fail_policy[1] === :unschedule
            # unscheduling due to failure also disables job
            disable!(job)
            unschedule!(scheduler, job)
            n = on_fail_policy[2]
            @info "Unscheduling job $(job.name) after $(n) $(n == 1 ? "failure." : "consecutive failures.")"
            return :unscheduled
        end
    end
    return
end

function checkExpirationAndMaxExecutions!(scheduler::Scheduler, job::Job)
    max_executions = _some(job.options.max_executions, scheduler.jobOptions.max_executions)
    expires_at = _some(job.options.expires_at, scheduler.jobOptions.expires_at)
    if max_executions !== nothing
        execs = getNMostRecentJobExecutions(scheduler.store, job.name, max_executions)
        if length(execs) >= max_executions
            unschedule!(scheduler, job)
            @info "Unscheduling job $(job.name) after reaching maximum executions of $(max_executions)."
            return true
        end
    end
    if expires_at !== nothing && expires_at < Dates.now(UTC)
        unschedule!(scheduler, job)
        @info "Unscheduling job $(job.name) after expiration at $(expires_at)."
        return true
    end
    return false
end

"""
    run!(scheduler::Scheduler)

Starts the scheduler, executing jobs at their scheduled times.
"""
function run!(scheduler::Scheduler)
    @info "Starting scheduler and all jobs."
    reset(scheduler.jobExecutionFinished)
    jobs = getJobs(scheduler.store)
    # generate initial JobExecution list
    @lock scheduler.lock begin
        scheduler.running = true
        empty!(scheduler.executingJobExecutions)
        empty!(scheduler.jobExecutions)
        for job in jobs
            # recalculate disabled status for job based on on_fail_policy and job execution history
            checkDisable!(scheduler, scheduler.store, job, _some(job.options.on_fail_policy, scheduler.jobOptions.on_fail_policy)) == :unschedule && continue
            push!(scheduler.jobExecutions, JobExecution(job, getnext(job.schedule)))
        end
        sort!(scheduler.jobExecutions, by=je -> je.scheduledStart)
    end
    # start scheduler job execution task
    errormonitor(Threads.@spawn :interactive begin
        readyToExecute = Tuple{Int, Bool, JobExecution}[]
        while true
            empty!(readyToExecute)
            now = trunc(Dates.now(UTC), Second)
            @lock scheduler.lock begin
                scheduler.running || break
                # check for jobs that are ready to execute
                for (i, je) in enumerate(scheduler.jobExecutions)
                    if je.scheduledStart <= now
                        if je.job.disabledAt !== nothing
                            push!(readyToExecute, (i, true, je))
                        elseif any(j -> j.job.name == je.job.name, scheduler.executingJobExecutions)
                            if _some(je.job.options.overlap_policy, scheduler.jobOptions.overlap_policy) == :skip
                                push!(readyToExecute, (i, true, je))
                            elseif _some(je.job.options.overlap_policy, scheduler.jobOptions.overlap_policy) == :concurrent
                                push!(readyToExecute, (i, false, je))
                                push!(scheduler.executingJobExecutions, je)
                                je.runConcurrently = true
                            elseif _some(je.job.options.overlap_policy, scheduler.jobOptions.overlap_policy) == :queue
                                nexecs = count(j -> j.job.name == je.job.name, scheduler.jobExecutions)
                                @warn "Job $(je.job.name) already executing, keeping scheduled execution queued until current execution finishes. There are $nexecs queued for this job."
                                # check if we need to schedule the next execution
                                next = getnext(je.job.schedule)
                                # check job expiration and max executions
                                unscheduled = checkExpirationAndMaxExecutions!(scheduler, je.job)
                                if unscheduled || any(j -> j.job.name == je.job.name && j.scheduledStart == next, scheduler.jobExecutions)
                                    # job is unscheduled or we've already scheduled this execution, skip
                                else
                                    push!(scheduler.jobExecutions, JobExecution(je.job, next))
                                end
                            end
                        else
                            push!(readyToExecute, (i, false, je))
                            push!(scheduler.executingJobExecutions, je)
                        end
                    else
                        break
                    end
                end
                # remove job executions that are ready or to be skipped while holding the lock and schedule next execution
                if !isempty(readyToExecute)
                    for (i, toSkip, je) in readyToExecute
                        deleteat!(scheduler.jobExecutions, i)
                        next = getnext(je.job.schedule)
                        # check job expiration and max executions
                        checkExpirationAndMaxExecutions!(scheduler, je.job) && continue
                        newje = JobExecution(je.job, next)
                        push!(scheduler.jobExecutions, newje)
                        if je.job.disabledAt !== nothing
                            @info "[$(newje.jobExecutionId)]: Skipping disabled job $(je.job.name) (disabled at $(je.job.disabledAt)) execution at $(now), next scheduled at $(next)"
                        elseif toSkip
                            @info "[$(newje.jobExecutionId)]: Skipping job $(je.job.name) execution at $(now), next scheduled at $(next)"
                        else
                            @info "[$(newje.jobExecutionId)]: Job $(je.job.name) execution scheduled at $(next)"
                        end
                    end
                    sort!(scheduler.jobExecutions, by=je -> je.scheduledStart)
                end
            end
            filter!(x -> !x[2], readyToExecute)
            if !isempty(readyToExecute)
                for (_, _, je) in readyToExecute
                    # we're ready to execute a job!
                    executeJob!(scheduler, je)
                end
            else
                @info "No jobs to execute, sleeping 500ms then checking again."
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
        if jobExecution.runConcurrently
            @info "[$(jobExecution.jobExecutionId)]: Executing job $(jobExecution.job.name) concurrently with previous execution at $(now)"
        else
            @info "[$(jobExecution.jobExecutionId)]: Executing job $(jobExecution.job.name) at $(now)"
        end
        retry_check = _some(jobExecution.job.options.retry_check, scheduler.jobOptions.retry_check)
        check = (eb, e) -> begin
            if isdisabled(jobExecution.job)
                @info "[$(jobExecution.jobExecutionId)]: Skipping job $(jobExecution.job.name) retry due to job being disabled"
                return false
            end
            @warn "[$(jobExecution.jobExecutionId)]: Job $(jobExecution.job.name) execution failed, retrying" exception=(e, catch_backtrace())
            return retry_check !== nothing ? retry_check(eb, e) : true
        end
        f = _some(jobExecution.job.options.retry_delays, scheduler.jobOptions.retry_delays) !== nothing ?
            Base.retry(jobExecution.job.action; delays=_some(jobExecution.job.options.retry_delays, scheduler.jobOptions.retry_delays), check=check) :
            jobExecution.job.action
        jobExecution.actualStart = now
        try
            Base.invokelatest(f)
            jobExecution.status = :succeeded
        catch e
            jobExecution.status = :failed
            @error "[$(jobExecution.jobExecutionId)]: Job $(jobExecution.job.name) execution failed" exception=(e, catch_backtrace())
        finally
            jobExecution.finish = Dates.now(UTC)
            @info "[$(jobExecution.jobExecutionId)]: Job $(jobExecution.job.name) execution finished at $(jobExecution.finish)"
        end
        # store the job execution
        storeJobExecution!(scheduler.store, jobExecution)
        # check if job should be disabled or unscheduled based on on_fail_policy
        if jobExecution.status == :failed
            checkDisable!(scheduler, scheduler.store, jobExecution.job, _some(jobExecution.job.options.on_fail_policy, scheduler.jobOptions.on_fail_policy))
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
    @info "Closing scheduler and waiting $(timeout)s for job executions to stop."
    @lock scheduler.lock begin
        scheduler.running = false
    end
    # we use a Timer here to notify jobExecutionFinished ourself if the scheduler
    # or last executing job doesn't do it themselves in time
    Timer(timeout) do t
        @warn "Scheduler closing timeout reached, returning without waiting for job executions to finish."
        notify(scheduler.jobExecutionFinished)
    end
    wait(scheduler.jobExecutionFinished)
    @info "Scheduler closed and job execution stopped."
    return
end

"""
    push!(scheduler::Scheduler, job::Job)

Adds a job to the scheduler and underlying Store, scheduling its next execution based on its cron schedule.
"""
function Base.push!(scheduler::Scheduler, job::Job)
    @lock scheduler.lock begin
        addJob!(scheduler.store, job)
        next = getnext(job.schedule)
        je = JobExecution(job, next)
        push!(scheduler.jobExecutions, je)
        @info "[$(je.jobExecutionId)]: Adding job $(job.name) to scheduler and scheduling next execution at $(next)."
        sort!(scheduler.jobExecutions, by=je -> je.scheduledStart)
    end
    return job
end

"""
    unschedule!(scheduler::Scheduler, job::Union{Job, String})

Removes a job from the scheduler and underlying Store, preventing it from being executed
in the future. Any currently scheduled executions of this job will 
also be removed from the scheduler. Note it is Store-dependent whether job execution history is removed.
"""
function unschedule!(scheduler::Scheduler, job::Union{Job, String})
    jobName = job isa Job ? job.name : job
    @info "Removing job $(jobName) from scheduler."
    @lock scheduler.lock begin
        removeJob!(scheduler.store, job)
        removeJob!(scheduler, job)
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

end # module
