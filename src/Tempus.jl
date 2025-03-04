module Tempus

using Dates, Logging, Serialization

include("cron.jl")

_some(x, y...) = x === nothing ? _some(y...) : x
_some(x) = x

@kwdef struct JobOptions
    overlap_policy::Union{Symbol, Nothing} = nothing # :skip, :queue, :concurrent
    retry_delays::Union{Base.ExponentialBackOff, Nothing} = nothing # see Base.ExponentialBackOff
    retry_check = nothing # see Base.retry `check` keyword argument
    on_fail_policy::Union{Tuple{Symbol, Int}, Nothing} = nothing # :ignore, :disable, :unschedule + max consecutive failures for disable/unschedule
end

mutable struct Job
    const name::Symbol
    const schedule::Cron
    const action::Function
    const options::JobOptions
    # fields managed by scheduler
    disabledAt::Union{DateTime, Nothing}
end

Job(name, schedule, action::Function; kw...) = Job(Symbol(name), schedule isa Cron ? schedule : parseCron(schedule), action, JobOptions(kw...), nothing)
Job(action::Function, name, schedule; kw...) = Job(name, schedule, action; kw...)
disable!(job::Job) = (job.disabledAt = Dates.now(UTC))
enable!(job::Job) = (job.disabledAt = nothing)

Base.hash(j::Job, h::UInt) = hash(j.name, h)

mutable struct JobExecution
    const job::Job
    const scheduledStart::DateTime
    runConcurrently::Bool
    actualStart::DateTime
    finish::DateTime
    status::Symbol # :succeeded, :failed
    JobExecution(job::Job, scheduledStart::DateTime) = new(job, scheduledStart, false)
end

abstract type Store end

function getJobs end
function addJob! end
function removeJob! end

# fallback for removing job by name
function removeJob!(store::Store, jobName::Symbol)
    jobs = getJobs(store)
    job = findfirst(j -> j.name == jobName, jobs)
    job === nothing && return
    removeJob!(store, job)
    return
end

function storeJobExecution! end
function getNMostRecentJobExecutions end

struct InMemoryStore <: Store
    jobs::Set{Job}
    jobExecutions::Dict{Symbol, Vector{JobExecution}} # job executions stored most recent first
end

InMemoryStore() = InMemoryStore(Set{Job}(), Dict{Symbol, Vector{JobExecution}}())

addJob!(store::InMemoryStore, job::Job) = push!(store.jobs, job)
removeJob!(store::InMemoryStore, job::Job) = delete!(store.jobs, job)
getJobs(store::InMemoryStore) = store.jobs
function getNMostRecentJobExecutions(store::InMemoryStore, jobName::Symbol, n::Int)
    execs = get(() -> JobExecution[], store.jobExecutions, jobName)
    return @view execs[1:min(n, length(execs))]
end

function storeJobExecution!(store::InMemoryStore, jobExecution::JobExecution)
    execs = get!(() -> JobExecution[], store.jobExecutions, jobExecution.job.name)
    pushfirst!(execs, jobExecution)
    return
end

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
getNMostRecentJobExecutions(store::FileStore, jobName::Symbol, n::Int) = getNMostRecentJobExecutions(store.store, jobName, n)

function storeJobExecution!(store::FileStore, jobExecution::JobExecution)
    storeJobExecution!(store.store, jobExecution)
    serialize(store.filepath, store.store)
    return
end

mutable struct Scheduler
    const lock::ReentrantLock
    const jobExecutions::Vector{JobExecution}
    const store::Store
    const jobExecutionFinished::Threads.Event
    const executingJobs::Set{Symbol}
    running::Bool
    const jobOptions::JobOptions
    Scheduler(
        store::Store=InMemoryStore();
        overlap_policy::Symbol=:skip,
        retry_delays::Union{Base.ExponentialBackOff, Nothing}=Base.ExponentialBackOff(; n=3),
        retry_check=nothing,
        on_fail_policy::Union{Tuple{Symbol, Int}, Nothing}=(:ignore, 0),
    ) = new(ReentrantLock(), JobExecution[], store, Threads.Event(), Set{Symbol}(), false, JobOptions(; overlap_policy, retry_delays, retry_check, on_fail_policy))
end

function checkDisable!(scheduler::Scheduler, store::Store, job::Job, on_fail_policy::Union{Tuple{Symbol, Int}, Nothing})
    (on_fail_policy === nothing || on_fail_policy[1] === :ignore) && return
    execs = getNMostRecentJobExecutions(store, job.name, on_fail_policy === nothing ? 0 : on_fail_policy[2])
    if all(e -> e.status == :failed, execs)
        if on_fail_policy[1] === :disable
            disable!(job)
            @info "Disabling job $(job.name) after $(on_fail_policy[2]) consecutive failures."
            return :disabled
        elseif on_fail_policy[1] === :unschedule
            unschedule!(scheduler, job)
            @info "Unscheduling job $(job.name) after $(on_fail_policy[2]) consecutive failures."
            return :unscheduled
        end
    end
    return
end

function run!(scheduler::Scheduler)
    @info "Starting scheduler and all jobs."
    reset(scheduler.jobExecutionFinished)
    jobs = getJobs(scheduler.store)
    # generate initial JobExecution list
    @lock scheduler.lock begin
        scheduler.running = true
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
                        elseif je.job.name in scheduler.executingJobs
                            if _some(je.job.options.overlap_policy, scheduler.jobOptions.overlap_policy) == :skip
                                push!(readyToExecute, (i, true, je))
                            elseif _some(je.job.options.overlap_policy, scheduler.jobOptions.overlap_policy) == :concurrent
                                push!(readyToExecute, (i, false, je))
                                je.runConcurrently = true
                            elseif _some(je.job.options.overlap_policy, scheduler.jobOptions.overlap_policy) == :queue
                                @info "Job $(je.job.name) already executing, keeping scheduled execution queued until current execution finishes."
                            end
                        else
                            push!(readyToExecute, (i, false, je))
                            push!(scheduler.executingJobs, je.job.name)
                        end
                    end
                end
                # remove job executions that are ready or to be skipped while holding the lock and schedule next execution
                if !isempty(readyToExecute)
                    for (i, toSkip, je) in readyToExecute
                        deleteat!(scheduler.jobExecutions, i)
                        next = getnext(je.job.schedule)
                        push!(scheduler.jobExecutions, JobExecution(je.job, next))
                        if je.job.disabledAt !== nothing
                            @info "Skipping disabled job $(je.job.name) (disabled at $(je.job.disabledAt)) execution at $(now), next scheduled at $(next)"
                        elseif toSkip
                            @info "Skipping job $(je.job.name) execution at $(now), next scheduled at $(next)"
                        else
                            @info "Job $(je.job.name) execution scheduled at $(next)"
                        end
                    end
                    sort!(scheduler.jobExecutions, by=je -> je.scheduledStart)
                end
            end
            filter!(x -> !x[2], readyToExecute)
            if !isempty(readyToExecute)
                for (_, _, je) in readyToExecute
                    # we're ready to execute a job!
                    run!(scheduler, je)
                end
            else
                @info "No jobs to execute, sleeping 500ms then checking again."
                sleep(0.5)
            end
        end
        notify(scheduler.jobExecutionFinished)
    end)
    return scheduler
end

function run!(scheduler::Scheduler, jobExecution::JobExecution)
    errormonitor(Threads.@spawn begin
        now = Dates.now(UTC)
        if jobExecution.runConcurrently
            @info "Executing job $(jobExecution.job.name) concurrently with previous execution at $(now)"
        else
            @info "Executing job $(jobExecution.job.name) at $(now)"
        end
        f = _some(jobExecution.job.options.retry_delays, scheduler.jobOptions.retry_delays) !== nothing ?
            Base.retry(jobExecution.job.action; delays=_some(jobExecution.job.options.retry_delays, scheduler.jobOptions.retry_delays), check=_some(jobExecution.job.options.retry_check, scheduler.jobOptions.retry_check)) :
            jobExecution.job.action
        jobExecution.actualStart = now
        try
            Base.invokelatest(f)
            jobExecution.status = :succeeded
        catch e
            jobExecution.status = :failed
            @error "Job $(jobExecution.job.name) execution failed" exception=(e, catch_backtrace())
        finally
            jobExecution.finish = Dates.now(UTC)
            @info "Job $(jobExecution.job.name) execution finished at $(jobExecution.finish)"
        end
        # store the job execution
        storeJobExecution!(scheduler.store, jobExecution)
        # check if job should be disabled or unscheduled based on on_fail_policy
        if jobExecution.status == :failed
            checkDisable!(scheduler, scheduler.store, jobExecution.job, _some(jobExecution.job.options.on_fail_policy, scheduler.jobOptions.on_fail_policy))
        end
        @lock scheduler.lock begin
            delete!(scheduler.executingJobs, jobExecution.job.name)
        end
    end)
    return
end

function Base.close(scheduler::Scheduler)
    @info "Closing scheduler and stopping job execution."
    @lock scheduler.lock begin
        scheduler.running = false
    end
    # check for any executing jobs
    while true
        @lock scheduler.lock begin
            isempty(scheduler.executingJobs) && break
        end
        @debug "Waiting for executing jobs to finish."
        sleep(0.5)
    end
    wait(scheduler.jobExecutionFinished)
    @info "Scheduler closed and job execution stopped."
    return
end

function Base.push!(scheduler::Scheduler, job::Job)
    @info "Adding job $(job.name) to scheduler and scheduling next execution."
    @lock scheduler.lock begin
        addJob!(scheduler.store, job)
        push!(scheduler.jobExecutions, JobExecution(job, getnext(job.schedule)))
        sort!(scheduler.jobExecutions, by=je -> je.scheduledStart)
    end
    return job
end

function unschedule!(scheduler::Scheduler, job::Union{Job, Symbol})
    jobName = job isa Job ? job.name : job
    @info "Removing job $(jobName) from scheduler."
    @lock scheduler.lock begin
        removeJob!(scheduler.store, job)
        removeJob!(scheduler, job)
    end
    return job
end

end # module
