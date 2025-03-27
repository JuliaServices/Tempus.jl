var documenterSearchIndex = {"docs":
[{"location":"#Tempus","page":"Tempus","title":"Tempus","text":"","category":"section"},{"location":"","page":"Tempus","title":"Tempus","text":"Tempus Julia package repo.","category":"page"},{"location":"","page":"Tempus","title":"Tempus","text":"Modules = [Tempus]","category":"page"},{"location":"#Tempus.Tempus","page":"Tempus","title":"Tempus.Tempus","text":"Tempus provides a cron-style job scheduling framework for Julia, inspired by Quartz in Java. \n\nFeatures:\n\nDefine jobs with cron-like scheduling expressions\nSupports job execution policies (overlap handling, retries, and failure strategies)\nMultiple job stores and JobStore interface (in-memory, file-based persistence)\nConcurrency-aware execution with configurable retry logic\nSupports disabling, enabling, and unscheduling jobs dynamically\nThread-safe scheduling with a background execution loop\n\n\n\n\n\n","category":"module"},{"location":"#Tempus.FileStore","page":"Tempus","title":"Tempus.FileStore","text":"FileStore <: Store\n\nA file-based job storage backend that persists jobs and execution history to disk.\n\nFields:\n\nfilepath::String: The file path where jobs are serialized.\nstore::InMemoryStore: In-memory store that handles operations before syncing to disk.\n\n\n\n\n\n","category":"type"},{"location":"#Tempus.InMemoryStore","page":"Tempus","title":"Tempus.InMemoryStore","text":"InMemoryStore <: Store\n\nAn in-memory job storage backend.\n\nFields:\n\njobs::Set{Job}: Stores active jobs.\njobExecutions::Dict{String, Vector{JobExecution}}: Stores execution history for each job.\n\n\n\n\n\n","category":"type"},{"location":"#Tempus.Job","page":"Tempus","title":"Tempus.Job","text":"Job\n\nRepresents a single job/unit of work. Can be scheduled to repeat.\n\nFields:\n\nname::String: Unique identifier for the job.\nschedule::Cron: The cron-style schedule expression.\naction::Function: The function to execute when the job runs.\noptions::JobOptions: Execution options for retries, failures, and overlap handling.\ndisabledAt::Union{DateTime, Nothing}: Timestamp when the job was disabled (if applicable).\n\n\n\n\n\n","category":"type"},{"location":"#Tempus.JobExecution","page":"Tempus","title":"Tempus.JobExecution","text":"JobExecution\n\nRepresents an instance of a job execution.\n\nFields:\n\njobExecutionId::String: Unique identifier for this job execution.\njob::Job: The job being executed.\nscheduledStart::DateTime: When the job was scheduled to run.\nrunConcurrently::Bool: Whether this execution is running concurrently with another.\nactualStart::DateTime: The actual start time.\nfinish::DateTime: The completion time.\nstatus::Symbol: The execution result (:succeeded, :failed).\n\n\n\n\n\n","category":"type"},{"location":"#Tempus.JobOptions","page":"Tempus","title":"Tempus.JobOptions","text":"JobOptions\n\nDefines options for job execution behavior.\n\nFields:\n\noverlap_policy::Union{Symbol, Nothing}: Determines job execution behavior when the same job is already running (:skip, :queue, :concurrent).\nretries::Int: Number of retries allowed on failure.\nretry_delays::Union{Base.ExponentialBackOff, Nothing}: Delay strategy for retries (defaults to exponential backoff if retries > 0).\nretry_check: Custom function to determine retry behavior (check argument from Base.retry).\nmax_failed_executions::Union{Int, Nothing}: Maximum number of failed executions allowed for a job before it will be disabled.\nmax_executions::Union{Int, Nothing}: Maximum number of executions allowed for a job.\nexpires_at::Union{DateTime, Nothing}: Expiration time for a job.\n\n\n\n\n\n","category":"type"},{"location":"#Tempus.Scheduler","page":"Tempus","title":"Tempus.Scheduler","text":"Scheduler\n\nThe main scheduling engine that executes jobs according to their schedules.\n\nFields:\n\nlock::ReentrantLock: Ensures thread-safe access.\njobExecutions::Vector{JobExecution}: List of scheduled job executions.\nstore::Store: Job storage backend.\njobExecutionFinished::Threads.Event: Signals all job executions have finished when shutting down.\nexecutingJobExecutions::Set{JobExecution}: Tracks currently executing jobs.\nrunning::Bool: Scheduler state (running/stopped).\njobOptions::JobOptions: Default job execution options.\nmax_concurrent_executions::Int: Limit on how many total executions can be running concurrently for this scheduler, defaults to Threads.nthreads()\n\n\n\n\n\n","category":"type"},{"location":"#Tempus.Store","page":"Tempus","title":"Tempus.Store","text":"Store\n\nDefines an interface for job storage backends.\n\n\n\n\n\n","category":"type"},{"location":"#Base.close-Tuple{Tempus.Scheduler}","page":"Tempus","title":"Base.close","text":"close(scheduler::Scheduler)\n\nCloses the scheduler, stopping job execution; waits for any currently executing jobs to finish. Will wait timeout seconds (5 by default) for any currently executing jobs to finish before returning.\n\n\n\n\n\n","category":"method"},{"location":"#Base.push!-Tuple{Tempus.Scheduler, Tempus.Job}","page":"Tempus","title":"Base.push!","text":"push!(scheduler::Scheduler, job::Job)\n\nAdds a job to the scheduler and underlying Store, scheduling its next execution based on its cron schedule.\n\n\n\n\n\n","category":"method"},{"location":"#Base.wait-Tuple{Tempus.Scheduler}","page":"Tempus","title":"Base.wait","text":"wait(scheduler::Scheduler)\n\nWaits for the scheduler to finish executing all jobs. Note the scheduler must be explicitly closed to stop the scheduler loop or pass close_when_no_jobs=true to run! to automatically close the scheduler when no jobs are left.\n\n\n\n\n\n","category":"method"},{"location":"#Tempus.addJob!","page":"Tempus","title":"Tempus.addJob!","text":"addJob!(store::Store, job::Job)\n\nAdd a new job to store.\n\n\n\n\n\n","category":"function"},{"location":"#Tempus.disable!-Tuple{Tempus.Job}","page":"Tempus","title":"Tempus.disable!","text":"disable!(job::Job)\n\nDisables a job, preventing it from being scheduled for execution.\n\n\n\n\n\n","category":"method"},{"location":"#Tempus.disableJob!-Tuple{Tempus.Store, Union{String, Tempus.Job}}","page":"Tempus","title":"Tempus.disableJob!","text":"disableJob!(store::Store, job::Union{Job, String})\n\nDisable a job in store by reference or name.\n\n\n\n\n\n","category":"method"},{"location":"#Tempus.enable!-Tuple{Tempus.Job}","page":"Tempus","title":"Tempus.enable!","text":"enable!(job::Job)\n\nEnables a previously disabled job, allowing it to be scheduled again.\n\n\n\n\n\n","category":"method"},{"location":"#Tempus.getJobs","page":"Tempus","title":"Tempus.getJobs","text":"getJobs(store::Store) -> Collection{Job}\n\nRetrieve all jobs stored in store, regardless of disabled status.\n\n\n\n\n\n","category":"function"},{"location":"#Tempus.getNMostRecentJobExecutions-Tuple{Tempus.Store, String, Int64}","page":"Tempus","title":"Tempus.getNMostRecentJobExecutions","text":"getNMostRecentJobExecutions(store::Store, jobName::String, n::Int) -> Vector{JobExecution}\n\nGet the n most recent job executions for a job persisted in store.\n\n\n\n\n\n","category":"method"},{"location":"#Tempus.isdisabled-Tuple{Tempus.Job}","page":"Tempus","title":"Tempus.isdisabled","text":"isdisabled(job::Job) -> Bool\n\nReturns true if the job is currently disabled.\n\n\n\n\n\n","category":"method"},{"location":"#Tempus.purgeJob!","page":"Tempus","title":"Tempus.purgeJob!","text":"purgeJob!(store::Store, job::Union{Job, String})\n\nRemove a job from store by reference or name. All job execution history will also be removed.\n\n\n\n\n\n","category":"function"},{"location":"#Tempus.run!-Tuple{Tempus.Scheduler}","page":"Tempus","title":"Tempus.run!","text":"run!(scheduler::Scheduler)\n\nStarts the scheduler, executing jobs at their scheduled times.\n\n\n\n\n\n","category":"method"},{"location":"#Tempus.runJobs!-Tuple{Tempus.Store, Any}","page":"Tempus","title":"Tempus.runJobs!","text":"runJobs!(store::Store, jobs; kw...)\n\nAdd each job in jobs to store, run a scheduler with kw options, wait for all jobs to finish, then close the scheduler.\n\n\n\n\n\n","category":"method"},{"location":"#Tempus.storeJobExecution!-Tuple{Tempus.Store, Tempus.JobExecution}","page":"Tempus","title":"Tempus.storeJobExecution!","text":"storeJobExecution!(store::Store, jobExecution::JobExecution)\n\nStore jobExecution in store.\n\n\n\n\n\n","category":"method"},{"location":"#Tempus.withscheduler-Tuple{Any, Vararg{Any}}","page":"Tempus","title":"Tempus.withscheduler","text":"withscheduler(f, args...; kw...)\n\nCreates a scheduler, runs a function f with it, then calls close.\n\n\n\n\n\n","category":"method"}]
}
