using AbstractStores
using Dates
using JSON
using Tempus

trim_action(; value="ok") = value

struct TrimJobParameters
    value::String
end

function trim_assert(condition::Bool, message::AbstractString)::Nothing
    condition || error(message)
    return nothing
end

function trim_cron()::Nothing
    trim_assert(Tempus.parseCronField("*") isa Tempus.Wildcard, "wildcard parser")
    trim_assert(Tempus.parseCronField("15") isa Tempus.Numeric, "numeric parser")
    trim_assert(Tempus.parseCronField("6-18") isa Tempus.Range, "range parser")
    trim_assert(Tempus.parseCronField("1,3,5") isa Tempus.List, "list parser")
    trim_assert(Tempus.parseCronField("*/15") isa Tempus.Step, "step parser")

    schedule = Tempus.Cron(
        Tempus.Numeric(0),
        Tempus.Step(Tempus.Wildcard(), 15),
        Tempus.Range(6, 18),
        Tempus.Wildcard(),
        Tempus.Wildcard(),
        Tempus.List([1, 3, 5]),
    )
    trim_assert(
        Tempus.getnext(schedule, DateTime(2026, 8, 10, 6, 7, 0)) ==
        DateTime(2026, 8, 10, 6, 15, 0),
        "cron next execution",
    )

    return nothing
end

function trim_job_state()::Nothing
    jobs_backend = AbstractStores.MemoryStore{Tempus.Job}()
    executions_backend = AbstractStores.MemoryStore{Vector{Tempus.JobExecution}}()
    store = Tempus.Store(jobs_backend, executions_backend; history_limit=2)
    schedule = Tempus.Cron(
        Tempus.Numeric(0),
        Tempus.Step(Tempus.Wildcard(), 15),
        Tempus.Wildcard(),
        Tempus.Wildcard(),
        Tempus.Wildcard(),
        Tempus.Wildcard(),
    )
    job = Tempus.Job(
        trim_action,
        "weather-refresh",
        schedule;
        job_params=Dict("value" => "sunny"),
        action_ref="trim_action",
        max_executions=5,
        retries=1,
    )
    action_ref = job.action_ref
    action_ref isa String && action_ref == "trim_action" || error("job action reference")
    action_data = job.action_data
    action_data isa String || error("job action data")
    parameters = JSON.parse(action_data, TrimJobParameters)
    trim_assert(trim_action(; value=parameters.value) == "sunny", "job JSON parameters")

    Tempus.addJob!(store, job)
    jobs = Tempus.getJobs(store)
    trim_assert(length(jobs) == 1 && only(jobs).name == job.name, "stored job")

    disabled_at = DateTime(2026, 8, 9, 13, 0, 0)
    Tempus.disableJob!(store, job; at=disabled_at)
    trim_assert(Tempus.isdisabled(job), "disabled job")
    trim_assert(only(Tempus.getJobs(store)).disabledAt == disabled_at,
                "persisted disabled job")
    Tempus.purgeJob!(store, job)
    trim_assert(isempty(Tempus.getJobs(store)), "purged job")
    return nothing
end

function run_tempus_trim()::Nothing
    trim_cron()
    trim_job_state()
    # Tempus.run! uses background tasks. Its trim coverage is tracked with the
    # Julia task-compilation fix, separate from this stock-Julia workload.
    return nothing
end

function @main(args::Vector{String})::Cint
    _ = args
    run_tempus_trim()
    return 0
end

Base.Experimental.entrypoint(main, (Vector{String},))
