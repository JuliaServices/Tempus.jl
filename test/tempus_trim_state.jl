using AbstractStores
using Dates
using JSON
using Tempus

# An application's use of Tempus, compiled with `juliac --trim=safe`: one
# JSON-encoded FileStore holds the state, the app adds its jobs at every
# startup (closures over its state, and a named function), and a scheduler
# runs them. The second startup reads back what the first one persisted.

const CLEANUP_JOB = "expire-weather-advisories"
const TICK_JOB = "trim-tick"
const FLAKY_JOB = "trim-flaky"
const FLAKY_ATTEMPTS = Threads.Atomic{Int}(0)

# application state the job closures capture
struct AppState
    ticks::Threads.Atomic{Int}
end

expire!(state::AppState, now::DateTime) = now > DateTime(2000) ? state.ticks[] : 0
tick!(state::AppState) = (Threads.atomic_add!(state.ticks, 1); nothing)
# fails on its first attempt; the scheduler's retry runs it again
trim_flaky!() = Threads.atomic_add!(FLAKY_ATTEMPTS, 1) == 0 ? error("first attempt fails") : nothing
trim_retry_check(state, @nospecialize(e)) = e isa ErrorException

# applications often keep the scheduler in an abstractly typed field
struct EventRuntime
    scheduler::Tempus.Scheduler
end

# A function barrier: inside it, only the field's abstract type is known.
@noinline stop!(runtime::EventRuntime) = close(runtime.scheduler)

function trim_assert(condition::Bool, message::String)::Nothing
    condition || error(message)
    return nothing
end

function trim_cron()::Nothing
    trim_assert(Tempus.getnext(Tempus.parseCron("*/15 6-18 * * MON,WED,FRI"),
                               DateTime(2026, 8, 10, 6, 7, 0)) ==
                DateTime(2026, 8, 10, 6, 15, 0), "cron step/range/day names")
    trim_assert(Tempus.getnext(Tempus.parseCron("@hourly"), DateTime(2026, 1, 1, 5, 30, 0)) ==
                DateTime(2026, 1, 1, 6, 0, 0), "cron alias")
    trim_assert(Tempus.getnext(Tempus.parseCron("30 0 0 1 JAN *"), DateTime(2026, 3, 1)) ==
                DateTime(2027, 1, 1, 0, 0, 30), "six-field cron with month name")
    trim_assert(Tempus.getnext(Tempus.parseCron("0 0 * * 5-7"), DateTime(2026, 8, 9, 1, 0, 0)) ==
                DateTime(2026, 8, 14), "day-of-week range through Sunday")
    trim_assert(Tempus.parseCron("0 0 * * * *") == Tempus.parseCron("0 0 * * * *"),
                "parsed schedules compare equal")
    return nothing
end

# what an application runs at startup
function start_jobs!(backend::AbstractStores.AbstractStore, state::AppState)::Tuple{Tempus.Store, Tempus.Scheduler}
    store = Tempus.Store(backend; prefix="tempus/")
    desired = Tempus.Job(() -> expire!(state, Dates.now(UTC)), CLEANUP_JOB, "0 0 * * * *";
                         overlap_policy=:skip, retries=0)
    jobs = Tempus.getJobs(store)
    existing = findfirst(job -> job.name == CLEANUP_JOB, jobs)
    if existing !== nothing
        trim_assert(jobs[existing].schedule == desired.schedule, "persisted schedule")
    end
    Tempus.addJob!(store, desired)
    Tempus.addJob!(store, Tempus.Job(() -> tick!(state), TICK_JOB, "* * * * * *"))
    scheduler = Tempus.Scheduler(store; logging=false, retry_check=trim_retry_check)
    Tempus.run!(scheduler)
    return store, scheduler
end

function wait_for(counter::Threads.Atomic{Int}, count::Int)::Nothing
    deadline = time() + 20.0
    while counter[] < count && time() < deadline
        sleep(0.05)
    end
    trim_assert(counter[] >= count, "scheduled job did not run")
    return nothing
end

function trim_scheduler(directory::String)::Nothing
    # Julia 1.13's trim verifier cannot resolve JSON.jl's typed decoding of
    # nested structs, which a JSON-encoded store needs; use memory there.
    backend = @static if VERSION >= v"1.14.0-"
        AbstractStores.FileStore(directory; codec=AbstractStores.JSONCodec())
    else
        AbstractStores.MemoryStore()
    end

    state = AppState(Threads.Atomic{Int}(0))
    store, scheduler = start_jobs!(backend, state)
    push!(scheduler, Tempus.OneShotJob(trim_flaky!, FLAKY_JOB))
    wait_for(state.ticks, 1)
    wait_for(FLAKY_ATTEMPTS, 2)
    close(scheduler)

    # restart: a new store over the same backend reads the persisted records
    store, scheduler = start_jobs!(backend, state)
    runtime = EventRuntime(scheduler)
    wait_for(state.ticks, state.ticks[] + 1)
    stop!(runtime)

    trim_assert(length(Tempus.getJobs(store)) == 3, "persisted jobs")
    history = Tempus.getNMostRecentJobExecutions(store, TICK_JOB, 10)
    trim_assert(!isempty(history) && history[1].succeeded, "persisted history")

    disabled_at = DateTime(2026, 8, 9, 13, 0, 0)
    Tempus.disableJob!(store, CLEANUP_JOB; at=disabled_at)
    cleanup = only(filter(job -> job.name == CLEANUP_JOB, Tempus.getJobs(store)))
    trim_assert(cleanup.disabledAt == disabled_at, "persisted disabled job")
    flaky = only(Tempus.getNMostRecentJobExecutions(store, FLAKY_JOB, 10))
    trim_assert(flaky.succeeded && FLAKY_ATTEMPTS[] == 2, "retried one-shot job")
    for name in (CLEANUP_JOB, TICK_JOB, FLAKY_JOB)
        Tempus.purgeJob!(store, name)
    end
    trim_assert(isempty(Tempus.getJobs(store)), "purged jobs")
    println(Core.stdout, "tempus trim workload ok: ", state.ticks[], " scheduled executions")
    return nothing
end

function @main(args::Vector{String})::Cint
    trim_cron()
    directory = mktempdir()
    try
        trim_scheduler(directory)
    finally
        rm(directory; recursive=true, force=true)
    end
    return 0
end

Base.Experimental.entrypoint(main, (Vector{String},))
