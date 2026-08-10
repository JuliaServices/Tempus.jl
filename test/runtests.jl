using Test, AbstractStores, Dates, JSON, SQLite, Tempus

import Tempus: parseCronField, parseCron, getnext

# A named function remains resolvable when a persistent store is reopened.
_filestore_test_action() = (global executed; push!(executed, Dates.now(UTC)))
_sqlite_test_action() = nothing

struct FailingHistoryStore <: AbstractStores.AbstractStore{Vector{Tempus.JobExecution}} end
Base.get(::FailingHistoryStore, ::AbstractString, default) = error("history unavailable")

@testset "parseCronField Tests" begin
    # Wildcard: should parse "*" into a Wildcard type.
    @testset "Wildcard" begin
        cf = parseCronField("*")
        @test cf isa Tempus.Wildcard
    end

    # Numeric: should parse a simple numeric value.
    @testset "Numeric" begin
        cf = parseCronField("5")
        @test cf isa Tempus.Numeric
        @test cf.value == 5
    end

    # Range: should parse a range like "1-10" into a Range type.
    @testset "Range" begin
        cf = parseCronField("1-10")
        @test cf isa Tempus.Range
        @test cf.start == 1
        @test cf.stop == 10
    end

    # Step: wildcard with step, e.g. "*/15".
    @testset "Step with Wildcard" begin
        cf = parseCronField("*/15")
        @test cf isa Tempus.Step
        @test cf.step == 15
        @test cf.range isa Tempus.Wildcard
    end

    # Step: range with step, e.g. "1-10/2".
    @testset "Step with Range" begin
        cf = parseCronField("1-10/2")
        @test cf isa Tempus.Step
        @test cf.step == 2
        @test cf.range isa Tempus.Range
        if cf.range isa Tempus.Range
            @test cf.range.start == 1
            @test cf.range.stop == 10
        end
    end

    # List: should parse a comma-separated list like "1,2,3".
    @testset "List" begin
        cf = parseCronField("1,2,3")
        @test cf isa Tempus.List
        @test cf.values == [1, 2, 3]
    end

    # Invalid expressions: ensure these throw an ArgumentError.
    @testset "Invalid Expressions" begin
        @test_throws ArgumentError parseCronField("invalid")
        @test_throws ArgumentError parseCronField("1-")
        @test_throws ArgumentError parseCronField("*/")
        @test_throws ArgumentError parseCronField("1,")
        @test_throws ArgumentError parseCronField("5-1")
        @test_throws ArgumentError parseCronField("*/abc")
    end
end

@testset "parseCron" begin
    cron = "* * * * *"
    cronObj = parseCron(cron)
    @test cronObj.second == Tempus.Numeric(0)
    @test cronObj.minute isa Tempus.Wildcard
    @test cronObj.hour isa Tempus.Wildcard
    @test cronObj.day_of_month isa Tempus.Wildcard
    @test cronObj.month isa Tempus.Wildcard
    @test cronObj.day_of_week isa Tempus.Wildcard

    cron = "*/15 * * * *"
    cronObj = parseCron(cron)
    @test cronObj.second == Tempus.Numeric(0)
    @test cronObj.minute isa Tempus.Step
    @test cronObj.minute.step == 15
    @test cronObj.minute.range isa Tempus.Wildcard
    @test cronObj.hour isa Tempus.Wildcard
    @test cronObj.day_of_month isa Tempus.Wildcard
    @test cronObj.month isa Tempus.Wildcard
    @test cronObj.day_of_week isa Tempus.Wildcard
end

@testset "getnext" begin
    cron = parseCron("* * * * *")
    dt = DateTime(2021, 1, 1, 0, 0, 0)
    next = getnext(cron, dt)
    @test next == DateTime(2021, 1, 1, 0, 1, 0)

    # more test cases here
    cron = parseCron("*/15 * * * *")
    dt = DateTime(2021, 1, 1, 0, 12, 0)
    next = getnext(cron, dt)
    @test next == DateTime(2021, 1, 1, 0, 15, 0)

    dt_edge = DateTime(2021, 1, 1, 0, 14, 59)
    next_edge = getnext(cron, dt_edge)
    @test next_edge == DateTime(2021, 1, 1, 0, 15, 0)

    dt_edge = DateTime(2021, 1, 1, 0, 15, 0)
    next_edge = getnext(cron, dt_edge)
    @test next_edge == DateTime(2021, 1, 1, 0, 30, 0)

    cron = parseCron("0 0 1 * *")
    dt = DateTime(2021, 1, 1, 0, 0, 0)
    next = getnext(cron, dt)
    @test next == DateTime(2021, 2, 1, 0, 0, 0)

    cron = parseCron("0 0 1 * *")
    dt = DateTime(2021, 1, 31, 23, 59, 59)
    next_edge_case = getnext(cron, dt)
    @test next_edge_case == DateTime(2021, 2, 1, 0, 0, 0)

    cron = parseCron("0 0 1 * *")
    dt = DateTime(2020, 2, 29, 23, 59, 59)
    next_edge_case = getnext(cron, dt)
    @test next_edge_case == DateTime(2020, 3, 1, 0, 0, 0)

    cron = parseCron("0 0 1 1 *")
    dt = DateTime(2020, 1, 1, 0, 0, 0)
    next_case = getnext(cron, dt)
    @test next_case == DateTime(2021, 1, 1, 0, 0, 0)
end

@testset "getnext Edge Cases" begin

    # 1. Basic wildcard: "* * * * *"
    @testset "Wildcard every minute" begin
        cron = parseCron("* * * * *")
        dt = DateTime(2021, 1, 1, 0, 0, 0)
        next = getnext(cron, dt)
        @test next == DateTime(2021, 1, 1, 0, 1, 0)
    end

    # 2. Fixed minute: "30 * * * *" 
    @testset "Fixed minute (30)" begin
        cron = parseCron("30 * * * *")
        dt_before = DateTime(2021, 1, 1, 0, 15, 0)
        next_before = getnext(cron, dt_before)
        @test next_before == DateTime(2021, 1, 1, 0, 30, 0)

        dt_after = DateTime(2021, 1, 1, 0, 35, 0)
        next_after = getnext(cron, dt_after)
        @test next_after == DateTime(2021, 1, 1, 1, 30, 0)
    end

    # 3. Fixed hour: "* 10 * * *" (only 10 AM allowed)
    @testset "Fixed hour (10 AM)" begin
        cron = parseCron("* 10 * * *")
        dt = DateTime(2021, 1, 1, 9, 59, 59)
        next = getnext(cron, dt)
        @test next == DateTime(2021, 1, 1, 10, 0, 0)
    end

    # 4. Fixed trigger at midnight (6-field expression): "0 0 0 * * *"
    @testset "Fixed midnight" begin
        cron = parseCron("0 0 0 * * *")
        dt = DateTime(2021, 1, 1, 0, 0, 0)
        # Since the current time exactly matches the allowed values,
        # getnext should move to the next occurrence (next day at midnight)
        next = getnext(cron, dt)
        @test next == DateTime(2021, 1, 2, 0, 0, 0)
    end

    # 5. Month boundary with day-of-month fixed: "* * 1 * *" (only 1st day allowed)
    @testset "Day-of-month fixed to 1" begin
        cron = parseCron("* * 1 * *")
        dt = DateTime(2021, 1, 15, 12, 0, 0)
        next = getnext(cron, dt)
        # Expect the next trigger on the 1st of the next month
        @test next == DateTime(2021, 2, 1, 0, 0, 0)
    end

    # 6. Step minute expression: "*/15 * * * *" (every 15 minutes)
    @testset "Step minute: every 15" begin
        cron = parseCron("*/15 * * * *")
        dt_inhour = DateTime(2021, 1, 1, 0, 12, 0) # Allowed minutes: 0, 15, 30, 45
        next = getnext(cron, dt_inhour)
        @test next == DateTime(2021, 1, 1, 0, 15, 0)

        dt_roll = DateTime(2021, 1, 1, 0, 47, 0)
        next_roll = getnext(cron, dt_roll)
        # Since 47 > 45, it should roll to the next hour, resetting to minute 0
        @test next_roll == DateTime(2021, 1, 1, 1, 0, 0)
    end

    # 7. Day-of-week constraint: "* * * * 2" (only Tuesday allowed)
    @testset "Fixed day-of-week (Tuesday)" begin
        cron = parseCron("* * * * 2")
        dt = DateTime(2021, 1, 1, 0, 0, 0)  # Jan 1, 2021 was a Friday
        next = getnext(cron, dt)
        # The next Tuesday after Jan 1, 2021 is Jan 5, 2021.
        @test next == DateTime(2021, 1, 5, 0, 0, 0)
    end

    # 8. Combination: day-of-month AND day-of-week constraint 
    # find next date that matches day of month OR day of week
    @testset "Day-of-month 15 and day-of-week Thursday" begin
        cron = parseCron("* * 15 * 4")
        dt = DateTime(2021, 1, 1, 0, 0, 0)
        next = getnext(cron, dt)
        # The next occurrence of the 15th OR a Thursday is Jan 7, 2021 (Thursday)
        @test next == DateTime(2021, 1, 7, 0, 0, 0)
    end

    # 9. Edge-case: End-of-month rollover.
    @testset "End-of-month rollover" begin
        cron = parseCron("* * * * *")
        dt = DateTime(2021, 1, 31, 23, 59, 59)
        next = getnext(cron, dt)
        # Expected: Since it’s the last minute of January, the next trigger should be Feb 1, 00:00:00
        @test next == DateTime(2021, 2, 1, 0, 0, 0)
    end

    # 10. List minute expression: "0,30 * * * *" (at minutes 0 and 30)
    @testset "List minute: 0,30" begin
        cron = parseCron("0,30 * * * *")
        @test cron.minute isa Tempus.List
        @test cron.minute.values == [0, 30]

        # Before minute 30 — should advance to minute 30 in the same hour
        dt = DateTime(2021, 1, 1, 0, 10, 0)
        next = getnext(cron, dt)
        @test next == DateTime(2021, 1, 1, 0, 30, 0)

        # At minute 30 — should advance to minute 0 of the next hour
        dt = DateTime(2021, 1, 1, 0, 30, 0)
        next = getnext(cron, dt)
        @test next == DateTime(2021, 1, 1, 1, 0, 0)

        # After minute 30 — should advance to minute 0 of the next hour
        dt = DateTime(2021, 1, 1, 0, 45, 0)
        next = getnext(cron, dt)
        @test next == DateTime(2021, 1, 1, 1, 0, 0)

        # At minute 0 — should advance to minute 30 of the same hour
        dt = DateTime(2021, 1, 1, 1, 0, 0)
        next = getnext(cron, dt)
        @test next == DateTime(2021, 1, 1, 1, 30, 0)
    end

    # 11. List with more values: "0,15,30,45 * * * *" (every 15 min via list)
    @testset "List minute: 0,15,30,45" begin
        cron = parseCron("0,15,30,45 * * * *")
        dt = DateTime(2021, 1, 1, 0, 7, 0)
        next = getnext(cron, dt)
        @test next == DateTime(2021, 1, 1, 0, 15, 0)

        dt = DateTime(2021, 1, 1, 0, 46, 0)
        next = getnext(cron, dt)
        @test next == DateTime(2021, 1, 1, 1, 0, 0)
    end

    # 12. Edge-case: Leap year.
    @testset "Leap year" begin
        cron = parseCron("* * * * *")
        dt = DateTime(2020, 2, 28, 23, 59, 59)
        next = getnext(cron, dt)
        # Expected: Since it’s the last minute of February in a leap year, the next trigger should be Feb 29, 00:00:00
        @test next == DateTime(2020, 2, 29, 0, 0, 0)
    end
end

# -- Higher-Level Tests for Scheduler --
# we make this a global variable so we can access it in the test cases
# even when a job gets serialized/deserialized for a file store
executed = DateTime[]

@testset "Scheduler Scheduling and Execution" begin
    # We'll create a simple job that records its execution time.
    test_job = Tempus.Job("testjob", "* * * * * *") do
        global executed
        push!(executed, Dates.now(UTC))
    end
    withscheduler() do sch
        # Schedule the test job.
        push!(sch, test_job)
        # Job should run every second, so we wait for a few seconds and check if it ran.
        sleep(2)  # wait for the job to run multiple times
    end
    @test length(executed) > 0
    # disabled job doesn't run
    empty!(executed)
    Tempus.disable!(test_job)
    withscheduler() do sch
        push!(sch, test_job)
        sleep(2)  # wait for the job to run multiple times if it was enabled
    end
    @test length(executed) == 0
    # re-enable
    empty!(executed)
    Tempus.enable!(test_job)
    withscheduler() do sch
        push!(sch, test_job)
        sleep(2)  # wait for the job to run multiple times
    end
    @test length(executed) > 0
    # overlap policy
    # skip
    # job that takes 2 seconds to run, but runs every second
    sleep_job = Tempus.Job("sleepjob", "* * * * * *") do
        sleep(2)
        push!(executed, Dates.now(UTC))
    end
    empty!(executed)
    withscheduler(; overlap_policy=:skip) do sch
        push!(sch, sleep_job)
        sleep(2.5)  # wait for the job to run once
    end
    @test length(executed) == 1
    # concurrent
    empty!(executed)
    withscheduler(; overlap_policy=:concurrent, max_concurrent_executions=2) do sch
        push!(sch, sleep_job)
        sleep(2.5)  # wait for the job to run once
    end
    @test length(executed) == 2
    # queue
    empty!(executed)
    withscheduler(; overlap_policy=:queue) do sch
        push!(sch, sleep_job)
        sleep(3.5)  # wait for the job to start executing twice sequentially
    end
    @test length(executed) == 2
    # retry settings
    # retry n times
    fail_job = Tempus.Job("failjob", "* * * * * *") do
        println("length(executed): ", length(executed))
        if length(executed) < 2
            push!(executed, Dates.now(UTC))
            error("Job failed")
        end
    end
    empty!(executed)
    withscheduler(; retries=2) do sch
        push!(sch, fail_job)
        sleep(3)  # wait for the job to run multiple times
    end
    @test length(executed) == 2
    # retry check
    toggle = Ref{Bool}(true)
    retry_check = (s, e) -> begin
        if toggle[] 
            println("Retry triggered")
            toggle[] = false
            return true
        else
            println("Retry not triggered")
            return false
        end
    end
    empty!(executed)
    withscheduler(; retries=2, retry_check=retry_check) do sch
        push!(sch, fail_job)
        sleep(3)  # wait for the job to run multiple times
    end
    @test length(executed) == 2
    @test toggle[] == false
    # # on_fail_policy
    # # ignore
    # always_fail_job = Tempus.Job("failjob", "* * * * * *") do
    #     push!(executed, Dates.now(UTC))
    #     error("always fail job")
    # end
    # empty!(executed)
    # withscheduler(; on_fail_policy=(:ignore, 0)) do sch
    #     push!(sch, always_fail_job)
    #     sleep(4)  # wait for the job to run multiple times
    # end
    # @test length(executed) > 4 # job should run every second + retries, but never get disabled
    # # on_fail_policy disable job after 1 failure
    # empty!(executed)
    # Tempus.enable!(always_fail_job)
    # withscheduler(; retries=0, on_fail_policy=(:disable, 1)) do sch
    #     push!(sch, always_fail_job)
    #     sleep(4)  # wait to verify job does not run
    # end
    # @test length(executed) == 1 # job ran once, failed, and was disabled
    # @test Tempus.isdisabled(always_fail_job)
    # # on_fail_policy unschedule the job after 1 failure
    # empty!(executed)
    # Tempus.enable!(always_fail_job)
    # withscheduler(; retries=0, on_fail_policy=(:unschedule, 1)) do sch
    #     push!(sch, always_fail_job)
    #     sleep(3)  # wait job to run, fail, and be unscheduled and ensure it isn't run again
    #     @test all(je -> je.job.name != always_fail_job.name, sch.jobExecutions)
    # end
    # @test length(executed) == 1 # job ran once, failed, and was unscheduled

    # if execution is being retried n times and job gets disabled/unscheduled, retries are stopped

    # dyanmically schedule and unschedule multiple jobs

    # test job stores
    # InMemoryStore

    # test the job is dynamically added to store

    # job is automatically started when persisted in store

    # if job is disabled, it persists through scheduler restart

    # FileStore uses a directory and persists both jobs and execution history.
    empty!(executed)
    mktempdir() do path
        fs_job = Tempus.Job(_filestore_test_action, "testjob_fs", "* * * * * *")
        fs = Tempus.FileStore(path)
        withscheduler(fs) do sch
            push!(sch, fs_job)
            sleep(3)
        end
        @test length(executed) > 0
        @test !isempty(
            Tempus.getNMostRecentJobExecutions(fs, "testjob_fs", 10),
        )

        # Reopen the same backend and verify both forms of state survived.
        empty!(executed)
        fs = Tempus.FileStore(path)
        @test !isempty(
            Tempus.getNMostRecentJobExecutions(fs, "testjob_fs", 10),
        )
        withscheduler(fs) do sch
            sleep(3)
        end
        @test length(executed) > 0
    end
end

@testset "AbstractStores-backed state" begin
    backend = MemoryStore()
    store = Tempus.Store(backend; prefix="scheduler/", history_limit=2)
    job = Tempus.Job(() -> nothing, "bounded", "* * * * * *")
    Tempus.addJob!(store, job)
    @test collect(keys(backend; prefix="scheduler/jobs/")) ==
        ["scheduler/jobs/bounded"]

    for second in 1:3
        execution = Tempus.JobExecution(
            job,
            DateTime(2024, 1, 1, 0, 0, second),
        )
        execution.actualStart = execution.scheduledStart
        execution.finish = execution.scheduledStart
        execution.status = :succeeded
        execution.result = nothing
        execution.exception = nothing
        Tempus.storeJobExecution!(store, execution)
    end
    history = Tempus.getNMostRecentJobExecutions(store, job.name, 10)
    @test length(history) == 2
    @test history[1].scheduledStart == DateTime(2024, 1, 1, 0, 0, 3)

    disabled_at = DateTime(2024, 1, 2)
    Tempus.disableJob!(store, job; at=disabled_at)
    @test only(Tempus.getJobs(store)).disabledAt == disabled_at
    @test job.disabledAt == disabled_at

    Tempus.purgeJob!(store, job.name)
    @test isempty(Tempus.getJobs(store))
    @test isempty(Tempus.getNMostRecentJobExecutions(store, job.name, 10))
end

@testset "Store backend validation" begin
    # a backend that cannot hold Jobs must fail at construction, not at first use
    @test_throws ArgumentError Tempus.Store(MemoryStore{String}())
    @test_throws ArgumentError Tempus.Store(MemoryStore(); history_limit=0)
end

@testset "Serializing backend persists bounded history" begin
    mktempdir() do dir
        job = Tempus.Job(_filestore_test_action, "history_job", "* * * * * *")
        store = Tempus.FileStore(dir; history_limit=2)
        Tempus.addJob!(store, job)
        for second in 1:3
            je = Tempus.JobExecution(job, DateTime(2024, 1, 1, 0, 0, second))
            je.actualStart = je.scheduledStart
            je.finish = je.scheduledStart
            je.status = :succeeded
            je.result = nothing
            je.exception = nothing
            Tempus.storeJobExecution!(store, je)
        end
        # Read through a *fresh* store: nothing is cached in this process, so a
        # `modify!` callback that mutated the value it was handed (which
        # AbstractStores reads as "no change", skipping the write) would show up
        # here as lost appends.
        reopened = Tempus.FileStore(dir; history_limit=2)
        history = Tempus.getNMostRecentJobExecutions(reopened, "history_job", 10)
        @test length(history) == 2
        @test [je.scheduledStart for je in history] ==
            [DateTime(2024, 1, 1, 0, 0, 3), DateTime(2024, 1, 1, 0, 0, 2)]
        @test only(Tempus.getJobs(reopened)).name == "history_job"
    end
end

@testset "Persisted jobs reload in a fresh process" begin
    mktempdir() do dir
        state = joinpath(dir, "state")     # the store owns this directory
        sentinel = joinpath(dir, "ran.txt")
        store = Tempus.FileStore(state)
        Tempus.addJob!(store,
            Tempus.Job(_filestore_test_action, "fresh_process_job", "0 * * * * *"))
        code = """
        using Tempus
        _filestore_test_action() = write(raw"$sentinel", "ran")
        store = Tempus.FileStore(raw"$state")
        jobs = Tempus.getJobs(store)
        length(jobs) == 1 || error("expected 1 job, got \$(length(jobs))")
        job = only(jobs)
        job.name == "fresh_process_job" || error("wrong job name: \$(job.name)")
        job.schedule === nothing && error("schedule did not survive persistence")
        job.action()
        """
        run(`$(Base.julia_cmd()) --project=$(Base.active_project()) -e $code`)
        @test isfile(sentinel)
    end
end

@testset "Unstorable execution does not wedge the scheduler" begin
    mktempdir() do dir
        runs = Ref(0)
        job = Tempus.Job("unstorable_result", "* * * * * *") do
            runs[] += 1
            # a *running* Task cannot be serialized, so persisting this
            # execution throws inside the execution task
            return Threads.@spawn (sleep(30); nothing)
        end
        scheduler = Tempus.Scheduler(Tempus.FileStore(dir); logging=false)
        try
            Tempus.run!(scheduler)
            push!(scheduler, job)
            sleep(3.5)
            @test runs[] > 1                                    # still scheduling
            @test isempty(scheduler.executingJobExecutions)     # bookkeeping intact
        finally
            # stop the loop without waiting: if this regression ever comes back,
            # `close` blocks on executions that never finished bookkeeping, and a
            # hung test is worse than a failed one
            @lock scheduler.lock (scheduler.running = false)
        end
    end
end

@testset "Scheduler simultaneous-ready regression" begin
    runs = Ref(0)
    store = Tempus.InMemoryStore()
    Tempus.addJob!(store, Tempus.OneShotJob(() -> (runs[] += 1), "simul_a"))
    Tempus.addJob!(store, Tempus.OneShotJob(() -> (runs[] += 1), "simul_b"))
    scheduler = Tempus.Scheduler(store; logging=false)
    try
        Tempus.run!(scheduler; close_when_no_jobs=true)
        wait(scheduler)
    finally
        close(scheduler)
    end
    @test runs[] >= 2
end

@testset "OneShot max_executions regression" begin
    runs = Ref(0)
    job = Tempus.OneShotJob(() -> (runs[] += 1), "oneshot_once")
    withscheduler(; logging=false) do scheduler
        push!(scheduler, job)
        sleep(2)
    end
    @test runs[] == 1
end

@testset "nextJobExecution bounds regression" begin
    store = Tempus.InMemoryStore()
    job = Tempus.Job(() -> nothing, "bounds_regression", "* * * * * *";
        max_failed_executions=3, max_executions=10)
    Tempus.addJob!(store, job)
    next = Tempus.nextJobExecution(store, job, 3, 10, nothing; logging=false)
    @test next isa Tempus.JobExecution
end

@testset "Queue scheduling dedupe regression" begin
    job = Tempus.Job("queue_dedupe_regression", "* * * * * *") do
        sleep(2)
    end
    withscheduler(; overlap_policy=:queue, logging=false) do scheduler
        push!(scheduler, job)
        sleep(3.5)
        scheduled = [je.scheduledStart for je in scheduler.jobExecutions if je.job.name == "queue_dedupe_regression"]
        @test length(scheduled) == length(unique(scheduled))
    end
end

@testset "runJobs! single-run regression" begin
    runs = Ref(0)
    job = Tempus.OneShotJob(() -> (runs[] += 1), "runjobs_oneshot")
    Tempus.runJobs!(Tempus.InMemoryStore(), [job]; logging=false)
    @test runs[] == 1
end

@testset "Logging no-next regression" begin
    job = Tempus.Job(() -> nothing, "logging_none_next", "* * * * * *")
    withscheduler(; logging=true) do scheduler
        push!(scheduler, job)
        Tempus.disable!(job)
        sleep(1.5)
    end
    @test true
end

@testset "SQLite load mapping regression" begin
    db = SQLite.DB()
    store = Tempus.SQLiteStore(db)
    job1 = Tempus.Job(_sqlite_test_action, "sqlite_job_1", "* * * * * *")
    job2 = Tempus.Job(_sqlite_test_action, "sqlite_job_2", "* * * * * *")
    Tempus.addJob!(store, job1)
    Tempus.addJob!(store, job2)
    je1 = Tempus.JobExecution(job1, DateTime(2024, 1, 1, 0, 0, 0))
    je1.actualStart = DateTime(2024, 1, 1, 0, 0, 0)
    je1.finish = DateTime(2024, 1, 1, 0, 0, 1)
    je1.status = :succeeded
    je1.result = nothing
    je1.exception = nothing
    Tempus.storeJobExecution!(store, je1)
    je2 = Tempus.JobExecution(job2, DateTime(2024, 1, 1, 0, 1, 0))
    je2.actualStart = DateTime(2024, 1, 1, 0, 1, 0)
    je2.finish = DateTime(2024, 1, 1, 0, 1, 1)
    je2.status = :failed
    je2.result = nothing
    je2.exception = ErrorException("expected")
    Tempus.storeJobExecution!(store, je2)
    reloaded = Tempus.SQLiteStore(db)
    @test length(Tempus.getJobs(reloaded)) == 2
    @test length(Tempus.getNMostRecentJobExecutions(reloaded, "sqlite_job_1", 10)) == 1
    @test length(Tempus.getNMostRecentJobExecutions(reloaded, "sqlite_job_2", 10)) == 1
    SQLite.close(db)
end

@testset "JobOptions keyword forwarding" begin
    job = Tempus.Job(() -> nothing, "kw_job", "* * * * *"; max_executions=2, retries=1)
    @test job.options.max_executions == 2
    @test job.options.retries == 1
    one_shot = Tempus.OneShotJob(() -> nothing, "oneshot"; retries=3)
    @test one_shot.options.max_executions == 1
    @test one_shot.options.retries == 3
    @test Tempus.OneShotJob(
        () -> nothing,
        "oneshot_override";
        max_executions=nothing,
    ).options.max_executions == 1
end

@testset "Timezone-aware getnext" begin
    # 9 PM Denver (MST=UTC-7) → 4 AM UTC next day
    cron = parseCron("0 0 21 * * *")
    @test getnext(cron, "America/Denver", DateTime(2024,1,15)) == DateTime(2024,1,15,4,0,0)

    # Spring forward: 2:30 AM doesn't exist March 10, 2024 in Denver → skips to next day
    cron = parseCron("0 30 2 * * *")
    @test getnext(cron, "America/Denver", DateTime(2024,3,10)) == DateTime(2024,3,11,8,30,0)

    # Fall back: 1:30 AM ambiguous Nov 3, 2024 → first occurrence (MDT, UTC-6)
    cron = parseCron("0 30 1 * * *")
    @test getnext(cron, "America/Denver", DateTime(2024,11,3)) == DateTime(2024,11,3,7,30,0)

    # Forward offset changes are not always one hour. Lord Howe advances by 30
    # minutes, so an every-second schedule resumes at 2:30 AM local.
    cron = parseCron("* * * * * *")
    @test getnext(cron, "Australia/Lord_Howe", DateTime(2024,10,5,15,29,59)) ==
        DateTime(2024,10,5,15,30,0)

    # Apia skipped all of December 30, 2011. The old one-hour assumption retried
    # inside the same gap and threw another NonExistentTimeError.
    @test getnext(cron, "Pacific/Apia", DateTime(2011,12,30,9,59,59)) ==
        DateTime(2011,12,30,10,0,0)

    # No timezone = existing UTC behavior
    cron = parseCron("0 0 12 * * *")
    @test getnext(cron, DateTime(2024,1,15)) == DateTime(2024,1,15,12,0,0)
end

@testset "Function ref + resolve" begin
    # Named module function round-trips
    ref = Tempus._function_ref(Tempus.parseCron)
    @test ref == "Tempus.parseCron"
    @test Tempus.resolve_function(ref) === Tempus.parseCron

    # Anonymous function returns nothing
    @test Tempus._function_ref(() -> nothing) === nothing

    # Bare name resolves via Main
    @test Tempus.resolve_function("_filestore_test_action") === _filestore_test_action
end

@testset "Job with params" begin
    handler(; msg="default") = msg
    job = Tempus.Job(handler, "param_test", "0 0 * * * *"; job_params=Dict("msg" => "hello"))
    @test job.action_ref !== nothing
    @test job.action_data !== nothing
    parsed = JSON.parse(job.action_data)
    @test parsed["msg"] == "hello"
    @test job.action(; parsed...) == "hello"
end

@testset "getnext rollover regressions" begin
    # every one of these threw an out-of-range ArgumentError, hit an
    # UndefVarError, or looped forever before the rollover rewrite
    @test getnext(parseCron("* * * * 0"), DateTime(2021, 1, 2)) == DateTime(2021, 1, 3)  # Sunday never matched dayofweek() == 7
    @test getnext(parseCron("0 * * * * 0"), DateTime(2021, 1, 3, 10, 30, 0)) == DateTime(2021, 1, 3, 10, 31, 0)
    @test getnext(parseCron("* * * * 2"), DateTime(2021, 1, 4)) == DateTime(2021, 1, 5)  # UndefVarError branch (advancing to a later weekday)
    @test getnext(parseCron("0 0 30 * *"), DateTime(2021, 2, 5)) == DateTime(2021, 3, 30)  # "Day: 30 out of range" for February
    @test getnext(parseCron("0 0 15 * *"), DateTime(2021, 12, 20)) == DateTime(2022, 1, 15)  # "Month: 13 out of range"
    @test getnext(parseCron("* * * * 2"), DateTime(2021, 1, 30)) == DateTime(2021, 2, 2)  # invalid date via the day-of-week path
    @test getnext(parseCron("0 0 12 * * *"), DateTime(2021, 1, 31, 13, 0, 0)) == DateTime(2021, 2, 1, 12, 0, 0)  # "Day: 32 out of range"
    @test getnext(parseCron("30 * * * *"), DateTime(2021, 1, 15, 23, 45, 0)) == DateTime(2021, 1, 16, 0, 30, 0)  # "Hour: 24 out of range"
    @test getnext(parseCron("30 * * * * *"), DateTime(2021, 1, 15, 23, 59, 45)) == DateTime(2021, 1, 16, 0, 0, 30)  # "Minute: 60 out of range"
    @test getnext(parseCron("0 0 29 2 *"), DateTime(2021, 3, 1)) == DateTime(2024, 2, 29)  # multi-year day search
    # an expression that can never fire is a bounded error, not an infinite loop
    @test_throws ArgumentError getnext(parseCron("0 0 30 2 *"), DateTime(2021, 1, 1))
end

@testset "Step field semantics" begin
    # steps advance from the range start (standard cron: 10-30/7 means
    # 10,17,24), not from multiples of the step value
    @test getnext(parseCron("10-30/7 * * * *"), DateTime(2021, 1, 1, 0, 0, 0)) == DateTime(2021, 1, 1, 0, 10, 0)
    @test getnext(parseCron("10-30/7 * * * *"), DateTime(2021, 1, 1, 0, 18, 0)) == DateTime(2021, 1, 1, 0, 24, 0)
    @test getnext(parseCron("10-30/7 * * * *"), DateTime(2021, 1, 1, 0, 25, 0)) == DateTime(2021, 1, 1, 1, 10, 0)
    # day-of-month wildcards step from 1: */10 means the 1st, 11th, 21st, 31st
    @test getnext(parseCron("0 0 */10 * *"), DateTime(2021, 1, 2)) == DateTime(2021, 1, 11)
    # a stepped range that excludes the current value must wrap to the range
    # start (this used to advance a year per iteration without terminating)
    @test getnext(parseCron("0 0 1 4-6/2 *"), DateTime(2021, 8, 15)) == DateTime(2022, 4, 1)
    @test getnext(parseCron("0 0 1 4-6/2 *"), DateTime(2021, 5, 15)) == DateTime(2021, 6, 1)
    # day-of-week steps count from Sunday (previously a missing method)
    @test getnext(parseCron("0 0 0 * * */2"), DateTime(2021, 1, 2)) == DateTime(2021, 1, 3)
    # a zero step would divide by zero at evaluation time
    @test_throws ArgumentError parseCron("*/0 * * * *")
    # wildcard steps are unchanged: */15 still means 0,15,30,45
    @test getnext(parseCron("*/15 * * * *"), DateTime(2021, 1, 1, 0, 12, 0)) == DateTime(2021, 1, 1, 0, 15, 0)
end

@testset "Cron parsing extensions" begin
    # 7 is Sunday, same as 0 (standard cron accepts both)
    @test getnext(parseCron("* * * * 7"), DateTime(2021, 1, 2)) == DateTime(2021, 1, 3)
    @test getnext(parseCron("0 0 0 * * 5-7"), DateTime(2021, 1, 9)) == DateTime(2021, 1, 10)   # Sat -> Sun via a range through 7
    @test getnext(parseCron("0 0 0 * * 5-7"), DateTime(2021, 1, 4)) == DateTime(2021, 1, 8)    # Mon -> Fri
    # month and day-of-week names, case-insensitive
    @test getnext(parseCron("0 0 * * MON-FRI"), DateTime(2021, 1, 2)) == DateTime(2021, 1, 4)
    @test getnext(parseCron("0 0 1 JAN *"), DateTime(2021, 3, 1)) == DateTime(2022, 1, 1)
    @test getnext(parseCron("0 0 * * sun"), DateTime(2021, 1, 4)) == DateTime(2021, 1, 10)
    # @-aliases
    @test getnext(parseCron("@daily"), DateTime(2021, 1, 1, 5, 0, 0)) == DateTime(2021, 1, 2)
    @test getnext(parseCron("@hourly"), DateTime(2021, 1, 1, 5, 30, 0)) == DateTime(2021, 1, 1, 6, 0, 0)
    @test getnext(parseCron("@weekly"), DateTime(2021, 1, 4)) == DateTime(2021, 1, 10)
    @test getnext(parseCron("@monthly"), DateTime(2021, 1, 4)) == DateTime(2021, 2, 1)
    @test getnext(parseCron("@yearly"), DateTime(2021, 1, 4)) == DateTime(2022, 1, 1)
    # whitespace runs and surrounding whitespace are tolerated
    @test getnext(parseCron("  0  0 * * *\t"), DateTime(2021, 1, 1, 5, 0, 0)) == DateTime(2021, 1, 2)
    @test_throws ArgumentError parseCron("@reboot")
    @test_throws ArgumentError parseCron("0 0 * * MONDAY-FRI")
    @test_throws ArgumentError parseCron("0 0 * * FRI-MON")
    @test_throws ArgumentError parseCron("* * * * 8")
end

@testset "close timeout failsafe" begin
    # Closing a scheduler that never started is already complete.
    fresh = Tempus.Scheduler(; logging=false)
    elapsed = @elapsed close(fresh; timeout=2)
    @test elapsed < 1
    fresh_wait = @async wait(fresh)
    @test Base.timedwait(() -> istaskdone(fresh_wait), 1) == :ok

    # A job that outlives the close timeout must not block close. A timeout must
    # also not signal actual completion or permit a restart that forgets the
    # still-running execution.
    started = Channel{Nothing}(1)
    release = Channel{Nothing}(1)
    hung = Tempus.OneShotJob(
        () -> (put!(started, nothing); take!(release); nothing),
        "hung_close_job",
    )
    scheduler = Tempus.Scheduler(; logging=false)
    Tempus.run!(scheduler)
    push!(scheduler, hung)
    take!(started)
    t0 = time()
    close(scheduler; timeout=0.2)
    @test time() - t0 < 2
    @test_throws ArgumentError Tempus.run!(scheduler)

    completion_wait = @async wait(scheduler)
    yield()
    @test !istaskdone(completion_wait)
    put!(release, nothing)
    @test Base.timedwait(() -> istaskdone(completion_wait), 5) == :ok

    # Once the prior loop and execution are truly done, this scheduler is safe
    # to reuse. The successful one-shot is disabled from its stored history.
    Tempus.run!(scheduler; close_when_no_jobs=true)
    wait(scheduler)
    close(scheduler; timeout=1)
end

@testset "run! initialization failure leaves scheduler stopped" begin
    jobs = MemoryStore{Tempus.Job}()
    store = Tempus.Store(jobs, FailingHistoryStore())
    Tempus.addJob!(store, Tempus.OneShotJob(() -> nothing, "init_failure"))
    scheduler = Tempus.Scheduler(store; logging=false)

    @test_throws ErrorException Tempus.run!(scheduler)
    @test !scheduler.running
    @test !scheduler.loopActive
    @test isempty(scheduler.jobExecutions)
    @test isempty(scheduler.executingJobExecutions)
    @test (@elapsed close(scheduler; timeout=1)) < 1
end

@testset "runJobs! waits for in-flight executions" begin
    # with close_when_no_jobs, the scheduler loop used to break as soon as the
    # queue was empty even though an execution was still running — and since
    # only close() cleared scheduler.running, the finishing execution never
    # notified jobExecutionFinished and wait(scheduler) hung forever
    runs = Ref(0)
    slow = Tempus.OneShotJob(() -> (sleep(2); runs[] += 1), "slow_oneshot_wait")
    Tempus.runJobs!(Tempus.InMemoryStore(), [slow]; logging=false)
    @test runs[] == 1
end

@testset "One-shot single pending execution" begin
    # dispatching a one-shot used to pre-schedule a duplicate immediate
    # execution (the history check ran before the first attempt recorded),
    # which double-ran the job under :concurrent overlap
    runs = Ref(0)
    job = Tempus.OneShotJob(() -> (sleep(1.2); runs[] += 1), "oneshot_concurrent_once")
    Tempus.runJobs!(Tempus.InMemoryStore(), [job]; overlap_policy=:concurrent, max_concurrent_executions=4, logging=false)
    @test runs[] == 1

    # a failing one-shot is still re-attempted (now scheduled at completion
    # rather than speculatively at dispatch) until max_failed_executions
    attempts = Ref(0)
    failing = Tempus.OneShotJob(() -> (attempts[] += 1; error("boom")), "oneshot_reattempt"; retries=0)
    Tempus.runJobs!(Tempus.InMemoryStore(), [failing]; retries=0, max_failed_executions=2, logging=false)
    @test attempts[] == 2
end

@testset "Saturated scheduler queue stays bounded" begin
    # at the concurrency limit the loop used to schedule \"next\" executions
    # every pass; for jobs without a cron schedule each got a fresh
    # millisecond timestamp, defeating dedup and growing the queue unboundedly
    store = Tempus.InMemoryStore()
    scheduler = Tempus.Scheduler(store; max_concurrent_executions=1, logging=false)
    Tempus.run!(scheduler)
    push!(scheduler, Tempus.Job(() -> sleep(6), "blocker", "* * * * * *"))
    push!(scheduler, Tempus.Job(() -> nothing, "starved", "* * * * * *"))
    sleep(5)
    @test length(scheduler.jobExecutions) <= 4
    close(scheduler; timeout=2)
end

@testset "Re-push replaces queued executions" begin
    scheduler = Tempus.Scheduler(; logging=false)
    Tempus.run!(scheduler)
    job = Tempus.Job(() -> nothing, "repush", "0 0 1 1 *")
    push!(scheduler, job)
    push!(scheduler, job)
    @test count(je -> je.job.name == "repush", scheduler.jobExecutions) == 1
    close(scheduler; timeout=2)

    # A finishing execution from the old definition must use the current stored
    # job's options. The old max_executions=1 used to disable this replacement.
    started = Channel{Nothing}(1)
    release = Channel{Nothing}(1)
    new_ran = Channel{Nothing}(1)
    store = Tempus.InMemoryStore()
    scheduler = Tempus.Scheduler(store; logging=false)
    old_job = Tempus.Job(
        () -> (put!(started, nothing); take!(release); nothing),
        "running_replacement",
        "* * * * * *";
        max_executions=1,
    )
    new_job = Tempus.Job(
        () -> put!(new_ran, nothing),
        "running_replacement",
        "* * * * * *",
    )
    Tempus.run!(scheduler)
    push!(scheduler, old_job)
    take!(started)
    push!(scheduler, new_job)
    put!(release, nothing)
    @test Base.timedwait(() -> isready(new_ran), 5) == :ok
    take!(new_ran)
    stored = only(filter(job -> job.name == new_job.name, Tempus.getJobs(store)))
    @test !Tempus.isdisabled(stored)
    close(scheduler; timeout=2)
end

@testset "Failed execution is showable" begin
    store = Tempus.InMemoryStore()
    job = Tempus.OneShotJob(() -> error("x"), "showable_failure"; retries=0)
    Tempus.runJobs!(store, [job]; retries=0, max_failed_executions=1, logging=false)
    history = Tempus.getNMostRecentJobExecutions(store, "showable_failure", 5)
    @test !isempty(history)
    @test history[1].status == :failed
    @test history[1].result === nothing  # was left #undef, so show() threw UndefRefError
    @test sprint(show, history[1]) isa String
end

@testset "unschedule!" begin
    store = Tempus.InMemoryStore()
    scheduler = Tempus.Scheduler(store; logging=false)
    Tempus.run!(scheduler)

    # A queued execution is cancelled with its stored job and history.
    job = Tempus.Job(() -> nothing, "unsched", "0 0 1 1 *")
    push!(scheduler, job)
    Tempus.unschedule!(scheduler, job)
    @test isempty(Tempus.getJobs(store))
    @test isempty(Tempus.getNMostRecentJobExecutions(store, "unsched", 10))
    @test all(je -> je.job.name != "unsched", scheduler.jobExecutions)

    # Completion must be serialized with unschedule!. It used to store history
    # after the purge and silently recreate the execution-history key.
    started = Channel{Nothing}(1)
    release = Channel{Nothing}(1)
    running = Tempus.OneShotJob(
        () -> (put!(started, nothing); take!(release); nothing),
        "unsched_running",
    )
    push!(scheduler, running)
    take!(started)
    Tempus.unschedule!(scheduler, running)
    put!(release, nothing)
    @test Base.timedwait(
        () -> (@lock scheduler.lock isempty(scheduler.executingJobExecutions)),
        5,
    ) == :ok
    @test isempty(Tempus.getJobs(store))
    @test isempty(Tempus.getNMostRecentJobExecutions(store, "unsched_running", 10))
    @test all(je -> je.job.name != "unsched_running", scheduler.jobExecutions)
    close(scheduler; timeout=3)
end

@testset "Options validation" begin
    @test_throws ArgumentError Tempus.Job(() -> nothing, "v1", "* * * * *"; overlap_policy=:sometimes)
    @test_throws ArgumentError Tempus.Job(() -> nothing, "v2", "* * * * *"; timezone="America/Nowhere")
    @test_throws ArgumentError Tempus.Job(() -> nothing, "v3", "* * * * *"; retries=-1)
    @test_throws ArgumentError Tempus.Job(() -> nothing, "v4", "* * * * *"; max_executions=0)
    @test_throws ArgumentError Tempus.Scheduler(; max_concurrent_executions=0)
    @test_throws ArgumentError close(Tempus.Scheduler(; logging=false); timeout=-1)
    @test_throws ArgumentError close(Tempus.Scheduler(; logging=false); timeout=Inf)
    scheduler = Tempus.Scheduler(; logging=false)
    Tempus.run!(scheduler)
    @test_throws ArgumentError Tempus.run!(scheduler)  # second loop would double-dispatch
    close(scheduler; timeout=2)
end
