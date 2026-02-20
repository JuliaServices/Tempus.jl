using Test, Dates, JSON, SQLite, Tempus

import Tempus: parseCronField, parseCron, getnext

# Named function for FileStore persistence test (anonymous functions can't be persisted)
_filestore_test_action() = (global executed; push!(executed, Dates.now(UTC)))
_sqlite_test_action() = nothing

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

    # simple FileStore (requires named function for persistence)
    empty!(executed)
    mktemp() do path, io
        fs_job = Tempus.Job(_filestore_test_action, "testjob_fs", "* * * * * *")
        fs = Tempus.FileStore(path)
        withscheduler(fs) do sch
            push!(sch, fs_job)
            sleep(3)
        end
        @test length(executed) > 0
        # now run again, checking that our job was successfully persisted in the file
        empty!(executed)
        fs = Tempus.FileStore(path)
        withscheduler(fs) do sch
            sleep(3)
        end
        @show executed
        @test length(executed) > 0
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
