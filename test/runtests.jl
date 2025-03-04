using Test, Dates, Tempus

import Tempus: parseCronField, parseCron, getnext

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
    @test cronObj.second isa Tempus.Wildcard
    @test cronObj.minute isa Tempus.Wildcard
    @test cronObj.hour isa Tempus.Wildcard
    @test cronObj.day_of_month isa Tempus.Wildcard
    @test cronObj.month isa Tempus.Wildcard
    @test cronObj.day_of_week isa Tempus.Wildcard

    cron = "*/15 * * * *"
    cronObj = parseCron(cron)
    @test cronObj.second isa Tempus.Wildcard
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

    # 10. Edge-case: Leap year.
    @testset "Leap year" begin
        cron = parseCron("* * * * *")
        dt = DateTime(2020, 2, 28, 23, 59, 59)
        next = getnext(cron, dt)
        # Expected: Since it’s the last minute of February in a leap year, the next trigger should be Feb 29, 00:00:00
        @test next == DateTime(2020, 2, 29, 0, 0, 0)
    end
end

# -- Higher-Level Tests for Scheduler --

@testset "Scheduler Scheduling and Execution" begin
    # We'll create a simple job that records its execution time.
    executed = Ref{Vector{DateTime}}([])
    test_job = Tempus.Job(:testjob, "* * * * * *") do
        push!(executed[], Dates.now(UTC))
    end

    # Create a scheduler with default in-memory store.
    scheduler = Tempus.Scheduler()

    try
        Tempus.run!(scheduler)

        # Schedule the test job.
        push!(scheduler, test_job)

        # Job should run every second, so we wait for a few seconds and check if it ran.
        sleep(3)  # wait for the job to run multiple times
        @test length(executed[]) > 0
    finally
        close(scheduler)
    end

    # simple FileStore
    empty!(executed[])
    mktemp() do path, io
        fs = Tempus.FileStore(path)
        sched = Tempus.Scheduler(fs)
        try
            Tempus.run!(sched)
            push!(sched, test_job)
            sleep(3)
            @test length(executed[]) > 0
        finally
            close(sched)
        end
        # now run again, checking that our job was successfully persistent in the file
        empty!(executed[])
        fs = Tempus.FileStore(path)
        sched = Tempus.Scheduler(fs)
        try
            Tempus.run!(sched)
            sleep(3)
            @test length(executed[]) > 0
        finally
            close(sched)
        end
    end
end