mutable struct DispatchFaultHistory <: AbstractStore{Vector{Tempus.JobExecution}}
    backing::MemoryStore{Vector{Tempus.JobExecution}}
    reads::Int
    fail_on::Int
    exception::ErrorException
    failed_task::Union{Nothing,Task}
end

function Base.get(history::DispatchFaultHistory, key::AbstractString, default)
    history.reads += 1
    if history.reads == history.fail_on
        history.failed_task = current_task()
        throw(history.exception)
    end
    return get(history.backing, key, default)
end

AbstractStores.modify!(f, history::DispatchFaultHistory, key::AbstractString; kw...) =
    AbstractStores.modify!(f, history.backing, key; kw...)

@testset "Dispatch failure releases only unlaunched reservations" begin
    # The overlap case fails during selection, before removing any selected
    # execution. The other cases fail at the first or second queue removal.
    for (failure_index, with_prior, overlap) in
        ((1, false, false), (2, false, false), (1, true, false),
         (2, true, false), (1, true, true))
        @testset "history read $failure_index, prior=$with_prior, overlap=$overlap" begin
            history = DispatchFaultHistory(MemoryStore{Vector{Tempus.JobExecution}}(),
                0, 0, ErrorException("dispatch history unavailable"), nothing)
            store = Tempus.Store(MemoryStore{Tempus.Job}(), history)
            scheduler = Tempus.Scheduler(store; logging=false,
                max_concurrent_executions=4, overlap_policy=:queue)
            started = Channel{Nothing}(1)
            release = Channel{Nothing}(1)
            ran = Channel{String}(2)
            prior = Tempus.OneShotJob(
                () -> (put!(started, nothing); take!(release); nothing), "prior")
            pending = [Tempus.JobExecution(
                Tempus.OneShotJob(() -> put!(ran, name), name),
                DateTime(2000) + Second(i),
            ) for (i, name) in enumerate(("first", "second"))]
            try
                @lock scheduler.lock begin
                    Tempus.run!(scheduler; close_when_no_jobs=true)
                    if with_prior
                        Tempus.addJob!(store, prior)
                        push!(scheduler.jobExecutions,
                            Tempus.JobExecution(prior, DateTime(2000)))
                    else
                        for je in pending
                            Tempus.addJob!(store, je.job)
                            push!(scheduler.jobExecutions, je)
                        end
                        history.fail_on = history.reads + failure_index
                    end
                end
                if with_prior
                    take_within!(started)
                    @lock scheduler.lock begin
                        for je in pending
                            Tempus.addJob!(store, je.job)
                            push!(scheduler.jobExecutions, je)
                        end
                        if overlap
                            push!(scheduler.jobExecutions,
                                Tempus.JobExecution(prior, DateTime(2000) + Second(3)))
                        end
                        history.fail_on = history.reads + failure_index
                    end
                end

                @test waitfor(() -> @lock scheduler.lock !scheduler.loopActive)
                @test !scheduler.running
                @test !isready(ran)
                @test history.failed_task !== nothing
                if history.failed_task !== nothing
                    @test_throws TaskFailedException wait(history.failed_task)
                    @test only(Base.current_exceptions(history.failed_task)).exception === history.exception
                end
                @lock scheduler.lock begin
                    @test length(scheduler.executingJobExecutions) == Int(with_prior)
                    @test all(je -> je.job === prior, scheduler.executingJobExecutions)
                    @test all(je -> count(queued -> queued === je, scheduler.jobExecutions) == 1, pending)
                    @test issorted(scheduler.jobExecutions; by=je -> je.scheduledStart)
                end

                waiter = @async wait(scheduler)
                yield()
                close(scheduler; timeout=0)
                if with_prior
                    @test !istaskdone(waiter)
                    @test_throws ArgumentError Tempus.run!(scheduler)
                    put!(release, nothing)
                end
                @test waitfor(() -> istaskdone(waiter), 2)

                # Restart uses stored definitions/history, so neither a lost
                # pending one-shot nor a repeated successful one-shot is okay.
                history.fail_on = 0
                Tempus.run!(scheduler; close_when_no_jobs=true)
                @test Set((take_within!(ran), take_within!(ran))) == Set(("first", "second"))
                @test waitfor(() -> @lock scheduler.lock !scheduler.loopActive)
                @test !isready(ran)
                @test isempty(scheduler.executingJobExecutions)
                for je in pending
                    executions = Tempus.getNMostRecentJobExecutions(store, je.job.name, 10)
                    @test length(executions) == 1
                    @test only(executions).status == :succeeded
                end
                if with_prior
                    @test length(Tempus.getNMostRecentJobExecutions(store, prior.name, 10)) == 1
                    @test !isready(started)
                end
            finally
                history.fail_on = 0
                with_prior && !isready(release) && put!(release, nothing)
                close(scheduler; timeout=2)
            end
        end
    end
end

@testset "Successful task handoff leaves the pending batch" begin
    scheduler = Tempus.Scheduler(; logging=false)
    started = Channel{Nothing}(1)
    release = Channel{Nothing}(1)
    executions = [Tempus.JobExecution(Tempus.OneShotJob(
        () -> (put!(started, nothing); take!(release); nothing), string(i)),
        DateTime(2000) + Second(i)) for i in 1:3]
    # Dispatch order is descending queue index because removal preserves indexes.
    ready = [(i, false, executions[i]) for i in 3:-1:1]
    union!(scheduler.executingJobExecutions, executions)
    calls = Tempus.JobExecution[]
    launch_error = ErrorException("task launch unavailable")
    launch = function (scheduler, je)
        push!(calls, je)
        length(calls) == 2 && throw(launch_error)
        Tempus.executeJob!(scheduler, je)
    end
    try
        caught = try
            Tempus._launch_ready!(launch, scheduler, ready)
        catch e
            e
        end
        @test caught === launch_error
        take_within!(started)
        @test calls == executions[[3, 2]]
        @test Set(last.(ready)) == Set(executions[1:2])
        @test executions[3] in scheduler.executingJobExecutions
        @test all(entry -> last(entry) !== executions[3], ready)
    finally
        # Only the successful handoff owns a running task. The retained batch
        # remains the dispatch loop's responsibility after the launcher fails.
        for (_, _, je) in ready
            delete!(scheduler.executingJobExecutions, je)
        end
        put!(release, nothing)
        close(scheduler; timeout=2)
    end
    @test isempty(scheduler.executingJobExecutions)
end
