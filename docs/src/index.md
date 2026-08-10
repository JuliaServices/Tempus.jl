# Tempus.jl

Quartz-inspired cron job scheduling for Julia: cron expressions (with optional
seconds, month/day names, and `@`-aliases), overlap policies, retries,
execution caps, timezone-aware schedules, and pluggable persistent state
through [AbstractStores.jl](https://github.com/JuliaServices/AbstractStores.jl).

```julia
using Tempus

scheduler = Tempus.Scheduler()          # in-memory state
Tempus.run!(scheduler)                  # dispatch loop runs in the background

push!(scheduler, Tempus.Job(
    () -> println("Hello from Tempus!"),
    "hello_job",
    "* * * * *",                        # every minute
))

# ... later
close(scheduler)
```

See the [README](https://github.com/JuliaServices/Tempus.jl) for a walkthrough
of state backends, job options, and the full cron syntax.

## API Reference

```@autodocs
Modules = [Tempus]
```
