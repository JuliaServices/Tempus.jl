# Tempus.jl

[![Build Status](https://github.com/JuliaServices/Tempus.jl/actions/workflows/ci.yml/badge.svg)](https://github.com/JuliaServices/Tempus.jl/actions)
[![Coverage](https://codecov.io/gh/JuliaServices/Tempus.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/JuliaServices/Tempus.jl)
[![Documentation](https://img.shields.io/badge/docs-stable-blue.svg)](https://JuliaServices.github.io/Tempus.jl/stable/)
[![Dev Documentation](https://img.shields.io/badge/docs-dev-blue.svg)](https://JuliaServices.github.io/Tempus.jl/dev/)

## Overview
**Tempus.jl** is a lightweight, Quartz-inspired job scheduling library for Julia. It provides an easy-to-use API for defining cron-like schedules and executing jobs at specified times.

### Features
- Define jobs using cron-style scheduling expressions
- Support for job execution policies (overlap handling, retries, failure strategies)
- Backend-neutral state through AbstractStores.jl
- Thread-safe scheduling with concurrency-aware execution
- Dynamic job control (enable, disable, unschedule jobs)
- Supports retry policies with exponential backoff

## Installation
Tempus.jl is registered in the Julia General registry. You can install it directly from GitHub:

```julia
using Pkg
Pkg.add("Tempus")
```

## Quick Start

### Defining and Scheduling a Job
```julia
using Tempus

# Define a job that prints a message every minute
job = Tempus.Job(
    () -> println("Hello from Tempus!"),
    "example_job",
    "* * * * *",
)

# Create an in-memory scheduler
scheduler = Tempus.Scheduler()

# Add the job to the scheduler
push!(scheduler, job)

# Start the scheduler (runs in a background thread)
Tempus.run!(scheduler)
```

### Disabling and Enabling Jobs
```julia
Tempus.disable!(job)  # Prevents the job from running
Tempus.enable!(job)   # Allows it to run again
```

These mutate the `Job` object. To disable a job in a persisting store so it stays
disabled across restarts, use `Tempus.disableJob!(store, job)`.

### Removing a Job
```julia
Tempus.unschedule!(scheduler, job)
```

### Choosing a State Backend

Tempus stores jobs and bounded execution history as two namespaced views over
one AbstractStores backend. The backend must accept heterogeneous values — an
`AbstractStore{Any}` — and hand them back with their types intact, which means
either `MemoryStore` or a store using the default `SerializedCodec`. A
`JSONCodec` backend decodes at its own `eltype` and would return
`Dict{String,Any}` where Tempus expects a `Job`. The default scheduler uses
process memory:

```julia
using AbstractStores, Tempus

backend = MemoryStore()
store = Tempus.Store(backend)
scheduler = Tempus.Scheduler(store)
```

Change only the backend to persist jobs and execution history across restarts:

```julia
backend = FileStore(joinpath(homedir(), ".tempus", "state"))
scheduler = Tempus.Scheduler(backend)
```

The same form works with `RedisStore` (needs `using Redis`) and `SQLStore`
(needs `using DBInterface` plus a driver), whose methods live in AbstractStores
package extensions. One backend can also hold state for several libraries and
the application because each user gets a separate prefix:

```julia
backend = SQLStore{Any}(connection; table="service_state")
tempus = Tempus.Store(backend; prefix="tempus/")
```

`Tempus.InMemoryStore()`, `Tempus.FileStore(directory)`, and
`Tempus.SQLiteStore(connection)` remain as convenience constructors. The
Tempus 2 file store used one JSON file; the new file constructor takes a
directory and writes one atomic file per key, so state written by Tempus 2 is
not read or migrated — a scheduler started against an old `jobs.dat` starts
empty. Execution history is now persisted too, which Tempus 2's file store did
not do.

Jobs are persisted with their action function, so use named functions from a
module that is loaded before reopening a persistent store: an action the process
cannot resolve fails the whole load. Anonymous functions and Julia's native
serialized representation are not stable across Julia sessions or Julia
versions.

## Cron Syntax
Tempus.jl uses a familiar cron syntax for scheduling:
```
* * * * *  → Every minute
0 12 * * * → Every day at noon
*/5 * * * * → Every 5 minutes
# also supports second-level precision
* * * * * * → Every second
```

## Contributing
Contributions are welcome! To contribute:
1. Fork the repository.
2. Create a new branch with your feature or bugfix.
3. Submit a pull request with your changes.

Make sure to include tests for any new functionality.

## License
Tempus.jl is licensed under the MIT License.
