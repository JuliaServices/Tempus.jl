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
- In-memory and file-based job storage backends
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
job = Tempus.Job("example_job", "* * * * *", () -> println("Hello from Tempus!"))

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

### Removing a Job
```julia
Tempus.unschedule!(scheduler, job)
```

### Using a File-Based Job Store
To persist job execution history across restarts, use a file-based store:
```julia
store = Tempus.FileStore("jobs.dat")
scheduler = Tempus.Scheduler(store)
```

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