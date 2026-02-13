module TempusSQLiteExt

using Tempus
using SQLite
using Dates
using Serialization

# --- Schema ---

function init_tempus_schema!(db::SQLite.DB)
    SQLite.execute(db, """
        CREATE TABLE IF NOT EXISTS tempus_jobs (
            name TEXT PRIMARY KEY,
            schedule TEXT,
            options BLOB,
            disabled_at TEXT,
            action BLOB NOT NULL
        )
    """)
    SQLite.execute(db, """
        CREATE TABLE IF NOT EXISTS tempus_job_executions (
            id TEXT PRIMARY KEY,
            job_name TEXT NOT NULL,
            scheduled_start TEXT NOT NULL,
            actual_start TEXT,
            finish TEXT,
            status TEXT,
            result BLOB,
            exception BLOB,
            run_concurrently INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (job_name) REFERENCES tempus_jobs(name) ON DELETE CASCADE
        )
    """)
    SQLite.execute(db, """
        CREATE INDEX IF NOT EXISTS idx_tempus_executions_job
        ON tempus_job_executions(job_name, scheduled_start DESC)
    """)
end

# --- Constructor ---

function Tempus.SQLiteStore(db::SQLite.DB)
    init_tempus_schema!(db)
    cache = Tempus.InMemoryStore()
    store = Tempus.SQLiteStore(ReentrantLock(), db, cache)
    load_from_sqlite!(store)
    return store
end

# --- Load from SQLite into cache ---

function load_from_sqlite!(store::Tempus.SQLiteStore)
    db = store.db::SQLite.DB
    # Load jobs
    rows = SQLite.DBInterface.execute(db, "SELECT name, schedule, options, disabled_at, action FROM tempus_jobs")
    for row in rows
        action = deserialize(IOBuffer(row.action))
        schedule_raw = row.schedule === missing || row.schedule === nothing ? nothing : String(strip(String(row.schedule), '"'))
        schedule = schedule_raw === nothing ? nothing : Tempus.parseCron(schedule_raw)
        options = row.options === missing || row.options === nothing ? Tempus.JobOptions() : deserialize(IOBuffer(row.options))
        disabled_at = row.disabled_at === missing || row.disabled_at === nothing ? nothing : DateTime(row.disabled_at)
        job = Tempus.Job(ReentrantLock(), action, String(row.name), schedule, options, disabled_at)
        @lock store.cache.lock push!(store.cache.jobs, job)
    end
    # Load executions
    rows = SQLite.DBInterface.execute(db, """
        SELECT id, job_name, scheduled_start, actual_start, finish, status, result, exception, run_concurrently
        FROM tempus_job_executions ORDER BY scheduled_start DESC
    """)
    for row in rows
        job_name = String(row.job_name)
        # Find the job in cache
        job = nothing
        for j in store.cache.jobs
            if j.name == job_name
                job = j
                break
            end
        end
        job === nothing && continue
        je = Tempus.JobExecution(job, DateTime(row.scheduled_start))
        if row.actual_start !== missing && row.actual_start !== nothing
            je.actualStart = DateTime(row.actual_start)
        end
        if row.finish !== missing && row.finish !== nothing
            je.finish = DateTime(row.finish)
        end
        if row.status !== missing && row.status !== nothing
            je.status = Symbol(row.status)
        end
        if row.result !== missing && row.result !== nothing
            try
                je.result = deserialize(IOBuffer(row.result))
            catch
            end
        end
        if row.exception !== missing && row.exception !== nothing
            try
                je.exception = deserialize(IOBuffer(row.exception))
            catch
            end
        end
        je.runConcurrently = row.run_concurrently == 1
        @lock store.cache.lock begin
            execs = get!(() -> Tempus.JobExecution[], store.cache.jobExecutions, job_name)
            pushfirst!(execs, je)
        end
    end
end

# --- Sync helpers ---

function serialize_to_blob(obj)
    io = IOBuffer()
    serialize(io, obj)
    return take!(io)
end

function sync_job_to_sqlite!(db::SQLite.DB, job::Tempus.Job)
    schedule_str = job.schedule === nothing ? nothing : strip(string(job.schedule), '"')
    disabled_str = job.disabledAt === nothing ? nothing : string(job.disabledAt)
    SQLite.execute(db, """
        INSERT OR REPLACE INTO tempus_jobs (name, schedule, options, disabled_at, action)
        VALUES (?, ?, ?, ?, ?)
    """, (job.name, schedule_str, serialize_to_blob(job.options), disabled_str, serialize_to_blob(job.action)))
end

function sync_execution_to_sqlite!(db::SQLite.DB, je::Tempus.JobExecution)
    actual_start = isdefined(je, :actualStart) ? string(je.actualStart) : nothing
    finish = isdefined(je, :finish) ? string(je.finish) : nothing
    status = isdefined(je, :status) ? string(je.status) : nothing
    result_blob = isdefined(je, :result) ? serialize_to_blob(je.result) : nothing
    exception_blob = isdefined(je, :exception) && je.exception !== nothing ? serialize_to_blob(je.exception) : nothing
    SQLite.execute(db, """
        INSERT OR REPLACE INTO tempus_job_executions
        (id, job_name, scheduled_start, actual_start, finish, status, result, exception, run_concurrently)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (je.jobExecutionId, je.job.name, string(je.scheduledStart),
          actual_start, finish, status, result_blob, exception_blob, je.runConcurrently ? 1 : 0))
end

# --- Store interface ---

function Tempus.addJob!(store::Tempus.SQLiteStore, job::Tempus.Job)
    @lock store.lock begin
        Tempus.addJob!(store.cache, job)
        sync_job_to_sqlite!(store.db::SQLite.DB, job)
    end
end

function Tempus.purgeJob!(store::Tempus.SQLiteStore, job::Tempus.Job)
    @lock store.lock begin
        Tempus.purgeJob!(store.cache, job)
        db = store.db::SQLite.DB
        SQLite.execute(db, "DELETE FROM tempus_job_executions WHERE job_name = ?", (job.name,))
        SQLite.execute(db, "DELETE FROM tempus_jobs WHERE name = ?", (job.name,))
    end
end

function Tempus.getJobs(store::Tempus.SQLiteStore)
    return Tempus.getJobs(store.cache)
end

function Tempus.getNMostRecentJobExecutions(store::Tempus.SQLiteStore, jobName::String, n::Int)
    return Tempus.getNMostRecentJobExecutions(store.cache, jobName, n)
end

function Tempus.storeJobExecution!(store::Tempus.SQLiteStore, je::Tempus.JobExecution)
    @lock store.lock begin
        Tempus.storeJobExecution!(store.cache, je)
        sync_execution_to_sqlite!(store.db::SQLite.DB, je)
    end
end

end # module TempusSQLiteExt
