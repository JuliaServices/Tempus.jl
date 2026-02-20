module TempusSQLiteExt

using Tempus
using SQLite
using Dates
using JSON

const SCHEMA_VERSION = "2"

# --- Schema ---

function init_tempus_schema!(db::SQLite.DB)
    SQLite.execute(db, """
        CREATE TABLE IF NOT EXISTS tempus_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    # Check schema version
    rows = SQLite.DBInterface.execute(db, "SELECT value FROM tempus_meta WHERE key = 'schema_version'")
    existing = nothing
    for row in rows
        existing = row.value
    end
    if existing !== nothing && existing != SCHEMA_VERSION
        error("Incompatible Tempus database schema version: $existing (expected $SCHEMA_VERSION). Tempus 2.0 requires a fresh database.")
    end
    SQLite.execute(db, "INSERT OR IGNORE INTO tempus_meta (key, value) VALUES ('schema_version', '$SCHEMA_VERSION')")
    SQLite.execute(db, """
        CREATE TABLE IF NOT EXISTS tempus_jobs (
            name TEXT PRIMARY KEY,
            schedule TEXT,
            timezone TEXT,
            action_ref TEXT NOT NULL,
            action_data TEXT,
            overlap_policy TEXT,
            retries INTEGER NOT NULL DEFAULT 0,
            max_failed_executions INTEGER,
            max_executions INTEGER,
            expires_at TEXT,
            disabled_at TEXT
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
    jobs_by_name = Dict{String, Tempus.Job}()
    # Load jobs
    rows = SQLite.DBInterface.execute(db, """
        SELECT name, schedule, timezone, action_ref, action_data,
               overlap_policy, retries, max_failed_executions, max_executions,
               expires_at, disabled_at
        FROM tempus_jobs
    """)
    for row in rows
        action = Tempus.resolve_function(String(row.action_ref))
        schedule_raw = row.schedule === missing || row.schedule === nothing ? nothing : String(strip(String(row.schedule), '"'))
        schedule = schedule_raw === nothing ? nothing : Tempus.parseCron(schedule_raw)
        action_data = row.action_data === missing || row.action_data === nothing ? nothing : String(row.action_data)
        opts = Tempus.JobOptions(;
            overlap_policy = _nullable_symbol(row.overlap_policy),
            retries = row.retries,
            max_failed_executions = _nullable_int(row.max_failed_executions),
            max_executions = _nullable_int(row.max_executions),
            expires_at = _nullable_datetime(row.expires_at),
            timezone = _nullable_string(row.timezone),
        )
        disabled_at = _nullable_datetime(row.disabled_at)
        job = Tempus.Job(ReentrantLock(), action, String(row.action_ref), action_data, String(row.name), schedule, opts, disabled_at)
        jobs_by_name[job.name] = job
        @lock store.cache.lock push!(store.cache.jobs, job)
    end
    # Load executions
    rows = SQLite.DBInterface.execute(db, """
        SELECT id, job_name, scheduled_start, actual_start, finish, status, run_concurrently
        FROM tempus_job_executions ORDER BY scheduled_start DESC
    """)
    for row in rows
        job_name = String(row.job_name)
        haskey(jobs_by_name, job_name) || continue
        job = jobs_by_name[job_name]
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
        je.runConcurrently = row.run_concurrently == 1
        @lock store.cache.lock begin
            execs = get!(() -> Tempus.JobExecution[], store.cache.jobExecutions, job_name)
            pushfirst!(execs, je)
        end
    end
end

# --- Nullable helpers ---

_nullable_string(v) = (v === missing || v === nothing) ? nothing : String(v)
_nullable_int(v) = (v === missing || v === nothing) ? nothing : Int(v)
_nullable_symbol(v) = (v === missing || v === nothing) ? nothing : Symbol(v)
_nullable_datetime(v) = (v === missing || v === nothing) ? nothing : DateTime(v)

# --- Sync helpers ---

function sync_job_to_sqlite!(db::SQLite.DB, job::Tempus.Job)
    SQLite.execute(db, """
        INSERT OR REPLACE INTO tempus_jobs
        (name, schedule, timezone, action_ref, action_data, overlap_policy, retries,
         max_failed_executions, max_executions, expires_at, disabled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job.name,
        job.schedule === nothing ? nothing : strip(string(job.schedule), '"'),
        job.options.timezone,
        job.action_ref,
        job.action_data,
        job.options.overlap_policy === nothing ? nothing : string(job.options.overlap_policy),
        job.options.retries,
        job.options.max_failed_executions,
        job.options.max_executions,
        job.options.expires_at === nothing ? nothing : string(job.options.expires_at),
        job.disabledAt === nothing ? nothing : string(job.disabledAt),
    ))
end

function sync_execution_to_sqlite!(db::SQLite.DB, je::Tempus.JobExecution)
    actual_start = isdefined(je, :actualStart) ? string(je.actualStart) : nothing
    finish = isdefined(je, :finish) ? string(je.finish) : nothing
    status = isdefined(je, :status) ? string(je.status) : nothing
    SQLite.execute(db, """
        INSERT OR REPLACE INTO tempus_job_executions
        (id, job_name, scheduled_start, actual_start, finish, status, run_concurrently)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (je.jobExecutionId, je.job.name, string(je.scheduledStart),
          actual_start, finish, status, je.runConcurrently ? 1 : 0))
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
