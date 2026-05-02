-- StravaFit schema. Re-running these statements is a no-op.
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS tokens (
    service       TEXT PRIMARY KEY,           -- 'strava' | 'google'
    access_token  TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    expires_at    INTEGER NOT NULL            -- unix seconds
);

CREATE TABLE IF NOT EXISTS processed_activities (
    strava_id   INTEGER PRIMARY KEY,
    external_id TEXT,                         -- source data point id (Google Health: numeric string)
    merged_at   INTEGER NOT NULL,
    result      TEXT NOT NULL,                -- 'success' | 'skipped:no_match' | 'pending_manual_review' | 'error:...'
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    strava_id     INTEGER NOT NULL,
    external_id   TEXT,                        -- source data point id
    trigger       TEXT NOT NULL,               -- 'webhook' | 'manual' | 'preview'
    status        TEXT NOT NULL,               -- 'queued' | 'running' | 'awaiting_delete' | 'success' | 'error'
    dry_run       INTEGER NOT NULL DEFAULT 0,  -- legacy boolean kept for back-compat
    mode          TEXT NOT NULL DEFAULT 'auto',-- 'dry_run' | 'auto' | 'semi_auto'
    started_at    INTEGER,
    finished_at   INTEGER,
    error         TEXT,
    log           TEXT,
    recovery_path TEXT                         -- on-disk merged FIT for failed replace ops
);

CREATE INDEX IF NOT EXISTS idx_jobs_strava_id ON jobs(strava_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status    ON jobs(status);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO settings (key, value) VALUES
    ('auto_merge_enabled', 'true'),
    ('default_dry_run',    'false');
