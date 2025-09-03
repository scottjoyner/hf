.bail off
.echo on
PRAGMA foreign_keys=OFF;
BEGIN;
ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'developer';
CREATE TABLE IF NOT EXISTS api_keys (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, api_key TEXT UNIQUE NOT NULL, created_ts INTEGER NOT NULL DEFAULT (strftime('%s','now')), revoked_ts INTEGER);
CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_revoked ON api_keys(revoked_ts);
ALTER TABLE models ADD COLUMN owner_user_id INTEGER;
ALTER TABLE models ADD COLUMN visibility TEXT DEFAULT 'private';
UPDATE models SET visibility='private' WHERE visibility IS NULL;
CREATE TABLE IF NOT EXISTS model_versions (id INTEGER PRIMARY KEY AUTOINCREMENT, repo_id TEXT NOT NULL, version TEXT NOT NULL, notes TEXT, created_ts INTEGER NOT NULL DEFAULT (strftime('%s','now')), UNIQUE(repo_id, version));
CREATE INDEX IF NOT EXISTS idx_versions_repo ON model_versions(repo_id);
ALTER TABLE files ADD COLUMN version TEXT;
CREATE INDEX IF NOT EXISTS idx_files_repo ON files(repo_id);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_files_repo_rfn_ver ON files(repo_id, rfilename, COALESCE(version,''));
ALTER TABLE uploads ADD COLUMN version TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS uniq_upload_target_bucket_key ON uploads(target, bucket, object_key);
CREATE TABLE IF NOT EXISTS platform_model_grants (id INTEGER PRIMARY KEY AUTOINCREMENT, platform_user_id INTEGER NOT NULL, repo_id TEXT NOT NULL, permitted_from_ts INTEGER NOT NULL, permitted_until_ts INTEGER, created_ts INTEGER NOT NULL DEFAULT (strftime('%s','now')), UNIQUE(platform_user_id, repo_id));
CREATE INDEX IF NOT EXISTS idx_grants_platform ON platform_model_grants(platform_user_id);
CREATE INDEX IF NOT EXISTS idx_grants_repo ON platform_model_grants(repo_id);
CREATE TABLE IF NOT EXISTS access_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL DEFAULT (strftime('%s','now')), user_id INTEGER, api_key_id INTEGER, event_type TEXT, repo_id TEXT, rfilename TEXT, object_key TEXT, size INTEGER, status TEXT, remote_addr TEXT, user_agent TEXT);
CREATE INDEX IF NOT EXISTS idx_logs_user ON access_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_logs_repo ON access_logs(repo_id);
COMMIT;
PRAGMA foreign_keys=ON;
