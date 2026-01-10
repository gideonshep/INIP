ALTER TABLE sources
  ADD COLUMN IF NOT EXISTS last_checked_at timestamptz,
  ADD COLUMN IF NOT EXISTS last_ok_at timestamptz,
  ADD COLUMN IF NOT EXISTS last_status_code int,
  ADD COLUMN IF NOT EXISTS fail_count int NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_error text;
