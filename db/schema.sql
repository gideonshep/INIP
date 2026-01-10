-- INIP MVP schema (Postgres)
-- Run against database: inip

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS sources (
  id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name         TEXT NOT NULL,
  source_type  TEXT NOT NULL,
  base_url     TEXT,
  active       BOOLEAN NOT NULL DEFAULT TRUE,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_items (
  id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id      UUID REFERENCES sources(id),
  url            TEXT NOT NULL UNIQUE,
  title          TEXT,
  published_at   TIMESTAMPTZ,
  fetched_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw_json       JSONB,
  content_snip   TEXT,
  content_text   TEXT,
  content_hash   TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_items_published_at ON raw_items(published_at);
CREATE INDEX IF NOT EXISTS idx_raw_items_content_hash ON raw_items(content_hash);

CREATE TABLE IF NOT EXISTS signals (
  id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  raw_item_id        UUID NOT NULL REFERENCES raw_items(id) ON DELETE CASCADE,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  is_relevant        BOOLEAN NOT NULL,
  severity           SMALLINT,
  line_of_business   TEXT,
  peril              TEXT,
  geo                TEXT,
  signal_type        TEXT,
  rating_factors     JSONB,
  affected_entity    TEXT,
  key_risk_indicator TEXT,
  summary            TEXT,
  supporting_quote   TEXT,
  confidence         TEXT,
  model_name         TEXT,
  prompt_version     TEXT,
  CONSTRAINT severity_range CHECK (severity IS NULL OR (severity >= 1 AND severity <= 5))
);

CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at);
CREATE INDEX IF NOT EXISTS idx_signals_severity ON signals(severity);
CREATE INDEX IF NOT EXISTS idx_signals_peril ON signals(peril);
CREATE INDEX IF NOT EXISTS idx_signals_lob ON signals(line_of_business);
CREATE INDEX IF NOT EXISTS idx_signals_geo ON signals(geo);

CREATE TABLE IF NOT EXISTS alerts (
  id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  signal_id UUID NOT NULL REFERENCES signals(id) ON DELETE CASCADE,
  channel   TEXT NOT NULL,
  target    TEXT,
  sent_at   TIMESTAMPTZ,
  status    TEXT NOT NULL DEFAULT 'pending'
);

CREATE INDEX IF NOT EXISTS idx_alerts_sent_at ON alerts(sent_at);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);

CREATE TABLE IF NOT EXISTS feedback (
  id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  signal_id  UUID NOT NULL REFERENCES signals(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  user_id    TEXT,
  rating     SMALLINT,
  comment    TEXT
);
