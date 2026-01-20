CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1. Enums for logic control
DROP TYPE IF EXISTS press_site_type CASCADE;
CREATE TYPE press_site_type AS ENUM ('static', 'dynamic', 'api');

DROP TYPE IF EXISTS press_item_status CASCADE;
CREATE TYPE press_item_status AS ENUM (
  'new',          -- Discovered, body not fetched yet
  'fetched_ok',   -- Body extracted successfully
  'thin_content', -- Extracted text was too short (likely error/redirect)
  'blocked',      -- WAF or 403 error
  'fetch_failed', -- Network timeout or 5xx
  'skipped'       -- Manually ignored
);

-- 2. Sources Registry
CREATE TABLE IF NOT EXISTS public.press_release_sources (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  org_name text NOT NULL,
  category text,                                   -- Insurer, Broker, Niche
  market_rank text,                                -- 'Top 5', 'Specialist', etc.
  list_url text NOT NULL UNIQUE,                   -- The URL we scan for new links
  site_type press_site_type NOT NULL DEFAULT 'static',
  
  -- JSON Blob for site-specific selectors. 
  -- Example: {"link_selector": "a.news-link", "date_selector": "span.date", "wait_for": ".results"}
  parser_hint jsonb DEFAULT '{}'::jsonb,

  is_enabled boolean NOT NULL DEFAULT true,
  last_checked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- 3. Discovered Items (The "List" View)
CREATE TABLE IF NOT EXISTS public.press_release_items (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id uuid NOT NULL REFERENCES public.press_release_sources(id) ON DELETE CASCADE,
  
  url text NOT NULL,                               -- The article detail URL
  title text,
  published_at timestamptz,
  
  -- Status Tracking
  content_status press_item_status NOT NULL DEFAULT 'new',
  attempt_count int NOT NULL DEFAULT 0,
  next_retry_at timestamptz NOT NULL DEFAULT now(),
  last_error text,
  
  discovered_at timestamptz NOT NULL DEFAULT now(),
  
  CONSTRAINT press_release_items_url_unique UNIQUE (url)
);	

-- Indexes for the worker queue
CREATE INDEX IF NOT EXISTS idx_press_items_queue 
ON public.press_release_items (content_status, next_retry_at) 
WHERE content_status IN ('new', 'fetch_failed', 'blocked');

-- 4. Extracted Content (The "Detail" View)
CREATE TABLE IF NOT EXISTS public.press_release_content (
  item_id uuid PRIMARY KEY REFERENCES public.press_release_items(id) ON DELETE CASCADE,
  
  content_text text,                               -- The cleaned body text
  content_html text,                               -- Optional: raw HTML of the body
  content_hash text,                               -- SHA256 for change detection
  
  extracted_at timestamptz NOT NULL DEFAULT now()
);