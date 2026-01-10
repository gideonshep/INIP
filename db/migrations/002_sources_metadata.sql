-- Add metadata fields to sources so RSS list can be stored & queried
ALTER TABLE sources
  ADD COLUMN IF NOT EXISTS domain TEXT,
  ADD COLUMN IF NOT EXISTS product_relevance TEXT,
  ADD COLUMN IF NOT EXISTS organisation TEXT,
  ADD COLUMN IF NOT EXISTS feed_name TEXT,
  ADD COLUMN IF NOT EXISTS geo_scope TEXT,
  ADD COLUMN IF NOT EXISTS priority TEXT,
  ADD COLUMN IF NOT EXISTS meta JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Make RSS/Atom sources upsertable (avoid duplicates)
-- NOTE: This assumes base_url is your feed URL field.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM   pg_constraint
    WHERE  conname = 'sources_source_type_base_url_key'
  ) THEN
    ALTER TABLE sources
      ADD CONSTRAINT sources_source_type_base_url_key UNIQUE (source_type, base_url);
  END IF;
END $$;
