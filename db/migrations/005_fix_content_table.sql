-- Ensure the content table has all required columns
ALTER TABLE public.press_release_content 
ADD COLUMN IF NOT EXISTS fetched_url text,
ADD COLUMN IF NOT EXISTS title text,
ADD COLUMN IF NOT EXISTS author text,
ADD COLUMN IF NOT EXISTS site_name text,
ADD COLUMN IF NOT EXISTS language text,
ADD COLUMN IF NOT EXISTS word_count int,
ADD COLUMN IF NOT EXISTS published_at timestamptz,
ADD COLUMN IF NOT EXISTS canonical_url text,
ADD COLUMN IF NOT EXISTS raw_extraction_json jsonb;