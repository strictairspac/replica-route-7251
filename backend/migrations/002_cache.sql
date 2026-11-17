-- 002: query-key shared read cache policy on trials.
-- metadata.create_all creates the column on a fresh database; this file upgrades an existing one.
ALTER TABLE trials ADD COLUMN cache_ttl_ms INT NOT NULL DEFAULT 1000;

-- Old trials completed before this change keep their original result JSON and remain readable.
-- New results add a `cache` section with entries, invalidation messages and change evidence.
