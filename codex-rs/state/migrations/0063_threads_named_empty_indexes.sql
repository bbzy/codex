-- Keep sorted discovery indexed when a deliberately named session has no preview.
DROP INDEX idx_threads_visible_created_at_ms;
CREATE INDEX idx_threads_visible_created_at_ms
    ON threads(archived, created_at_ms DESC)
    WHERE preview <> '' OR length(trim(COALESCE(name, ''))) > 0;

DROP INDEX idx_threads_visible_updated_at_ms;
CREATE INDEX idx_threads_visible_updated_at_ms
    ON threads(archived, updated_at_ms DESC)
    WHERE preview <> '' OR length(trim(COALESCE(name, ''))) > 0;

DROP INDEX idx_threads_visible_recency_at_ms;
CREATE INDEX idx_threads_visible_recency_at_ms
    ON threads(archived, recency_at_ms DESC, id DESC)
    WHERE preview <> '' OR length(trim(COALESCE(name, ''))) > 0;
