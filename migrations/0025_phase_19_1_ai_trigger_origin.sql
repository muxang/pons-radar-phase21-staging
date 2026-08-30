ALTER TABLE ai_research_jobs
    ADD COLUMN trigger_origin text NOT NULL DEFAULT 'LEGACY_UNKNOWN',
    ADD COLUMN trigger_realtime_eligible boolean NOT NULL DEFAULT false;

ALTER TABLE ai_research_reports
    ADD COLUMN trigger_type text NOT NULL DEFAULT 'LEGACY_UNKNOWN',
    ADD COLUMN trigger_origin text NOT NULL DEFAULT 'LEGACY_UNKNOWN';

COMMENT ON COLUMN ai_research_jobs.trigger_origin IS
    'Origin of the evidence that scheduled research; only LIVE plus realtime eligibility may auto-call a provider.';
COMMENT ON COLUMN ai_research_reports.trigger_origin IS
    'Persisted scheduling provenance. ADMIN_MANUAL and historical origins are never presented as realtime research.';
