CREATE TABLE ai_research_reports (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), token_id uuid NOT NULL REFERENCES tokens(id),
 provider text NOT NULL, model text NOT NULL,
 report_version integer NOT NULL CHECK(report_version>0), prompt_version integer NOT NULL CHECK(prompt_version>0),
 prompt_schema_version integer NOT NULL CHECK(prompt_schema_version>0), input_schema_version integer NOT NULL CHECK(input_schema_version>0), output_schema_version integer NOT NULL CHECK(output_schema_version>0),
 research_mode text NOT NULL CHECK(research_mode IN('CURRENT_RESEARCH','KNOWLEDGE_TIME')),
 knowledge_cutoff timestamptz NOT NULL, evidence_generated_at timestamptz NOT NULL,
 evidence_hash bytea NOT NULL CHECK(octet_length(evidence_hash)=32), input_hash bytea NOT NULL CHECK(octet_length(input_hash)=32),
 structured_report jsonb NOT NULL, category text NOT NULL, summary text NOT NULL CHECK(char_length(summary)<=4096),
 confidence integer NOT NULL CHECK(confidence BETWEEN 0 AND 100), usage_metadata jsonb,
 status text NOT NULL DEFAULT 'COMPLETED' CHECK(status IN('COMPLETED','SUPERSEDED')),
 created_at timestamptz NOT NULL DEFAULT now(), superseded_by uuid REFERENCES ai_research_reports(id),
 UNIQUE(token_id,research_mode,knowledge_cutoff,evidence_hash,prompt_version,model)
);
CREATE INDEX ai_reports_token_history ON ai_research_reports(token_id,created_at DESC,id DESC);

CREATE TABLE ai_research_jobs (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), token_id uuid NOT NULL REFERENCES tokens(id),
 research_mode text NOT NULL CHECK(research_mode IN('CURRENT_RESEARCH','KNOWLEDGE_TIME')),
 knowledge_cutoff timestamptz NOT NULL, trigger_type text NOT NULL CHECK(trigger_type IN('MANUAL','SIGNAL','SMART_CONSENSUS','EVIDENCE_REFRESH')),
 priority integer NOT NULL DEFAULT 0, status text NOT NULL DEFAULT 'PENDING' CHECK(status IN('PENDING','PROCESSING','RETRY','SUCCEEDED','FAILED','CACHED')),
 evidence_hash bytea CHECK(evidence_hash IS NULL OR octet_length(evidence_hash)=32), report_id uuid REFERENCES ai_research_reports(id),
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0), next_attempt_at timestamptz NOT NULL DEFAULT now(), locked_at timestamptz,
 last_error text CHECK(last_error IS NULL OR char_length(last_error)<=2048), requested_by uuid REFERENCES users(id),
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ai_jobs_due ON ai_research_jobs(priority DESC,next_attempt_at,id) WHERE status IN('PENDING','PROCESSING','RETRY');
CREATE UNIQUE INDEX ai_jobs_active_semantic ON ai_research_jobs(token_id,research_mode,knowledge_cutoff,trigger_type) WHERE status IN('PENDING','PROCESSING','RETRY');

COMMENT ON TABLE ai_research_reports IS 'Immutable AI interpretations of versioned deterministic evidence; never a chain fact or signal input.';
COMMENT ON COLUMN ai_research_reports.knowledge_cutoff IS 'No evidence known after this timestamp may enter the package.';
