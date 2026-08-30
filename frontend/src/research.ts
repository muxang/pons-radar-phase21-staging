export type ResearchReport = Record<string, unknown>;
export interface AiResearchResponse {
  current: ResearchReport | null;
  history: ResearchReport[];
  job: ResearchReport | null;
  use_ai_research_in_signal: boolean;
}

export function aiResearchViewModel(value: AiResearchResponse) {
  return {
    current: value.current,
    history: value.history,
    job: value.job,
    chainSignalIndependent: value.use_ai_research_in_signal === false,
    empty: value.current === null,
  };
}
