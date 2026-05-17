export interface SagaListItem {
  sagaId: string;
  workflowType: string;
  status: string;
  startTime: string;
  endTime: string | null;
  durationMs: number | null;
  eventCount: number;
  rcaStatus?: RcaStatus;
  anomalyCount?: number;
}

export interface SagaEvent {
  id: number;
  eventId: string;
  sagaId: string;
  eventType: string;
  stepName: string;
  serviceName: string;
  timestamp: string;
  payload: string;
  receivedAt: string;
}

export interface Anomaly {
  id: number;
  sagaId: string;
  anomalyType: string;
  detectedAt: string;
  description: string;
  observedValue?: string;
  expectedValue?: string;
  thresholdSource?: string;
  evidenceEventIds?: string[];
  measuredAt?: string;
}

export type RcaStatus = 'SELECTED' | 'INSUFFICIENT_EVIDENCE' | 'NO_DIAGNOSIS_NEEDED';
export type CandidateStatus = 'SELECTED' | 'CONTRIBUTING' | 'RULED_OUT' | 'POSSIBLE_BUT_UNSUPPORTED';
export type Directness = 'DIRECT_SIGNATURE' | 'THRESHOLD_VIOLATION' | 'CORROBORATED_INFERENCE' | 'HEURISTIC_INFERENCE';
export type Specificity = 'LABEL_IS_SIGNATURE' | 'LABEL_NARROWS_SIGNATURE' | 'LABEL_INFERS_FROM_SIGNATURE' | 'LABEL_HEURISTIC';
export type EvidenceSource = 'WORKFLOW_SPEC' | 'EVENT_PAYLOAD' | 'STATE_MACHINE' | 'CONFIG' | 'CODE_CONSTANT';

export interface Evidence {
  fact: string;
  observedValue?: string;
  expectedValue?: string;
  source: EvidenceSource;
  anomalyId?: number;
  eventIds?: string[];
}

export interface RcaCandidate {
  ruleId: string;
  rootCause: string;
  severity: string;
  status: CandidateStatus;
  directness: Directness;
  specificity: Specificity;
  evidence: Evidence[];
  suggestions?: string[];
  selectionReason?: string;
  contributionReason?: string;
  ruledOutReason?: string;
  missingForStronger?: string;
}

export interface RcaResult {
  id: number;
  sagaId: string;
  rootCause: string | null;
  severity: string;
  evidence: string | null;
  suggestions: string | null;
  createdAt: string;
  rcaStatus?: RcaStatus;
  primaryRootCauses?: string[];
  candidates?: RcaCandidate[];
  missingEvidence?: string[];
}

export interface SagaDetail {
  sagaId: string;
  workflowType: string;
  status: string;
  startTime: string;
  endTime: string | null;
  durationMs: number | null;
  events: SagaEvent[];
  anomalies: Anomaly[];
  rcaResults: RcaResult[];
}

export interface Page<T> {
  content: T[];
  totalElements: number;
  totalPages: number;
  size: number;
  number: number;
}

export interface MetricItem {
  name: string;
  count: number;
}

export interface DashboardSummary {
  windowHours: number;
  since: string;
  totalSagas: number;
  completedSagas: number;
  failedSagas: number;
  compensatedSagas: number;
  activeSagas: number;
  totalAnomalies: number;
  rcaSelected: number;
  rcaInsufficientEvidence: number;
  rcaNoDiagnosisNeeded: number;
  topAnomalyTypes: MetricItem[];
  topRootCauses: MetricItem[];
  topAffectedSteps: MetricItem[];
}

export interface ScenarioDefinition {
  id: string;
  name: string;
  kind: string;
  description: string;
  expectedOutcome: string;
  expectedRca: string;
  openAfterSeconds: number;
}

export interface ScenarioRunResponse {
  scenarioId: string;
  name: string;
  sagaId: string;
  orderId: string;
  expectedOutcome: string;
  expectedRca: string;
  expectedAnomalies: string[];
  notes: string[];
}
