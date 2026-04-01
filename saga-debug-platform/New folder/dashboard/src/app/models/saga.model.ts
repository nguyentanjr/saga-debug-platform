export interface SagaListItem {
  sagaId: string;
  workflowType: string;
  status: string;
  startTime: string;
  endTime: string | null;
  durationMs: number | null;
  eventCount: number;
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
}

export interface RcaResult {
  id: number;
  sagaId: string;
  rootCause: string;
  severity: string;
  evidence: string;
  suggestions: string;
  createdAt: string;
}

export interface SagaDetail {
  sagaId: string;
  workflowType: string;
  status: string;
  startTime: string;
  endTime: string | null;
  durationMs: number | null;
  events: SagaEvent[];
  rcaResults: RcaResult[];
}

export interface Page<T> {
  content: T[];
  totalElements: number;
  totalPages: number;
  size: number;
  number: number;
}
