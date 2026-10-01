import { SagaDetailComponent } from './saga-detail.component';
import { RcaResult, SagaDetail, SagaEvent } from '../../models/saga.model';

function ev(id: number, eventId: string, eventType: string, stepName: string): SagaEvent {
  return { id, eventId, sagaId: 's', eventType, stepName, serviceName: 'svc', timestamp: '', payload: '{}', receivedAt: '' };
}

function saga(status: string, events: SagaEvent[], rca: RcaResult[], anomalies: any[] = []): SagaDetail {
  return { sagaId: 's', workflowType: 'W', status, startTime: '', endTime: null, durationMs: null,
           events, anomalies, rcaResults: rca };
}

const compensated = [
  ev(1, 'e1', 'ORDER_CREATED', 'CREATE'),
  ev(2, 'e2', 'PAID', 'PAY'),
  ev(3, 'e3', 'STOCK_FAILED', 'RESERVE'),
  ev(4, 'e4', 'REFUND', 'UNDO_PAY'),
  ev(5, 'e5', 'REFUNDED', 'UNDO_PAY')
];

function rcaAnchoredAt(step: string | null, createdAt = '2026-10-01T00:00:00Z'): RcaResult {
  return {
    id: 1, sagaId: 's', rootCause: 'FORWARD_STEP_FAILURE', severity: 'LOW', evidence: null, suggestions: null,
    createdAt, rcaStatus: 'EVIDENCE_AVAILABLE', primaryRootCauses: ['FORWARD_STEP_FAILURE'],
    candidates: [
      { ruleId: 'R99', rootCause: 'OTHER', severity: 'LOW', status: 'RULED_OUT', evidence: [], anchorStepId: 'CREATE' },
      { ruleId: 'R11', rootCause: 'FORWARD_STEP_FAILURE', severity: 'LOW', status: 'STRONG_EVIDENCE',
        anchorStepId: step, evidence: [{ fact: 'failed', eventIds: ['e3'] }, { fact: 'undone', eventIds: ['e5'] }] }
    ]
  };
}

describe('SagaDetailComponent diagnosis view', () => {
  let c: SagaDetailComponent;

  beforeEach(() => {
    c = new SagaDetailComponent({ snapshot: { paramMap: { get: () => 's' } } } as any, {} as any);
  });

  it('marks the anchored step as root cause and steps first seen after it as after-failure', () => {
    c.saga = saga('COMPENSATED', compensated, [rcaAnchoredAt('RESERVE')]);
    c.buildDiagnosis();

    expect(c.steps.map(s => `${s.name}:${s.role}`))
      .toEqual(['CREATE:forward', 'PAY:forward', 'RESERVE:rootCause', 'UNDO_PAY:rollback']);
    expect(c.steps[3].eventCount).toBe(2);
    expect(c.primaryCandidate?.ruleId).toBe('R11');
    expect(c.verdict()).toBe('Forward Step Failure at step RESERVE.');
  });

  it('highlights exactly the cited events and tells the root-cause event apart', () => {
    c.saga = saga('COMPENSATED', compensated, [rcaAnchoredAt('RESERVE')]);
    c.buildDiagnosis();

    expect(compensated.filter(e => c.isEvidenceEvent(e)).map(e => e.eventId)).toEqual(['e3', 'e5']);
    expect(compensated.filter(e => c.isRootCauseEvent(e)).map(e => e.eventId)).toEqual(['e3']);
  });

  it('does not invent a failed step when the rule names no anchor', () => {
    c.saga = saga('COMPENSATED', compensated, [rcaAnchoredAt(null)]);
    c.buildDiagnosis();

    expect(c.steps.every(s => s.role === 'forward')).toBeTrue();
    expect(c.verdict()).toBe('Forward Step Failure.');
  });

  it('uses the newest RCA when the saga was re-analysed', () => {
    const older = rcaAnchoredAt('PAY', '2026-10-01T00:00:00Z');
    const newer = rcaAnchoredAt('RESERVE', '2026-10-01T00:05:00Z');
    c.saga = saga('COMPENSATED', compensated, [older, newer]);
    c.buildDiagnosis();

    expect(c.steps.find(s => s.role === 'rootCause')?.name).toBe('RESERVE');
  });

  it('flags the step an anomaly cites, and stays modest on a completed saga with too little evidence', () => {
    const insufficient: RcaResult = { id: 2, sagaId: 's', rootCause: null, severity: 'NONE', evidence: null,
      suggestions: null, createdAt: '2026-10-01T00:00:00Z', rcaStatus: 'INSUFFICIENT_EVIDENCE', candidates: [] };
    c.saga = saga('COMPLETED', compensated.slice(0, 3), [insufficient],
      [{ id: 1, sagaId: 's', anomalyType: 'RETRY_BURST', detectedAt: '', description: '', evidenceEventIds: ['e2'] }]);
    c.buildDiagnosis();

    expect(c.steps.find(s => s.name === 'PAY')?.hasAnomaly).toBeTrue();
    expect(c.steps.find(s => s.name === 'CREATE')?.hasAnomaly).toBeFalse();
    expect(c.verdict()).toContain('Completed, but with 1 anomaly');
    expect(c.verdictClass()).toBe('verdict-unknown');
  });

  it('says nothing is diagnosed yet before any RCA exists', () => {
    c.saga = saga('IN_PROGRESS', compensated.slice(0, 2), []);
    c.buildDiagnosis();

    expect(c.verdictClass()).toBe('verdict-pending');
    expect(c.verdict()).toContain('Not analysed yet');
  });
});
