import { SagaDetailComponent } from './saga-detail.component';
import { RcaCandidate, RcaResult, SagaDetail, SagaEvent } from '../../models/saga.model';

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
    id: 1, sagaId: 's', rootCause: 'FORWARD_STEP_FAILURE', ruleId: 'R11', severity: 'LOW', evidence: null, suggestions: null,
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

  it('highlights exactly the cited events, as evidence rather than as the cause', () => {
    c.saga = saga('COMPENSATED', compensated, [rcaAnchoredAt('RESERVE')]);
    c.buildDiagnosis();

    expect(compensated.filter(e => c.isEvidenceEvent(e)).map(e => e.eventId)).toEqual(['e3', 'e5']);
    expect(c.citedEventIds()).toEqual(['e3', 'e5']);
  });

  it('labels the anchored step as affected and leaves the others unlabelled', () => {
    c.saga = saga('COMPENSATED', compensated, [rcaAnchoredAt('RESERVE')]);
    c.buildDiagnosis();

    expect(c.steps.map(s => c.stepRoleLabel(s))).toEqual(['', '', 'affected step', 'after failure']);
  });

  it('drops step anomaly markers once the diagnosis names a step', () => {
    const anomalies = [{ id: 1, sagaId: 's', anomalyType: 'STEP_TIMEOUT', detectedAt: '', description: '', evidenceEventIds: ['e2', 'e3'] }];
    c.saga = saga('COMPENSATED', compensated, [rcaAnchoredAt('RESERVE')], anomalies);
    c.buildDiagnosis();

    expect(c.steps.find(s => s.name === 'PAY')?.hasAnomaly).toBeTrue();
    expect(c.steps.some(s => c.showStepAnomaly(s))).toBeFalse();
  });

  it('labels a long wait between timeline rows and shows one date for a single day', () => {
    const at = (sec: number) => new Date(Date.UTC(2026, 9, 1, 10, 0, sec)).toISOString();
    const events = [ev(1, 'e1', 'A', 'S1'), ev(2, 'e2', 'B', 'S2'), ev(3, 'e3', 'C', 'S3')];
    events[0].receivedAt = at(0); events[1].receivedAt = at(1); events[2].receivedAt = at(45);
    c.saga = saga('COMPLETED', events, []);
    c.buildTimeline();

    expect([0, 1, 2].map(k => c.gapLabel(k))).toEqual([null, null, '44 s later']);
    expect(c.timelineDay).not.toBeNull();
    expect(c.timeFormat).toBe('mediumTime');
  });

  it('summarises an anomaly by what was seen, not how it was measured', () => {
    expect(c.anomalySummary({ observedValue: 'RESERVE took 44090ms over 1 attempt(s); start=OBSERVED_CAUSAL_LINK', expectedValue: '<= 5000ms' }))
      .toBe('RESERVE took 44090ms over 1 attempt(s) (expected <= 5000ms)');
  });

  it('states a reason all candidates of a group share once, and keeps distinct ones per candidate', () => {
    const same = (r: string) => ({ ruleId: r, rootCause: r, severity: 'LOW', status: 'SUPPORTING_EVIDENCE', evidence: [], statusReason: 'Consequence of R25' }) as RcaCandidate;
    expect(c.sharedReason([same('A'), same('B')])).toBe('Consequence of R25');
    expect(c.sharedReason([same('A'), { ...same('B'), statusReason: 'other' }])).toBeNull();
    expect(c.sharedReason([same('A')])).toBeNull();
  });

  it('leaves the trace link out of suggestions, since the header offers it', () => {
    const rca = rcaAnchoredAt('RESERVE');
    rca.suggestions = JSON.stringify(['Check logs', 'View traces: http://localhost:16686/trace/abc']);
    expect(c.suggestions(rca)).toEqual(['Check logs']);
  });

  it('does not invent a failed step when the rule names no anchor', () => {
    c.saga = saga('COMPENSATED', compensated, [rcaAnchoredAt(null)]);
    c.buildDiagnosis();

    expect(c.steps.every(s => s.role === 'forward')).toBeTrue();
    expect(c.verdict()).toBe('Forward Step Failure.');
  });

  it('follows the candidate RCA promoted, not another strong one with the same cause', () => {
    const rca = rcaAnchoredAt('RESERVE');
    rca.candidates!.unshift({ ruleId: 'R30', rootCause: 'FORWARD_STEP_FAILURE', severity: 'LOW',
      status: 'STRONG_EVIDENCE', evidence: [], anchorStepId: 'PAY' });
    c.saga = saga('COMPENSATED', compensated, [rca]);
    c.buildDiagnosis();

    expect(c.primaryCandidate?.ruleId).toBe('R11');
    expect(c.steps.find(s => s.role === 'rootCause')?.name).toBe('RESERVE');
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

  it('puts runtime reports between the events they happened between', () => {
    const at = (sec: number) => new Date(Date.UTC(2026, 9, 1, 10, 0, sec)).toISOString();
    const events = [ev(1, 'e1', 'ORDER_CREATED', 'CREATE'), ev(2, 'e2', 'PAID', 'PAY'), ev(3, 'e3', 'RESERVED', 'RESERVE')];
    events[0].receivedAt = at(0); events[1].receivedAt = at(1); events[2].receivedAt = at(50);
    c.saga = { ...saga('COMPLETED', events, []), lifecycle: [
      { service: 'inventory-service', state: 'UP', occurredAt: at(48), source: 'docker', basis: 'PROCESS', detail: 'start' },
      { service: 'inventory-service', state: 'DOWN', occurredAt: at(2), source: 'docker', basis: 'PROCESS', detail: 'die exitCode=1' }
    ] };
    c.buildTimeline();

    expect(c.timeline.map(i => i.kind === 'event' ? i.event.eventId : i.entry.state))
      .toEqual(['e1', 'e2', 'DOWN', 'UP', 'e3']);
  });

  it('says a probe saw the service unreachable, not that it exited', () => {
    expect(c.lifecycleLabel({ service: 'billing', state: 'DOWN', occurredAt: '', source: 'alertmanager', basis: 'PROBE' }))
      .toBe('billing stopped answering probes');
    expect(c.lifecycleLabel({ service: 'billing', state: 'DOWN', occurredAt: '', source: 'docker', basis: 'PROCESS' }))
      .toBe('billing exited');
  });
});
