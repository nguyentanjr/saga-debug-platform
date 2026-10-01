import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription, interval } from 'rxjs';
import { SagaService } from '../../services/saga.service';
import { Evidence, RcaCandidate, RcaResult, SagaDetail, SagaEvent } from '../../models/saga.model';

/** Mid-tone colours that all keep a white icon legible. */
/** One step of the saga as it actually ran, in first-seen order. */
export interface StepView {
  name: string;
  /** forward = ran before the root-cause step; rootCause = the anchored step; rollback = first seen after it. */
  role: 'forward' | 'rootCause' | 'rollback';
  eventCount: number;
  hasAnomaly: boolean;
}

const SERVICE_PALETTE = ['#3f51b5', '#4caf50', '#ff9800', '#9c27b0', '#00897b', '#e91e63', '#5d4037', '#1e88e5'];

@Component({
  selector: 'app-saga-detail',
  templateUrl: './saga-detail.component.html',
  styleUrls: ['./saga-detail.component.css']
})
export class SagaDetailComponent implements OnInit, OnDestroy {

  sagaId = '';
  saga: SagaDetail | null = null;
  loading = false;
  rcaLoading = false;
  errorMessage = '';
  expandedPayload: number | null = null;
  traceIds: string[] = [];
  /** Diagnosis view, recomputed on every load. */
  latestRca: RcaResult | null = null;
  primaryCandidate: RcaCandidate | null = null;
  steps: StepView[] = [];
  evidenceEventIds = new Set<string>();
  highlightedEventId: string | null = null;
  private pollSub?: Subscription;

  constructor(
    private route: ActivatedRoute,
    private sagaService: SagaService
  ) {}

  ngOnInit(): void {
    this.sagaId = this.route.snapshot.paramMap.get('sagaId') || '';
    this.loadSaga();
    this.pollSub = interval(10000).subscribe(() => {
      if (this.shouldAutoRefresh()) {
        this.loadSaga(false);
      }
    });
  }

  ngOnDestroy(): void {
    this.pollSub?.unsubscribe();
  }

  loadSaga(showSpinner: boolean = true): void {
    if (showSpinner) {
      this.loading = true;
    }
    this.errorMessage = '';
    this.sagaService.getSagaDetail(this.sagaId).subscribe({
      next: (detail) => {
        this.saga = detail;
        this.loading = false;
        this.extractTraceIds();
        this.buildDiagnosis();
      },
      error: (err) => {
        console.error('Failed to load saga', err);
        this.errorMessage = 'Failed to load saga details. ' +
          (err.status === 0 ? 'API server unreachable.' : 'Error: ' + (err.error?.message || err.statusText));
        this.loading = false;
      }
    });
  }

  shouldAutoRefresh(): boolean {
    if (!this.saga || this.loading || this.rcaLoading) {
      return false;
    }
    return ['STARTED', 'IN_PROGRESS', 'COMPENSATING'].includes(this.saga.status);
  }

  triggerRca(): void {
    this.rcaLoading = true;
    this.errorMessage = '';
    this.sagaService.triggerRca(this.sagaId).subscribe({
      next: (result) => {
        this.rcaLoading = false;
        this.loadSaga(); // Reload to show new RCA result
      },
      error: (err) => {
        console.error('RCA failed', err);
        this.errorMessage = 'RCA analysis failed. ' +
          (err.status === 0 ? 'API server unreachable.' : 'Error: ' + (err.error?.message || err.statusText));
        this.rcaLoading = false;
      }
    });
  }

  togglePayload(index: number): void {
    this.expandedPayload = this.expandedPayload === index ? null : index;
  }

  formatPayload(payload: string): string {
    try {
      return JSON.stringify(JSON.parse(payload), null, 2);
    } catch {
      return payload;
    }
  }

  extractTraceIds(): void {
    if (!this.saga) return;
    const ids = new Set<string>();
    for (const event of this.saga.events) {
      try {
        const parsed = JSON.parse(event.payload);
        if (parsed.traceId) {
          ids.add(parsed.traceId);
        }
      } catch {}
    }
    this.traceIds = Array.from(ids);
  }

  /**
   * Derives the diagnosis view purely from what the observer already returned: the newest RCA,
   * its leading candidate, the step it is anchored to and the events cited as evidence. Step
   * roles are positional (before / at / after the anchored step), so no event or step vocabulary
   * of the observed system is assumed here.
   */
  buildDiagnosis(): void {
    const saga = this.saga;
    if (!saga) return;
    const results = [...saga.rcaResults].sort((a, b) => b.createdAt.localeCompare(a.createdAt));
    this.latestRca = results[0] || null;
    const candidates = this.latestRca ? this.getCandidates(this.latestRca) : [];
    const primary = this.latestRca?.primaryRootCauses || [];
    this.primaryCandidate =
      candidates.find(c => c.status === 'STRONG_EVIDENCE' && primary.includes(c.rootCause))
      || candidates.find(c => c.status === 'STRONG_EVIDENCE')
      || candidates.find(c => c.status === 'SUPPORTING_EVIDENCE')
      || null;

    const cited = new Set<string>();
    for (const e of this.primaryCandidate?.evidence || []) {
      (e.eventIds || []).forEach(id => cited.add(id));
    }
    for (const a of saga.anomalies) {
      (a.evidenceEventIds || []).forEach(id => cited.add(id));
    }
    this.evidenceEventIds = cited;

    const anomalous = new Set<string>();
    for (const a of saga.anomalies) {
      for (const id of a.evidenceEventIds || []) {
        const ev = saga.events.find(x => x.eventId === id);
        if (ev) anomalous.add(this.stepOf(ev));
      }
    }
    const anchor = this.primaryCandidate?.anchorStepId || null;
    const steps: StepView[] = [];
    let anchorSeen = false;
    for (const ev of saga.events) {
      const name = this.stepOf(ev);
      const existing = steps.find(s => s.name === name);
      if (existing) {
        existing.eventCount++;
        continue;
      }
      let role: StepView['role'] = anchorSeen ? 'rollback' : 'forward';
      if (anchor && name === anchor) {
        role = 'rootCause';
        anchorSeen = true;
      }
      steps.push({ name, role, eventCount: 1, hasAnomaly: anomalous.has(name) });
    }
    this.steps = steps;
  }

  stepOf(event: SagaEvent): string {
    return this.saga?.workflowSteps?.[event.id] || event.stepName || event.eventType;
  }

  isEvidenceEvent(event: SagaEvent): boolean {
    return !!event.eventId && this.evidenceEventIds.has(event.eventId);
  }

  isRootCauseEvent(event: SagaEvent): boolean {
    const anchor = this.primaryCandidate?.anchorStepId;
    return !!anchor && this.stepOf(event) === anchor && this.isEvidenceEvent(event);
  }

  /** Plain-language one-liner for the diagnosis banner. */
  verdict(): string {
    const saga = this.saga;
    if (!saga) return '';
    if (saga.workflowConflicts?.length) return 'Status undetermined: conflicting workflow declarations.';
    const rca = this.latestRca;
    if (!rca) return 'Not analysed yet. RCA runs automatically after the saga ends.';
    if (this.isHealthy(rca)) return 'Saga completed normally. Nothing to diagnose.';
    if (this.isInsufficient(rca)) {
      return saga.status === 'COMPLETED'
        ? `Completed, but with ${saga.anomalies.length} anomal${saga.anomalies.length === 1 ? 'y' : 'ies'}; evidence is not enough to name a cause.`
        : 'Something went wrong, but the evidence is not enough to name a cause.';
    }
    const c = this.primaryCandidate;
    if (!c) return rca.rootCause ? `Most likely cause: ${this.formatEnumLabel(rca.rootCause)}.` : '';
    const where = c.anchorStepId ? ` at step ${c.anchorStepId}` : '';
    return `${this.formatEnumLabel(c.rootCause)}${where}.`;
  }

  verdictClass(): string {
    const rca = this.latestRca;
    if (!rca) return 'verdict-pending';
    if (this.isHealthy(rca)) return 'verdict-ok';
    if (this.isInsufficient(rca)) return 'verdict-unknown';
    return 'verdict-problem';
  }

  stepRoleLabel(step: StepView): string {
    switch (step.role) {
      case 'rootCause': return 'root cause';
      case 'rollback': return 'after failure';
      default: return 'ok';
    }
  }

  eventLabel(eventId: string): string {
    const ev = this.saga?.events.find(e => e.eventId === eventId);
    return ev ? ev.eventType : eventId.slice(0, 8);
  }

  focusEvent(eventId: string): void {
    this.highlightedEventId = eventId;
    document.getElementById('ev-' + eventId)?.scrollIntoView({ behavior: 'smooth', block: 'center' });
    setTimeout(() => {
      if (this.highlightedEventId === eventId) this.highlightedEventId = null;
    }, 2000);
  }

  parseJson(jsonStr?: string | null): any[] {
    if (!jsonStr) {
      return [];
    }
    try {
      const parsed = JSON.parse(jsonStr);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  isJaegerLink(text: string): boolean {
    return !!text && text.includes('http://localhost:16686');
  }

  getTextBeforeLink(text: string): string {
    const idx = text.indexOf('http://');
    return idx > 0 ? text.substring(0, idx) : '';
  }

  extractLink(text: string): string {
    const match = text.match(/https?:\/\/[^\s,]+/);
    return match ? match[0] : '';
  }

  /**
   * A colour per service, derived from its name, so the timeline tells services apart for any
   * observed system. It used to be three CSS rules named after the demo's services; every other
   * system's events all shared one default colour.
   */
  serviceColor(serviceName?: string | null): string | null {
    if (!serviceName) return null;
    let hash = 0;
    for (let i = 0; i < serviceName.length; i++) {
      hash = (hash * 31 + serviceName.charCodeAt(i)) | 0;
    }
    return SERVICE_PALETTE[Math.abs(hash) % SERVICE_PALETTE.length];
  }

  getStatusClass(status: string): string {
    return 'status-' + status.toLowerCase();
  }

  getSeverityClass(severity: string): string {
    switch (severity) {
      case 'CRITICAL': return 'severity-critical';
      case 'HIGH': return 'severity-high';
      case 'MEDIUM': return 'severity-medium';
      case 'LOW': return 'severity-low';
      default: return 'severity-none';
    }
  }

  isEvidenceAvailable(rca: RcaResult): boolean {
    return rca.rcaStatus === 'EVIDENCE_AVAILABLE'
      || (!rca.rcaStatus && !!rca.rootCause);
  }

  isInsufficient(rca: RcaResult): boolean {
    return rca.rcaStatus === 'INSUFFICIENT_EVIDENCE';
  }

  isHealthy(rca: RcaResult): boolean {
    return rca.rcaStatus === 'NO_DIAGNOSIS_NEEDED';
  }

  isLegacyOnly(rca: RcaResult): boolean {
    return !rca.rcaStatus && !rca.candidates;
  }

  hasCandidates(rca: RcaResult): boolean {
    return this.getCandidates(rca).length > 0;
  }

  getCandidates(rca: RcaResult): RcaCandidate[] {
    return rca.candidates || [];
  }

  getActiveCandidates(rca: RcaResult): RcaCandidate[] {
    return this.getCandidates(rca).filter(candidate =>
      this.isEvidenceCandidate(candidate));
  }

  getStrongEvidenceCandidates(rca: RcaResult): RcaCandidate[] {
    return this.getCandidates(rca).filter(candidate =>
      candidate.status === 'STRONG_EVIDENCE');
  }

  getSupportingEvidenceCandidates(rca: RcaResult): RcaCandidate[] {
    return this.getCandidates(rca).filter(candidate =>
      candidate.status === 'SUPPORTING_EVIDENCE');
  }

  getWeakEvidenceCandidates(rca: RcaResult): RcaCandidate[] {
    return this.getCandidates(rca).filter(candidate =>
      candidate.status === 'WEAK_EVIDENCE');
  }

  getAlternativeCandidates(rca: RcaResult): RcaCandidate[] {
    return this.getCandidates(rca).filter(candidate =>
      candidate.status === 'RULED_OUT');
  }

  hasActiveCandidates(rca: RcaResult): boolean {
    return this.getActiveCandidates(rca).length > 0;
  }

  hasStrongEvidenceCandidates(rca: RcaResult): boolean {
    return this.getStrongEvidenceCandidates(rca).length > 0;
  }

  hasSupportingEvidenceCandidates(rca: RcaResult): boolean {
    return this.getSupportingEvidenceCandidates(rca).length > 0;
  }

  hasWeakEvidenceCandidates(rca: RcaResult): boolean {
    return this.getWeakEvidenceCandidates(rca).length > 0;
  }

  hasAlternativeCandidates(rca: RcaResult): boolean {
    return this.getAlternativeCandidates(rca).length > 0;
  }

  getMissingEvidence(rca: RcaResult): string[] {
    return rca.missingEvidence || [];
  }

  getRcaStatusClass(rca: RcaResult): string {
    if (this.isEvidenceAvailable(rca)) return 'rca-status-evidence';
    if (this.isInsufficient(rca)) return 'rca-status-insufficient';
    if (this.isHealthy(rca)) return 'rca-status-healthy';
    return 'rca-status-unknown';
  }

  getRcaStatusIcon(rca: RcaResult): string {
    if (this.isEvidenceAvailable(rca)) return 'fact_check';
    if (this.isInsufficient(rca)) return 'help_outline';
    if (this.isHealthy(rca)) return 'verified';
    return 'help';
  }

  getDisplayStatus(rca: RcaResult): string {
    if (this.isEvidenceAvailable(rca)) return this.isLegacyOnly(rca) ? 'Root Cause Identified' : 'RCA Evidence Summary';
    if (this.isInsufficient(rca)) return 'Insufficient Evidence';
    if (this.isHealthy(rca)) return 'No Diagnosis Needed';
    return 'Legacy Result';
  }

  getStatusDetail(rca: RcaResult): string {
    return '';
  }

  getCandidateClass(status: string): string {
    return 'candidate-' + status.toLowerCase();
  }

  isEvidenceCandidate(candidate: RcaCandidate): boolean {
    return candidate.status === 'STRONG_EVIDENCE'
      || candidate.status === 'SUPPORTING_EVIDENCE'
      || candidate.status === 'WEAK_EVIDENCE';
  }

  getCandidateReason(candidate: RcaCandidate): string | undefined {
    return candidate.statusReason;
  }

  getReasonLabel(status: string): string {
    switch (status) {
      case 'STRONG_EVIDENCE':
      case 'SUPPORTING_EVIDENCE':
        return 'Evidence basis';
      case 'WEAK_EVIDENCE': return 'Caution';
      case 'RULED_OUT': return 'Ruled out because';
      default: return 'Reason';
    }
  }

  getEvidence(candidate: RcaCandidate): Evidence[] {
    return candidate.evidence || [];
  }

  formatEnumLabel(value: string): string {
    return value.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
  }

  hasLegacyDescriptions(): boolean {
    if (!this.saga) {
      return false;
    }
    return this.saga.anomalies.some(a => !!a.description && (!a.observedValue || !a.expectedValue || !a.thresholdSource));
  }

  getSourceClass(source?: string): string {
    if (!source) {
      return 'source-unknown';
    }
    const normalized = source.toLowerCase();
    if (normalized.startsWith('workflow.yaml') || normalized.startsWith('workflow_spec')) {
      return 'source-workflow_spec';
    }
    if (normalized.startsWith('config') || normalized.includes('application.yml')) {
      return 'source-config';
    }
    if (normalized.startsWith('code') || normalized.includes('constant')) {
      return 'source-code_constant';
    }
    if (normalized.includes('payload') || normalized.includes('event')) {
      return 'source-event_payload';
    }
    if (normalized.includes('state')) {
      return 'source-state_machine';
    }
    return 'source-unknown';
  }

  getEventIcon(eventType: string): string {
    switch (eventType) {
      case 'ORDER_CREATED': return 'shopping_cart';
      case 'PAYMENT_COMPLETED': return 'payment';
      case 'PAYMENT_FAILED': return 'money_off';
      case 'INVENTORY_RESERVED': return 'inventory';
      case 'INVENTORY_FAILED': return 'remove_shopping_cart';
      case 'REFUND_PAYMENT': return 'undo';
      case 'PAYMENT_REFUNDED': return 'receipt_long';
      default: return 'event';
    }
  }
}
