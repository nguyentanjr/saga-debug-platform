import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription, interval } from 'rxjs';
import { SagaService } from '../../services/saga.service';
import { Evidence, RcaCandidate, RcaResult, SagaDetail } from '../../models/saga.model';

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

  isSelected(rca: RcaResult): boolean {
    return rca.rcaStatus === 'SELECTED' || (!rca.rcaStatus && !!rca.rootCause);
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
      candidate.status === 'SELECTED' || candidate.status === 'CONTRIBUTING');
  }

  getAlternativeCandidates(rca: RcaResult): RcaCandidate[] {
    return this.getCandidates(rca).filter(candidate =>
      candidate.status === 'RULED_OUT' || candidate.status === 'POSSIBLE_BUT_UNSUPPORTED');
  }

  hasActiveCandidates(rca: RcaResult): boolean {
    return this.getActiveCandidates(rca).length > 0;
  }

  hasAlternativeCandidates(rca: RcaResult): boolean {
    return this.getAlternativeCandidates(rca).length > 0;
  }

  getPrimaryCauses(rca: RcaResult): string[] {
    if (rca.primaryRootCauses && rca.primaryRootCauses.length > 0) {
      return rca.primaryRootCauses;
    }
    return rca.rootCause ? [rca.rootCause] : [];
  }

  getPrimaryCauseSeverity(rca: RcaResult, cause: string): string | null {
    const selected = this.getCandidates(rca).find(candidate =>
      candidate.rootCause === cause && candidate.status === 'SELECTED');
    const severity = selected?.severity || (this.isLegacyOnly(rca) ? rca.severity : null);
    return severity && severity !== 'NONE' ? severity : null;
  }

  getMissingEvidence(rca: RcaResult): string[] {
    return rca.missingEvidence || [];
  }

  getRcaStatusClass(rca: RcaResult): string {
    if (this.isSelected(rca)) return 'rca-status-selected';
    if (this.isInsufficient(rca)) return 'rca-status-insufficient';
    if (this.isHealthy(rca)) return 'rca-status-healthy';
    return 'rca-status-unknown';
  }

  getRcaStatusIcon(rca: RcaResult): string {
    if (this.isSelected(rca)) return 'check_circle';
    if (this.isInsufficient(rca)) return 'help_outline';
    if (this.isHealthy(rca)) return 'verified';
    return 'help';
  }

  getDisplayStatus(rca: RcaResult): string {
    if (this.isSelected(rca)) return 'Root Cause Identified';
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

  getCandidateReason(candidate: RcaCandidate): string | undefined {
    return candidate.statusReason;
  }

  getReasonLabel(status: string): string {
    switch (status) {
      case 'SELECTED': return 'Selection reason';
      case 'CONTRIBUTING': return 'Contribution reason';
      case 'RULED_OUT': return 'Ruled out because';
      case 'POSSIBLE_BUT_UNSUPPORTED': return 'Possible but unsupported';
      default: return 'Reason';
    }
  }

  getEvidence(candidate: RcaCandidate): Evidence[] {
    return candidate.evidence || [];
  }

  getCandidateSuggestions(candidate: RcaCandidate): string[] {
    return candidate.suggestions || [];
  }

  formatDirectness(directness: string): string {
    return this.formatEnumLabel(directness);
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
