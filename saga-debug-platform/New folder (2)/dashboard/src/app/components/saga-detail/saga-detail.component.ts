import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { SagaService } from '../../services/saga.service';
import { SagaDetail, RcaResult } from '../../models/saga.model';

@Component({
  selector: 'app-saga-detail',
  templateUrl: './saga-detail.component.html',
  styleUrls: ['./saga-detail.component.css']
})
export class SagaDetailComponent implements OnInit {

  sagaId = '';
  saga: SagaDetail | null = null;
  loading = false;
  rcaLoading = false;
  errorMessage = '';
  expandedPayload: number | null = null;
  traceIds: string[] = [];

  constructor(
    private route: ActivatedRoute,
    private sagaService: SagaService
  ) {}

  ngOnInit(): void {
    this.sagaId = this.route.snapshot.paramMap.get('sagaId') || '';
    this.loadSaga();
  }

  loadSaga(): void {
    this.loading = true;
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

  parseJson(jsonStr: string): any[] {
    try {
      return JSON.parse(jsonStr);
    } catch {
      return [];
    }
  }

  isJaegerLink(text: string): boolean {
    return text.includes('http://localhost:16686');
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
