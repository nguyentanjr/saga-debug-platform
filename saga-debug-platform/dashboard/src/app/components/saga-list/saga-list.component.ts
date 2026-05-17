import { Component, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { Router } from '@angular/router';
import { MatPaginator, PageEvent } from '@angular/material/paginator';
import { Subscription, interval } from 'rxjs';
import { SagaService } from '../../services/saga.service';
import { DashboardSummary, MetricItem, SagaListItem, ScenarioDefinition, ScenarioRunResponse } from '../../models/saga.model';

@Component({
  selector: 'app-saga-list',
  templateUrl: './saga-list.component.html',
  styleUrls: ['./saga-list.component.css']
})
export class SagaListComponent implements OnInit, OnDestroy {

  displayedColumns: string[] = ['sagaId', 'status', 'workflowType', 'startTime', 'durationMs', 'eventCount'];
  sagas: SagaListItem[] = [];
  totalElements = 0;
  pageSize = 20;
  pageIndex = 0;
  selectedStatus = '';
  loading = false;
  scenarioLoadingId = '';
  errorMessage = '';
  scenarioMessage = '';
  scenarioResult: ScenarioRunResponse | null = null;
  dashboardSummary: DashboardSummary | null = null;
  summaryWindowHours = 24;
  private pollSub?: Subscription;
  scenarios: ScenarioDefinition[] = [];

  statusOptions = ['', 'STARTED', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'COMPENSATING', 'COMPENSATED'];

  @ViewChild(MatPaginator) paginator!: MatPaginator;

  constructor(private sagaService: SagaService, private router: Router) {}

  ngOnInit(): void {
    this.loadSagas();
    this.loadDashboardSummary();
    this.loadScenarios();
    this.pollSub = interval(10000).subscribe(() => {
      this.loadSagas(false);
      this.loadDashboardSummary(false);
    });
  }

  ngOnDestroy(): void {
    this.pollSub?.unsubscribe();
  }

  loadSagas(showSpinner: boolean = true): void {
    if (showSpinner) {
      this.loading = true;
    }
    this.errorMessage = '';
    const status = this.selectedStatus || undefined;
    this.sagaService.getSagas(this.pageIndex, this.pageSize, status).subscribe({
      next: (page) => {
        this.sagas = page.content;
        this.totalElements = page.totalElements;
        this.loading = false;
      },
      error: (err) => {
        console.error('Failed to load sagas', err);
        this.errorMessage = err.status === 0
          ? 'Cannot connect to API server. Make sure correlation-service is running on port 8084.'
          : 'Failed to load sagas: ' + (err.error?.message || err.statusText);
        this.loading = false;
      }
    });
  }

  loadDashboardSummary(showError: boolean = true): void {
    this.sagaService.getDashboardSummary(this.summaryWindowHours).subscribe({
      next: (summary) => {
        this.dashboardSummary = summary;
      },
      error: (err) => {
        console.error('Failed to load dashboard summary', err);
        if (showError) {
          this.errorMessage = err.status === 0
            ? 'Cannot connect to dashboard summary API.'
            : 'Failed to load dashboard summary: ' + (err.error?.message || err.statusText);
        }
      }
    });
  }

  loadScenarios(): void {
    this.sagaService.getScenarios().subscribe({
      next: (scenarios) => {
        this.scenarios = scenarios;
      },
      error: (err) => {
        console.error('Failed to load scenarios', err);
        this.errorMessage = err.status === 0
          ? 'Cannot connect to scenario API on correlation-service.'
          : 'Failed to load scenarios: ' + (err.error?.error || err.error?.message || err.statusText);
      }
    });
  }

  runScenario(scenario: ScenarioDefinition): void {
    this.scenarioLoadingId = scenario.id;
    this.scenarioMessage = '';
    this.scenarioResult = null;
    this.errorMessage = '';

    this.sagaService.runScenario(scenario.id).subscribe({
      next: (result) => {
        this.scenarioLoadingId = '';
        this.scenarioResult = result;
        this.scenarioMessage = `${result.name} created saga ${result.sagaId}.`;
        this.loadSagas(false);
        this.loadDashboardSummary(false);
      },
      error: (err) => {
        console.error('Scenario failed', err);
        this.scenarioLoadingId = '';
        this.errorMessage = err.status === 0
          ? 'Cannot connect to scenario API on correlation-service.'
          : 'Scenario failed: ' + (err.error?.error || err.error?.message || err.statusText);
      }
    });
  }

  isScenarioRunning(scenario: ScenarioDefinition): boolean {
    return this.scenarioLoadingId === scenario.id;
  }

  onPageChange(event: PageEvent): void {
    this.pageIndex = event.pageIndex;
    this.pageSize = event.pageSize;
    this.loadSagas();
  }

  onFilterChange(): void {
    this.pageIndex = 0;
    this.loadSagas();
  }

  onRowClick(saga: SagaListItem): void {
    this.router.navigate(['/sagas', saga.sagaId]);
  }

  openSaga(sagaId: string): void {
    this.router.navigate(['/sagas', sagaId]);
  }

  completionRate(summary: DashboardSummary): number {
    return summary.totalSagas > 0
      ? Math.round((summary.completedSagas / summary.totalSagas) * 100)
      : 0;
  }

  hasMetricItems(items: MetricItem[] | undefined): boolean {
    return !!items && items.length > 0;
  }

  getStatusColor(status: string): string {
    switch (status) {
      case 'COMPLETED': return 'primary';
      case 'FAILED': return 'warn';
      case 'COMPENSATING': return 'accent';
      case 'COMPENSATED': return 'accent';
      case 'IN_PROGRESS': return 'primary';
      default: return '';
    }
  }
}
