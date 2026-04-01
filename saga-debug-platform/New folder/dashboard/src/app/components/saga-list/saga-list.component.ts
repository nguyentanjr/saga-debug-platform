import { Component, OnInit, ViewChild } from '@angular/core';
import { Router } from '@angular/router';
import { MatPaginator, PageEvent } from '@angular/material/paginator';
import { SagaService } from '../../services/saga.service';
import { SagaListItem } from '../../models/saga.model';

@Component({
  selector: 'app-saga-list',
  templateUrl: './saga-list.component.html',
  styleUrls: ['./saga-list.component.css']
})
export class SagaListComponent implements OnInit {

  displayedColumns: string[] = ['sagaId', 'status', 'workflowType', 'startTime', 'durationMs', 'eventCount'];
  sagas: SagaListItem[] = [];
  totalElements = 0;
  pageSize = 20;
  pageIndex = 0;
  selectedStatus = '';
  loading = false;
  errorMessage = '';

  statusOptions = ['', 'STARTED', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'COMPENSATING', 'COMPENSATED'];

  @ViewChild(MatPaginator) paginator!: MatPaginator;

  constructor(private sagaService: SagaService, private router: Router) {}

  ngOnInit(): void {
    this.loadSagas();
  }

  loadSagas(): void {
    this.loading = true;
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
