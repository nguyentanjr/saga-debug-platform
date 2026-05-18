import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';
import {
  SagaListItem,
  SagaDetail,
  RcaResult,
  Page,
  DashboardSummary,
  ScenarioDefinition,
  ScenarioRunResponse
} from '../models/saga.model';

@Injectable({
  providedIn: 'root'
})
export class SagaService {

  private apiUrl = environment.apiUrl;

  constructor(private http: HttpClient) {}

  getSagas(page: number = 0, size: number = 20, status?: string): Observable<Page<SagaListItem>> {
    let params = new HttpParams()
      .set('page', page.toString())
      .set('size', size.toString());

    if (status) {
      params = params.set('status', status);
    }

    return this.http.get<Page<SagaListItem>>(`${this.apiUrl}/api/sagas`, { params });
  }

  getSagaDetail(sagaId: string): Observable<SagaDetail> {
    return this.http.get<SagaDetail>(`${this.apiUrl}/api/sagas/${sagaId}`);
  }

  triggerRca(sagaId: string): Observable<RcaResult> {
    return this.http.post<RcaResult>(`${this.apiUrl}/rca/analyze/${sagaId}`, {});
  }

  getRcaResults(sagaId: string): Observable<RcaResult[]> {
    return this.http.get<RcaResult[]>(`${this.apiUrl}/rca/results/${sagaId}`);
  }

  getDashboardSummary(windowHours: number = 24): Observable<DashboardSummary> {
    const params = new HttpParams().set('windowHours', windowHours.toString());
    return this.http.get<DashboardSummary>(`${this.apiUrl}/api/dashboard/summary`, { params });
  }

  getScenarios(): Observable<ScenarioDefinition[]> {
    return this.http.get<ScenarioDefinition[]>(`${this.apiUrl}/api/scenarios`);
  }

  runScenario(scenarioId: string): Observable<ScenarioRunResponse> {
    return this.http.post<ScenarioRunResponse>(`${this.apiUrl}/api/scenarios/${scenarioId}/run`, {});
  }
}
