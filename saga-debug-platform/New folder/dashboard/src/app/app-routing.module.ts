import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { SagaListComponent } from './components/saga-list/saga-list.component';
import { SagaDetailComponent } from './components/saga-detail/saga-detail.component';

const routes: Routes = [
  { path: '', redirectTo: '/sagas', pathMatch: 'full' },
  { path: 'sagas', component: SagaListComponent },
  { path: 'sagas/:sagaId', component: SagaDetailComponent }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
