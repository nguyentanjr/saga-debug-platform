// Dashboard for the observer of the hotel system (semotpan/saga-orchestration) on port 18084.
// Requests stay on this origin and the dev server proxies them (proxy.hotel.json), so the
// observer's CORS setting does not have to change. No demo driver exists for that system.
export const environment = {
  production: false,
  apiUrl: '',
  demoUrl: ''
};
