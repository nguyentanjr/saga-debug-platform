export const environment = {
  production: false,
  apiUrl: 'http://localhost:8084',

  // The demo scenario driver, which creates orders and toggles payment failure mode.
  // A separate service on purpose: the observer at apiUrl must not be able to command
  // the systems it observes. See docs/ADR/0001-observer-boundary.md.
  // A deployment without the demo stack leaves this unreachable and the scenario card
  // simply reports that it is unavailable.
  demoUrl: 'http://localhost:8085'
};
