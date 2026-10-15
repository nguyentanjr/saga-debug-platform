"""Builds the real-project integration fixtures from the 2026-10-11 retest captures.

Input: tmp/project-retests (captured events and native database snapshots, local only).
Output: correlation-service/src/test/resources/integration/real-project-fixtures.json

Expected status comes from each source system's own persisted state (native) and the
plan written before fault injection -- never from what the observer reported. Event types
are re-derived with the current shipped mapping where the captured payload allows it, so
a declaration fix is exercised rather than frozen out.
"""
import json, os, sys, yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RETEST = os.path.join(ROOT, 'tmp', 'project-retests')
OUT = os.path.join(ROOT, 'correlation-service', 'src', 'test', 'resources', 'integration',
                   'real-project-fixtures.json')

def mapping(name):
    with open(os.path.join(ROOT, 'declarations', 'ingestion', name), encoding='utf-8') as f:
        return yaml.safe_load(f)['source']

KMS_REQUEST = mapping('kms-order-request.yaml')

def native(fixture):
    path = os.path.join(RETEST, fixture + '.json')
    if not os.path.exists(path):
        return None
    with open(path, encoding='utf-8') as f:
        return json.load(f).get('native')

def expected(system, fixture, nat):
    """(status, basis): the outcome the source system itself recorded."""
    if system == 'hotel':
        # Hotel snapshots carry no native row; outcome from the pre-run plan and the
        # source's own terminal event (ROOM_REJECTED / ROOM_CANCELLED / PAYMENT_ACCEPTED).
        if 'approved' in fixture: return 'COMPLETED', 'plan: approved, payment accepted'
        if any(k in fixture for k in ('room-missing', 'room-unavailable', 'hotel-crash')):
            return 'FAILED', 'plan: room rejected by hotel-service'
        return 'COMPENSATED', 'plan: payment rejected, source emitted ROOM_CANCELLED'
    if system == 'kms':
        s, p = nat['stock_status'], nat['payment_status']
        basis = f'native stock={s} payment={p}'
        if s == 'Success' and p == 'Success': return 'COMPLETED', basis
        if s == 'Failed': return 'FAILED', basis + ' (payment rollback, if any, is unobservable)'
        if s == 'Rollback': return 'COMPENSATING', basis + ' (stock rollback emits no ack)'
        return 'IN_PROGRESS', basis
    if system == 'spm':
        st = nat['status']
        basis = f'native order status={st}'
        if st == 'COMPLETED': return 'COMPLETED', basis
        if st == 'PENDING': return 'IN_PROGRESS', basis
        if 'stock-rejected' in fixture: return 'COMPENSATING', basis + ' (refund requested, no ack)'
        return 'FAILED', basis
    if system == 'eventuate':
        st = nat['orderState']
        return {'APPROVED': 'COMPLETED', 'REJECTED': 'FAILED', 'PENDING': 'IN_PROGRESS'}[st], \
            f'native orderState={st}'
    raise ValueError(system)

def event_type(system, event):
    if system == 'kms' and event['eventId'].startswith('t_order-'):
        payload = json.loads(event['originalPayload'])
        return KMS_REQUEST['eventTypes'][payload['stockStatus']]
    return event['eventType']

# Defects the source itself committed, established from its own state, which the observer
# should report. spm customer pause: after resume the source processed the reservation
# twice (customer balance 100 -> 98 and stock 5 -> 3 instead of 99 / 4,
# spm-pause-resource-check.json), so events after the run's ending are a true finding.
SOURCE_DEFECTS = {
    'spm-customer-pause-recovered': (['FORBIDDEN_POST_TERMINAL'],
        'source reprocessed after resume: balance 100->98, stock 5->3 (expected 99/4)'),
}

def source_defect(name):
    for prefix, defect in SOURCE_DEFECTS.items():
        if name.startswith(prefix):
            return defect
    return ([], None)

plans = json.load(open(os.path.join(RETEST, 'replay-plan.json'), encoding='utf-8'))
fixtures = []
for plan in plans:
    if plan['permutation'] != 0:
        continue
    name = plan['fixture']
    system = name.split('-')[0]
    nat = native(name.replace('-id-settled', '-id-settled'))
    status, basis = expected(system, name, nat)
    fixtures.append({
        'fixture': name, 'system': system, 'workflow': plan['workflow'],
        'expectedStatus': status, 'labelBasis': basis,
        'expectedFindings': source_defect(name)[0], 'defectBasis': source_defect(name)[1],
        'events': [{'eventId': e['eventId'], 'eventType': event_type(system, e),
                    'timestamp': e['timestamp'], 'serviceName': e['serviceName']}
                   for e in plan['events']],
    })

os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(fixtures, f, indent=1)
print(len(fixtures), 'fixtures ->', OUT)
