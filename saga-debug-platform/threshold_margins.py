"""E4 — bien quyet dinh cua cac hang so nguong, offline tren bao cao da ghi.

Voi moi luot va moi hang so, tinh gia tri cua hang so tai do finding cua luot do bi LAT.
Neu moi luot giu nguyen ket luan trong mot khoang rong quanh gia tri dang dung, hang so do
khong duoc "chinh cho khop" du lieu; neu mot luot nam sat nguong, ket qua nhay voi no.

Chi tinh lai tu raw_events theo dung dieu kien trong code (AnomalyDetectionService /
RcaService), khong chay engine. Khong phai do live.

  delayed-compensation-multiplier (1.5): DELAYED_COMPENSATION khi
      gap(failed -> compensation started, dong ho nguon) > timeout(REFUND_PAYMENT)=10s * m
      -> lat tai m_c = gap / 10s; finding co mat khi m < m_c.
  missing-terminal-timeout-multiplier (10): MISSING_TERMINAL_EVENT khi run chua ket thuc im
      lang > maxStepTimeoutAcrossWorkflows * m. Base hien tai = 120s (F23: max tren MOI workflow
      da nap, keo tu kms-order-saga), base dung cua ORDER_SAGA = 10s.
      -> lat tai m_c = silence / base; finding co mat khi m < m_c.
  clock-skew-tolerance (500ms): OUT_OF_ORDER khi, theo thu tu nhan, mot event mang moc nguon
      som hon event nhan truoc no qua tolerance -> lat tai tol_c = do dao nguoc lon nhat.

Chay: python threshold_margins.py <bao cao json> [...]
"""
import json
import sys
from datetime import datetime

REFUND_TIMEOUT_S = 10
BASE_NOW_S = 120      # F23: gia tri code dang dung cho moi workflow
BASE_ORDER_SAGA_S = 10
CURRENT = {"delayed": 1.5, "missing_terminal": 10, "skew_ms": 500}


def ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def margins(run):
    ev = run["raw_events"]
    out = {}
    by_time = sorted(ev, key=lambda e: ts(e["timestamp"]))
    failed = next((e for e in by_time if e["eventType"].endswith("_FAILED")), None)
    comp = next((e for e in by_time if e["eventType"] == "REFUND_PAYMENT"), None)
    if failed and comp:
        gap = (ts(comp["timestamp"]) - ts(failed["timestamp"])).total_seconds()
        out["delayed_m_c"] = round(gap / REFUND_TIMEOUT_S, 3)
    if run.get("saga_status") in ("STARTED", "IN_PROGRESS") and ev:
        silence = (ts(run["scored_at"]) - max(ts(e["receivedAt"]) for e in ev)).total_seconds()
        out["silence_s"] = round(silence, 1)
        out["missing_terminal_m_c_now"] = round(silence / BASE_NOW_S, 2)
        out["missing_terminal_m_c_order_saga"] = round(silence / BASE_ORDER_SAGA_S, 2)
    by_recv = sorted(ev, key=lambda e: ts(e["receivedAt"]))
    inversion = 0.0
    for prev, cur in zip(by_recv, by_recv[1:]):
        inversion = max(inversion, (ts(prev["timestamp"]) - ts(cur["timestamp"])).total_seconds() * 1000)
    out["max_inversion_ms"] = round(inversion, 1)
    return out


def main():
    for path in sys.argv[1:]:
        runs = [r for r in json.load(open(path, encoding="utf-8"))["raw_runs"] if not r.get("error")]
        print(f"\n== {path} ({len(runs)} luot)")
        print(f"  {'luot':32} {'delayed m_c':>11} {'silence':>8} {'MT m_c(120s)':>12} {'MT m_c(10s)':>11} {'inv ms':>8}  anomalies")
        for r in runs:
            m = margins(r)
            print(f"  {r['scenario_id'][:28] + '#' + str(r['run_index']):32} {str(m.get('delayed_m_c', '-')):>11} "
                  f"{str(m.get('silence_s', '-')):>8} {str(m.get('missing_terminal_m_c_now', '-')):>12} "
                  f"{str(m.get('missing_terminal_m_c_order_saga', '-')):>11} {m['max_inversion_ms']:>8}  "
                  f"{','.join(sorted(r.get('anomalies_actual') or []))}")


if __name__ == "__main__":
    main()
