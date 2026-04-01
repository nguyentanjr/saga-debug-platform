"""
Saga Debug Platform — Benchmark Script
Gửi N orders, đo latency, throughput, và thời gian correlation.
Output: bảng kết quả cho chương Thực nghiệm trong luận văn.

Yêu cầu: pip install requests tabulate
Chạy:    python benchmark.py --count 50
"""

import requests
import time
import argparse
import json
import statistics
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Config ───
ORDER_SERVICE_URL = "http://localhost:8081/api/orders"
CORRELATION_API_URL = "http://localhost:8084/api/sagas"
RCA_API_URL = "http://localhost:8084/rca"


def create_order(i):
    """Gửi 1 order, trả về (order_id, response_time_ms, status_code)."""
    payload = {
        "customerId": f"BENCH-{i:04d}",
        "productId": "PRODUCT-1",
        "quantity": 1,
        "amount": round(10 + i * 0.5, 2)
    }
    start = time.perf_counter()
    try:
        resp = requests.post(ORDER_SERVICE_URL, json=payload, timeout=30)
        elapsed = (time.perf_counter() - start) * 1000  # ms
        if resp.status_code == 200:
            data = resp.json()
            return {
                "index": i,
                "order_id": data.get("id"),
                "status_code": resp.status_code,
                "response_time_ms": round(elapsed, 2),
                "error": None
            }
        else:
            return {
                "index": i,
                "order_id": None,
                "status_code": resp.status_code,
                "response_time_ms": round(elapsed, 2),
                "error": f"HTTP {resp.status_code}: {resp.text[:200]}"
            }
    except requests.exceptions.ConnectionError:
        elapsed = (time.perf_counter() - start) * 1000
        return {
            "index": i,
            "order_id": None,
            "status_code": 0,
            "response_time_ms": round(elapsed, 2),
            "error": f"Connection refused — order-service not running at {ORDER_SERVICE_URL}?"
        }
    except Exception as e:
        elapsed = (time.perf_counter() - start) * 1000
        return {
            "index": i,
            "order_id": None,
            "status_code": 0,
            "response_time_ms": round(elapsed, 2),
            "error": str(e)
        }


def wait_for_sagas_completion(saga_ids, timeout=120, poll_interval=3):
    """Chờ tất cả saga hoàn thành (terminal status) hoặc timeout."""
    terminal_statuses = {"COMPLETED", "FAILED", "COMPENSATED"}
    start = time.time()
    completed = {}

    print(f"\n⏳ Chờ {len(saga_ids)} sagas hoàn thành (timeout={timeout}s)...")

    while time.time() - start < timeout:
        pending = [sid for sid in saga_ids if sid not in completed]
        if not pending:
            break

        for saga_id in pending:
            try:
                resp = requests.get(f"{CORRELATION_API_URL}/{saga_id}", timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("status") in terminal_statuses:
                        completed[saga_id] = data
            except:
                pass

        done = len(completed)
        total = len(saga_ids)
        print(f"   [{done}/{total}] sagas completed...", end="\r")

        if len(completed) == len(saga_ids):
            break
        time.sleep(poll_interval)

    print(f"\n   ✅ {len(completed)}/{len(saga_ids)} sagas reached terminal status")
    return completed


def get_saga_metrics(completed_sagas):
    """Tính metrics từ saga data."""
    durations = []
    event_counts = []
    statuses = {}

    for saga_id, data in completed_sagas.items():
        status = data.get("status", "UNKNOWN")
        statuses[status] = statuses.get(status, 0) + 1

        duration = data.get("durationMs")
        if duration is not None:
            durations.append(duration)

        events = data.get("events", [])
        event_counts.append(len(events))

    return durations, event_counts, statuses


def run_rca_benchmark(saga_ids):
    """Chạy RCA cho tất cả saga, đo thời gian."""
    rca_times = []
    rca_results = {}

    print(f"\n🔍 Chạy RCA cho {len(saga_ids)} sagas...")

    for saga_id in saga_ids:
        try:
            start = time.perf_counter()
            resp = requests.post(f"{RCA_API_URL}/analyze/{saga_id}", timeout=30)
            elapsed = (time.perf_counter() - start) * 1000
            rca_times.append(elapsed)

            if resp.status_code == 200:
                data = resp.json()
                root_cause = data.get("rootCause", "UNKNOWN")
                rca_results[root_cause] = rca_results.get(root_cause, 0) + 1
        except:
            pass

    return rca_times, rca_results


def percentile(data, p):
    """Tính percentile."""
    if not data:
        return 0
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * p / 100)
    idx = min(idx, len(sorted_data) - 1)
    return sorted_data[idx]


def print_results(order_results, durations, event_counts, statuses,
                  rca_times, rca_results, total_time):
    """In kết quả benchmark."""
    response_times = [r["response_time_ms"] for r in order_results if r["status_code"] == 200]
    errors = [r for r in order_results if r["status_code"] != 200]

    print("\n" + "=" * 70)
    print("📊 KẾT QUẢ BENCHMARK — SAGA DEBUG PLATFORM")
    print("=" * 70)
    print(f"   Thời gian chạy: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Tổng thời gian: {total_time:.1f}s")

    # ── 1. Order API Performance ──
    print("\n" + "─" * 40)
    print("1️⃣  ORDER API PERFORMANCE")
    print("─" * 40)
    print(f"   {'Metric':<35} {'Value':>15}")
    print(f"   {'─' * 50}")
    print(f"   {'Tổng requests gửi':<35} {len(order_results):>15}")
    print(f"   {'Thành công':<35} {len(response_times):>15}")
    print(f"   {'Lỗi':<35} {len(errors):>15}")
    if response_times:
        print(f"   {'Response Time - Min':<35} {min(response_times):>12.1f} ms")
        print(f"   {'Response Time - Avg':<35} {statistics.mean(response_times):>12.1f} ms")
        print(f"   {'Response Time - P50 (Median)':<35} {percentile(response_times, 50):>12.1f} ms")
        print(f"   {'Response Time - P95':<35} {percentile(response_times, 95):>12.1f} ms")
        print(f"   {'Response Time - P99':<35} {percentile(response_times, 99):>12.1f} ms")
        print(f"   {'Response Time - Max':<35} {max(response_times):>12.1f} ms")
        throughput = len(response_times) / (total_time if total_time > 0 else 1)
        print(f"   {'Throughput':<35} {throughput:>10.1f} req/s")

    # ── 2. Saga Execution Metrics ──
    print("\n" + "─" * 40)
    print("2️⃣  SAGA EXECUTION METRICS")
    print("─" * 40)
    print(f"   {'Metric':<35} {'Value':>15}")
    print(f"   {'─' * 50}")
    for status, count in sorted(statuses.items()):
        print(f"   Status: {status:<25} {count:>15}")
    if durations:
        print(f"   {'─' * 50}")
        print(f"   {'Saga Duration - Min':<35} {min(durations):>12.0f} ms")
        print(f"   {'Saga Duration - Avg':<35} {statistics.mean(durations):>12.0f} ms")
        print(f"   {'Saga Duration - P50':<35} {percentile(durations, 50):>12.0f} ms")
        print(f"   {'Saga Duration - P95':<35} {percentile(durations, 95):>12.0f} ms")
        print(f"   {'Saga Duration - Max':<35} {max(durations):>12.0f} ms")
    if event_counts:
        print(f"   {'Avg Events per Saga':<35} {statistics.mean(event_counts):>12.1f}")

    # ── 3. RCA Performance ──
    print("\n" + "─" * 40)
    print("3️⃣  RCA ANALYSIS PERFORMANCE")
    print("─" * 40)
    print(f"   {'Metric':<35} {'Value':>15}")
    print(f"   {'─' * 50}")
    if rca_times:
        print(f"   {'RCA Time - Min':<35} {min(rca_times):>12.1f} ms")
        print(f"   {'RCA Time - Avg':<35} {statistics.mean(rca_times):>12.1f} ms")
        print(f"   {'RCA Time - P95':<35} {percentile(rca_times, 95):>12.1f} ms")
        print(f"   {'RCA Time - Max':<35} {max(rca_times):>12.1f} ms")
    print(f"   {'─' * 50}")
    for rc, count in sorted(rca_results.items()):
        print(f"   RCA Result: {rc:<21} {count:>15}")

    # ── 4. So sánh với phương pháp thủ công ──
    print("\n" + "─" * 40)
    print("4️⃣  SO SÁNH: THỦ CÔNG vs. PLATFORM")
    print("─" * 40)
    avg_rca = statistics.mean(rca_times) if rca_times else 0
    manual_time_min = 30  # phút — ước lượng debug thủ công
    print(f"   {'Phương pháp':<25} {'Thời gian':>15} {'Đơn vị':>10}")
    print(f"   {'─' * 50}")
    print(f"   {'Debug thủ công (ước lượng)':<25} {manual_time_min:>15} {'phút':>10}")
    print(f"   {'Platform RCA (đo được)':<25} {avg_rca:>12.0f} {'ms':>13}")
    if avg_rca > 0:
        improvement = (manual_time_min * 60 * 1000) / avg_rca
        print(f"   {'Cải thiện':<25} {improvement:>12.0f}x {'nhanh hơn':>10}")

    print("\n" + "=" * 70)
    print("✅ Benchmark hoàn thành!")
    print("=" * 70)

    # ── Export CSV ──
    csv_file = "benchmark_results.csv"
    with open(csv_file, "w") as f:
        f.write("metric,value,unit\n")
        if response_times:
            f.write(f"order_api_avg_ms,{statistics.mean(response_times):.1f},ms\n")
            f.write(f"order_api_p50_ms,{percentile(response_times, 50):.1f},ms\n")
            f.write(f"order_api_p95_ms,{percentile(response_times, 95):.1f},ms\n")
            f.write(f"order_api_p99_ms,{percentile(response_times, 99):.1f},ms\n")
            f.write(f"throughput_rps,{len(response_times) / total_time:.2f},req/s\n")
        if durations:
            f.write(f"saga_duration_avg_ms,{statistics.mean(durations):.0f},ms\n")
            f.write(f"saga_duration_p50_ms,{percentile(durations, 50):.0f},ms\n")
            f.write(f"saga_duration_p95_ms,{percentile(durations, 95):.0f},ms\n")
        if rca_times:
            f.write(f"rca_avg_ms,{statistics.mean(rca_times):.1f},ms\n")
            f.write(f"rca_p95_ms,{percentile(rca_times, 95):.1f},ms\n")
        f.write(f"total_orders,{len(order_results)},count\n")
        f.write(f"success_count,{len(response_times)},count\n")
        f.write(f"error_count,{len(errors)},count\n")
        for status, count in statuses.items():
            f.write(f"saga_status_{status.lower()},{count},count\n")
        for rc, count in rca_results.items():
            f.write(f"rca_{rc.lower()},{count},count\n")

    print(f"\n📁 Kết quả đã export: {csv_file}")


def main():
    parser = argparse.ArgumentParser(description="Saga Debug Platform Benchmark")
    parser.add_argument("--count", type=int, default=50,
                        help="Số lượng orders gửi (default: 50)")
    parser.add_argument("--concurrent", type=int, default=1,
                        help="Số threads gửi đồng thời (default: 1 = sequential)")
    parser.add_argument("--wait-timeout", type=int, default=120,
                        help="Timeout chờ saga complete (seconds, default: 120)")
    parser.add_argument("--skip-rca", action="store_true",
                        help="Bỏ qua bước RCA benchmark")
    args = parser.parse_args()

    print("=" * 70)
    print("🚀 SAGA DEBUG PLATFORM — BENCHMARK")
    print("=" * 70)
    print(f"   Orders:     {args.count}")
    print(f"   Concurrent: {args.concurrent}")
    print(f"   Timeout:    {args.wait_timeout}s")
    print()

    # ── Health check ──
    print("🏥 Health check...")
    for name, url in [("Order Service", ORDER_SERVICE_URL),
                      ("Correlation API", CORRELATION_API_URL)]:
        try:
            r = requests.get(url.replace("/api/orders", "/actuator/health").replace("/api/sagas", "/actuator/health"), timeout=5)
            print(f"   {name}: {'✅ UP' if r.status_code == 200 else '⚠️  ' + str(r.status_code)}")
        except:
            try:
                # fallback: try the API directly
                r = requests.get(url + "?size=1", timeout=5)
                print(f"   {name}: {'✅ UP' if r.status_code in [200, 405] else '⚠️  ' + str(r.status_code)}")
            except Exception as e:
                print(f"   {name}: ❌ DOWN ({e.__class__.__name__})")
                print(f"   → URL: {url}")
    print()

    # ── Ghi nhớ thời điểm bắt đầu ──
    benchmark_start_iso = datetime.utcnow().isoformat() + "Z"

    # ── Phase 1: Gửi orders ──
    print(f"📤 Gửi {args.count} orders...")
    total_start = time.time()
    order_results = []

    if args.concurrent <= 1:
        # Sequential
        for i in range(1, args.count + 1):
            result = create_order(i)
            order_results.append(result)
            status = "✓" if result["status_code"] == 200 else "✗"
            print(f"   [{i}/{args.count}] {status} {result['response_time_ms']:.0f}ms", end="\r")
        print()
    else:
        # Concurrent
        with ThreadPoolExecutor(max_workers=args.concurrent) as executor:
            futures = {executor.submit(create_order, i): i
                       for i in range(1, args.count + 1)}
            for future in as_completed(futures):
                result = future.result()
                order_results.append(result)
                done = len(order_results)
                print(f"   [{done}/{args.count}] orders sent...", end="\r")
        print()

    send_time = time.time() - total_start
    order_results.sort(key=lambda r: r["index"])

    success_count = len([r for r in order_results if r["status_code"] == 200])
    error_samples = [r for r in order_results if r["status_code"] != 200][:3]
    print(f"   ✅ {success_count}/{args.count} orders gửi thành công ({send_time:.1f}s)")

    if error_samples:
        print(f"   ❌ Lỗi mẫu:")
        for e in error_samples:
            print(f"      [{e['index']}] {e['error']}")

    if success_count == 0:
        print("\n⛔ Không có order nào thành công! Kiểm tra:")
        print(f"   1. Order service đang chạy? (URL: {ORDER_SERVICE_URL})")
        print(f"   2. Docker containers đang up? (docker ps)")
        print(f"   3. Port 8081 đúng chưa?")
        return

    # ── Phase 2: Chờ sagas hoàn thành ──
    # Chỉ lấy saga IDs mới (tạo SAU khi benchmark bắt đầu)
    time.sleep(3)  # chờ event processing
    saga_ids = []
    try:
        resp = requests.get(f"{CORRELATION_API_URL}?size=200&startTime={benchmark_start_iso}&endTime=2099-01-01T00:00:00Z", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            content = data.get("content", [])
            saga_ids = [s["sagaId"] for s in content]
        # Fallback: nếu filter không work, lấy tất cả nhưng chỉ lấy đúng count
        if not saga_ids:
            resp = requests.get(f"{CORRELATION_API_URL}?size={args.count}", timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                content = data.get("content", [])
                saga_ids = [s["sagaId"] for s in content]
        print(f"\n📋 Tìm thấy {len(saga_ids)} sagas cho benchmark này")
    except Exception as e:
        print(f"⚠️  Không thể query correlation API: {e}")

    completed_sagas = {}
    if saga_ids:
        completed_sagas = wait_for_sagas_completion(saga_ids, timeout=args.wait_timeout)

    durations, event_counts, statuses = get_saga_metrics(completed_sagas)

    # ── Phase 3: RCA Benchmark ──
    rca_times = []
    rca_results = {}
    if not args.skip_rca and saga_ids:
        rca_times, rca_results = run_rca_benchmark(list(completed_sagas.keys())[:args.count])

    total_time = time.time() - total_start

    # ── Phase 4: Kết quả ──
    print_results(order_results, durations, event_counts, statuses,
                  rca_times, rca_results, total_time)


if __name__ == "__main__":
    main()
