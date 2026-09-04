# Saga Debug Platform

Nền tảng **quan sát và phân tích nguyên nhân gốc** cho giao dịch phân tán theo mô hình Saga
trong kiến trúc microservices.

Hệ thống lắng nghe sự kiện nghiệp vụ mà các service đã phát ra, tái dựng lại từng lần chạy
saga, đối chiếu với **đặc tả workflow khai báo**, phát hiện bất thường, rồi đưa ra ứng viên
nguyên nhân gốc **kèm bằng chứng kiểm chứng được** — không dùng điểm tin cậy dạng số.

Khoá luận tốt nghiệp — Trường Đại học Công nghệ, ĐHQGHN.

---

## Bài toán

Trong microservices, một nghiệp vụ như đặt hàng không còn nằm trong một giao dịch cơ sở dữ
liệu duy nhất. Nó trải qua nhiều service, mỗi service có DB riêng, giao tiếp bất đồng bộ.
Khi hỏng, có ba thứ khiến việc gỡ lỗi trở nên khó:

- **Không có call stack xuyên service.** Lỗi không ném exception lên trên như monolith — nó
  biểu hiện thành *sự kiện bị thiếu*, *sự kiện đến sai thứ tự*, hoặc *saga đứng im*.
- **Trạng thái nằm rải rác.** Không service nào biết "saga X đang ở đâu".
- **Nhiều lỗi là sự *vắng mặt* của sự kiện** — không có gì để bắt bằng cơ chế event-driven.

Distributed tracing cho biết request đi qua đâu, nhưng không nói *bước nào của saga đang sai
so với thiết kế*. Đó là khoảng trống nền tảng này lấp.

---

## Kiến trúc

```
┌──────────────┐   ┌────────────────┐   ┌──────────────────┐
│ order-service│──►│                │   │                  │
├──────────────┤   │     Kafka      │──►│ correlation-     │──► PostgreSQL
│payment-service│──►│   (8 topic)    │   │ service  ⭐ CORE │
├──────────────┤   │                │   │                  │──► Dashboard
│inventory-svc │──►│                │   └──────────────────┘    (Angular)
└──────────────┘   └────────────────┘            ▲
       │                                          │
       └────► OTel Collector ──► Jaeger      HTTP  │  POST /api/ingest/{source}
                                             (nguồn ngoài Kafka)
```

Ba service nghiệp vụ chỉ là **demo sinh dữ liệu saga thật**. Toàn bộ đóng góp nằm ở
`correlation-service`, và nó **quan sát thụ động** — không can thiệp vào luồng nghiệp vụ.

### Pipeline xử lý

```
Kafka ─┐
HTTP  ─┴► EventIngestionService ─► CorrelationService.processEvent
                                        ├─► 10 detector bất thường chạy inline
                                        └─► lưu sự kiện + suy trạng thái từ đặc tả

[theo lịch, 15s] 6 detector quét bất thường dạng "sự kiện không tới"
[theo lịch, 30s] RcaService: 18 luật → RcaSelector xếp hạng → lưu kết quả + cảnh báo
```

---

## Đóng góp chính

**1. Trạng thái saga suy từ đặc tả, không hard-code.**
`WorkflowSpec.statusAfter()` đọc transition đi ra của một bước để biết bước đó xong thì saga
kết thúc hay còn chạy. Thêm workflow mới = thêm một file YAML vào `workflows/`, không sửa Java.

**2. RCA dựa trên bằng chứng theo tầng, không dùng điểm số.**
Mỗi ứng viên nguyên nhân mang theo bằng chứng cụ thể: giá trị quan sát được, giá trị kỳ vọng,
nguồn của ngưỡng, và các sự kiện làm chứng. Bốn tầng — `STRONG` / `SUPPORTING` / `WEAK` /
`RULED_OUT`. Một con số như `confidence = 0.87` *trông* chính xác nhưng không giải thích được;
"đây là bằng chứng mạnh vì có chữ ký trực tiếp trong payload" thì kiểm chứng được.

**3. Xếp hạng theo vị trí trong luồng chạy.**
Khi nhiều luật cùng khớp, `RcaSelector` xếp theo: tầng bằng chứng → **vị trí bước bị neo trong
lần chạy này** → thời điểm → mức nghiêm trọng → mã luật. Tiêu chí thứ hai thay cho việc so
chuỗi mã luật, vốn khiến `"R07"` luôn thắng `"R10"` vì lý do thuần từ vựng.

**4. Phân biệt nhân quả quan sát được với nhân quả suy luận.**
`ExecutionProjection` gắn nhãn xuất xứ (`StartProvenance`) cho mọi phép đo thời gian, nên một
ranh giới *suy ra từ đặc tả* không bao giờ bị trình bày như một ranh giới *quan sát được*.

**5. Gắn vào hệ thống có sẵn mà không sửa code của hệ đó.**
`SourceMapping` khai báo trường dữ liệu của hệ nguồn nằm ở đâu; `SourceMappingInferrer`
**đề xuất** file đó từ một mẫu sự kiện, kèm bằng chứng cho từng trường suy ra.

---

## Chạy thử

**Yêu cầu:** Docker + Docker Compose, JDK 17, Maven, Node 16+.

**1. Backend + hạ tầng** (10 dịch vụ: 4 Spring Boot, PostgreSQL, Kafka, Kafka UI, Jaeger,
OTel Collector, Toxiproxy):

```bash
mvn -q -DskipTests package
docker compose up -d
```

**2. Dashboard** — chạy riêng, *không* nằm trong `docker-compose.yml`:

```bash
cd dashboard && npm install && npm start
```

| Giao diện | Địa chỉ | Nguồn |
|---|---|---|
| Dashboard | http://localhost:4200 | `npm start` (ngoài compose) |
| correlation-service | http://localhost:8084 | compose |
| order / payment / inventory | :8081 / :8082 / :8083 | compose |
| Jaeger | http://localhost:16686 | compose |
| Kafka UI | http://localhost:8088 | compose |
| Toxiproxy admin | http://localhost:8474 | compose |

Tạo một saga hỏng rồi xem chẩn đoán:

```bash
curl -X POST http://localhost:8081/api/orders -H "Content-Type: application/json" -d "{\"customerId\":\"demo\",\"productId\":\"PRODUCT-1\",\"quantity\":9999,\"amount\":999900}"
```

Đặt số lượng vượt tồn kho → thanh toán xong thì giữ hàng thất bại → hệ thống phải hoàn tiền.
Mở dashboard để xem dòng sự kiện, bất thường, và kết quả RCA.

---

## Kiểm thử

```bash
mvn -B test
```

**148 test, 0 failure.** Tập trung ở `correlation-service` (16 lớp test). Đáng chú ý, các test
chứng minh hai điểm mở rộng bằng fixture thật trong `src/test/resources/`:

- **Nhiều workflow**: ngoài `ORDER_SAGA`, test nạp thêm `FULFILLMENT_SAGA`, `PARALLEL_CHECKOUT`
  (nhánh song song / join) và `LOAN_APPROVAL` (outcome có tên: `APPROVED` / `MANUAL_REVIEW` /
  `REJECTED` định tuyến ba hướng khác nhau) — không sửa một dòng Java nào.
- **Quan sát hệ thống bên ngoài**: `ingestion/legacy-order-system.yaml` đọc một hệ nguồn dùng
  `id` / `type` / `occurredAt` / `context.orderNumber` và từ vựng riêng (`order.payment.captured`),
  hoàn toàn không biết đến khái niệm saga.

> Ba service demo (order / payment / inventory) **chưa có test**; chúng chỉ sinh dữ liệu.

## Đánh giá

```bash
python evaluate.py --list          # liệt kê kịch bản
python evaluate.py                 # smoke test
python evaluate.py --full          # chạy đầy đủ
python evaluate.py --full --split test
```

Bộ kịch bản: **28 kịch bản** (`SC01`–`SC29`, không có `SC18`), chia 5 nhóm
(A Service Availability = 5, B Network/Timing = 8, C State Machine = 5, D Compensation = 5,
E Correlation/Identity = 5), **15 train / 13 test**, **10 `real` / 18 `synthetic`**.

Kết quả lần chạy 2026-05-23 (`results/evaluation_report.json`) — **trên 10 kịch bản `real`,
27 lượt chạy, thuộc nhóm A/B/D**:

| Chỉ số | Giá trị |
|---|---|
| Độ chính xác RCA | 100.0 % |
| Time-to-Insight trung bình | 50.8 s |
| Độ trễ RCA trung bình / p95 | 32.1 ms / 80.6 ms |
| Precision / Recall / F1 | 1.0 / 1.0 / 1.0 |
| Lỗi runtime | 0 |

> **Đọc con số này cho đúng.** Nhóm C và E toàn bộ là `synthetic` nên **không nằm trong con số
> 100 %**. Bộ kịch bản nhỏ và xây trên một workflow đã biết trước. Và **chưa có baseline nào**
> để so sánh. Giá trị nằm ở chỗ chứng minh pipeline chạy ổn định và sinh được bằng chứng có
> cấu trúc, không nằm ở con số tuyệt đối.

---

## Hạn chế đã biết

| Hạn chế | Chi tiết |
|---|---|
| Luật RCA hard-code | 18 luật là phương thức Java trong `RcaService`. `resources/rules/rule.yaml` **không được nạp lúc chạy** — là bản phác thảo thiết kế |
| Chưa có source mapping ở production | Cơ chế **đã được chứng minh bằng test** (`ForeignSystemIngestionTest` + `test/resources/ingestion/legacy-order-system.yaml`), nhưng `main/resources/ingestion/` rỗng nên bản chạy thật luôn dùng identity mapping |
| Không có baseline | Khung đánh giá so với nhãn kỳ vọng, không so với phương pháp đối chứng |
| Danh mục kịch bản tự tham chiếu | Kịch bản được suy ra từ chính bộ luật đã cài, nên độ phủ nội tại gần như không thể thấp |
| REST API không xác thực | Kể cả `/api/ingest` và `/api/admin/inject-event` |
| Toxiproxy chưa nối dây | Kịch bản `SC07` được đánh dấu `synthetic` vì các service nối thẳng `kafka:9092` |
| Chạy cục bộ | Một máy Docker Compose; chưa kiểm ở quy mô lớn |

---

## Tài liệu

| Tài liệu | Nội dung |
|---|---|
| [`docs/GROUND_TRUTH.md`](docs/GROUND_TRUTH.md) | **Bản sự thật gốc** — số liệu và tên lớp đã đối chiếu với mã nguồn, kèm lệnh tự kiểm |
| [`docs/HIEU_SAU_PROJECT.md`](docs/HIEU_SAU_PROJECT.md) | Giải thích sâu cơ chế bên trong: từng tầng, lý do thiết kế, chỗ dễ bị hỏi vặn |
| [`docs/RCA_SCENARIO_TAXONOMY.md`](docs/RCA_SCENARIO_TAXONOMY.md) | Thiết kế bộ kịch bản đánh giá và phương pháp luận |
| [`docs/SCRIPT_THUYET_TRINH.md`](docs/SCRIPT_THUYET_TRINH.md) | Script thuyết trình bảo vệ + chuẩn bị Q&A |

> Khi mã nguồn đổi: sửa `GROUND_TRUTH.md` **trước**, rồi lan sang các tài liệu khác.

---

## Cấu trúc thư mục

```
saga-debug-platform/
├── saga-events/           # DTO sự kiện dùng chung (BaseEvent + causationId/spanId)
├── order-service/         # demo: tạo đơn + điều phối bù trừ
├── payment-service/       # demo: Resilience4j (retry + circuit breaker) + chaos fail-mode
├── inventory-service/     # demo: giữ hàng
├── correlation-service/   # ⭐ CORE
│   └── src/main/java/com/example/correlation/
│       ├── messaging/     # SagaEventConsumer (đăng ký topic theo regex)
│       ├── ingestion/     # cổng vào chung: RawObservation, SourceMapping(+Registry/Inferrer)
│       ├── execution/     # ExecutionProjection: dựng lại các bước đã chạy
│       ├── workflow/      # WorkflowRegistry + WorkflowSpec
│       ├── service/       # CorrelationService, AnomalyDetectionService, RcaService
│       │   └── rca/       # RcaSelector (tầng xếp hạng)
│       ├── domain/        # entity JPA + enum
│       ├── controller/    # REST API
│       └── resources/
│           ├── db/migration/   # 9 file Flyway V1..V9
│           └── workflows/      # đặc tả saga (YAML)
├── dashboard/             # Angular UI
├── evaluate.py            # khung đánh giá (28 kịch bản)
└── docs/
```
