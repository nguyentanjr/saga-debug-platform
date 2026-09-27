# Trạng thái KLTN — correlation-service

> Chốt ngày **2026-09-07**. Số liệu lấy từ `results/evaluation_report.json`
> (lần chạy `2026-09-07T14:39:22Z`, build hiện tại đã có F1, F2, R12, R14).
> Chi tiết, ranh giới dataset và cách chạy lại: `docs/GROUND_TRUTH.md` §10 và §12.
>
> Tài liệu này là bản tóm tắt cho luận văn. Khi số đổi, sửa `GROUND_TRUTH.md` trước.
>
> **Số bị thay, không trích dẫn nữa:** lần chạy 2026-05-23 (RCA 100 %) — *superseded,
> không tái lập được*; lần chạy 2026-09-06 (lenient 85.2 % / **strict 74.1 %**) —
> *superseded, pre-F1*, phần strict **không so sánh được** với strict hôm nay (§5).

---

## 1. Ba đóng góp chính

### A. Tương quan dựa trên bằng chứng, có namespace theo nguồn

Danh tính execution là **cặp** `(source, rawId)`, không phải chuỗi id thô. Hai hệ được
quan sát là hai không gian id: `ORD-A` của hệ A và `ORD-A` của hệ B là hai lần chạy khác
nhau tình cờ trùng chuỗi.

Việc xếp một quan sát vào một execution đi theo thang bằng chứng, dừng ở bậc đầu tiên áp
dụng được, **không bậc nào là đoán**:

```
DECLARED_KEY        → id hệ nguồn tự khai và có trong observation
PROPAGATED_CONTEXT  → trace context được propagate (OTel agent, không cần sửa code hệ nguồn)
ALIAS_LINK          → một event mang cả hai định danh, có linked_by_event_id trỏ tới nó
(không đủ)          → NOT_CORRELATABLE, từ chối
```

Không gộp theo business key trùng, theo event type, hay theo cửa sổ thời gian. Mỗi
observation lưu kèm `CorrelationBasis` — trên cơ sở nào nó được xếp vào đó.

*ADR 0002, ADR 0003 · `ExecutionKey`, `ExecutionAliasResolver`, `EventIngestionService`*

### B. Dựng lại workflow độc lập thứ tự đọc

Observer gặp hai thứ tự khác nhau và không được lẫn: **processing order** (thứ tự platform
đọc event — thuộc tính của topic, partition, thread, retry) và **business order** (thứ tự
công việc đã xảy ra — thuộc tính của hệ được quan sát). Chỉ cái thứ hai nói gì đó về lần
chạy.

Trạng thái được dựng lại từ **toàn bộ** tập bằng chứng mỗi lần, không cộng dồn từng event
theo thứ tự đến. Thứ tự do event time quyết định; khi đồng hồ không tách được hai quan sát
thì đồ thị workflow quyết định; khi cả hai đều im lặng thì `EvidenceTimeline` **không
chọn** — nó báo `INDETERMINATE_ORDER` và không tuyên bố kết cục.

Bất biến: *cùng tập evidence + thứ tự giao khác nhau → cùng kết luận*. Ngoại lệ duy nhất
là `OUT_OF_ORDER`, vì chủ đề của chính finding đó là hai thứ tự lệch nhau.

*Invariant I9 · `EvidenceTimeline`*

### C. Phân biệt observed fact với inferred state, và thu hồi được phát biểu

`AnomalyType` tự khai **hình dạng phát biểu** của nó:

- `OCCURRENCE` — điều đã xảy ra. Quan sát thêm không xoá được.
- `ABSENCE` — điều được kỳ vọng nhưng không thấy. Quan sát thêm có thể bác bỏ.

Khi hai execution được gộp, mọi absence claim của cả hai nửa bị **thu hồi** (`retracted_at`
+ `retraction_reason`) chứ không bị xoá — bằng chứng vẫn còn, và detector theo lịch raise
lại nếu vẫn đúng với toàn bộ execution. RCA chỉ đọc `findStandingBySagaId`, nên không suy
luận từ finding đã bị thu hồi.

Đi kèm: `UNKNOWN ≠ INVALID` trong phán xét transition. INVALID chỉ dành cho "đồ thị đã nói
outcome này đi đâu và target không nằm ở đó"; target vẫn reachable mà chưa quan sát được
là **thiếu quan sát của observer**, không phải lỗi của hệ nguồn.

*ADR 0006, Invariant I10 · `AnomalyType.Claim`, `WorkflowSpec.explainTransition`*

### D. Đánh giá đóng góp — trạng thái đã kiểm chứng (chốt 2026-09-07)

| Đóng góp | Trạng thái | Bằng chứng, và ranh giới của nó |
|---|---|---|
| **Tương quan dựa trên bằng chứng** | **HOLDS** | 27/27 lượt chính thức có `execution_id` dạng `source::rawId`; 0 `UNRESOLVED`; thang bằng chứng từ chối khi không đủ. **Chỉ báo cáo coverage** — không có precision/recall/F1 vì dataset không có negative correlation case |
| **Dựng lại workflow độc lập thứ tự đọc** | **HOLDS** | Test property **120 hoán vị** cho cùng một status/startTime/endTime, **cộng** so sánh live vs backlog trên `kafka-microservices-with-saga`: 11/11 execution cho cùng trạng thái ở cả hai chế độ nạp. ⚠️ **Không được nói 27 lượt chính thức tự chúng chứng minh điều này** — chúng đều là live ingestion, không có hoán vị thứ tự nào |
| **Phát hiện bất thường có ý thức về bằng chứng** | **PARTIALLY HOLDS** | Lượt settle 2026-09-21: anomaly **P/R/F1 = 0.852 / 0.852 / 0.852**, 4 "false positive" và 4 false negative. **Bốn FP đó đã kiểm từng cái và không phải FP** — xem L-24 — nhưng **không được báo cáo precision = 1.000 nữa**: con số cũ đo khi harness đọc trước lúc sweep của detector kịp chạy. **FP-2 đã sửa** (L-11), **FP-3 đã sửa** (L-12), **L-19** `FORBIDDEN_POST_TERMINAL` phụ thuộc thứ tự đến vẫn còn. **Không được nói 'chỉ còn một vấn đề'** — mỗi lần nói thế đều có cái tiếp theo xuất hiện |
| **RCA có ý thức về bằng chứng** | **PARTIALLY HOLDS** | F1, F2, R12, R14, FP-2 và FP-3 đã dựng đúng ngữ nghĩa dự định. Nhưng audit 2026-09-20 chỉ ra hai chỗ **kết luận vượt quá bằng chứng vẫn được đề cử làm primary**: **L-20** R01 gán `STRONG` cho `CIRCUIT_BREAKER_OPEN` từ một chuỗi con trong log (comment ngay cạnh thừa nhận chưa quan sát breaker state), và **L-21** R06 gán `STRONG` cho `TRANSIENT_SERVICE_FAILURE` trên bằng chứng không phân biệt được 'service tự hồi phục' với 'observer nhận muộn'. Cả hai đều đi thẳng vào promotion vì chỉ `STRONG` mới được đề cử |
| **Gắn hệ mới không cần sửa hệ nguồn** | **HOLDS** | 3 repo Saga công khai được gắn vào chỉ bằng YAML/cấu hình, **0 dòng sửa** trong cả ba: `semotpan/saga-orchestration`, `sylleryum/kafka-microservices-with-saga`, `uuhnaut69/saga-pattern-microservices`. Một deployment nạp đồng thời 13 source mapping và 5 workflow spec (14 mapping từ 2026-09-20, thêm `second-estate-orders` — ví dụ mẫu cho N3, gắn vào cluster nên không khớp gì trong bộ chính thức) |

---

## 2. Trạng thái correctness

| Invariant | Bằng chứng | Trạng thái |
|---|---|---|
| I1 Observer boundary | Không có `RestTemplate`/`WebClient`/`KafkaTemplate` trong module; `ScenarioService` đã tách sang `demo-scenarios` | VERIFIED |
| I2 Correlation có bằng chứng | Thang trong `EventIngestionService.correlate`, `NOT_CORRELATABLE` khi thiếu | VERIFIED |
| I3 Inference có nhãn xuất xứ | `CorrelationBasis` lưu thành cột, `StartProvenance`, `retraction_reason` | VERIFIED |
| I4 Identity có namespace | `ExecutionKey`, CHECK trong V13, live `default::<uuid>` | VERIFIED |
| I5 Retry idempotent | `markProcessed` sau commit + UNIQUE trong DB | VERIFIED |
| I6 Local ACID | `TransactionTemplate` tường minh (resolve → merge → persist) | TEST-VERIFIED / LIVE-UNVERIFIED |
| I7 Hệ nguồn không phải tuân convention observer | `SourceMapping` đọc payload/key/header; một mapping chạy trên cả Kafka và HTTP | VERIFIED |
| I8 Phân giải không đoán | Thang trong `WorkflowRegistry`; nhập nhằng → `UNRESOLVED` + WARN; `ExecutionLookup` trả 409 | VERIFIED |
| I9 Processing order không lọt vào kết luận | Test 120 hoán vị; **nhưng** 3 detector theo lịch còn trộn clock (xem §6) | PARTIALLY VERIFIED |
| I10 UNKNOWN vs INVALID | `explainTransition` + 22 test | VERIFIED |
| Anomaly retraction / provenance | Đủ trong scope ADR 0006 (merge + late workflow resolution); ngoài scope thì không (xem §6) | PARTIALLY VERIFIED |

**Test suite: 457 test / 43 lớp, 0 failure, 0 error, 0 skipped** (`mvn clean install`,
build 2026-09-20). Tăng từ 343 nhờ các đợt R12 (16 test), F1 (13), F2 (12),
`PromotionAttributionTest` (13, chốt ngữ nghĩa `ClaimSubject` của R14) và
`RegressionEvidenceCompletenessTest` (chốt điều kiện terminal của FP-2) N1 Scheduler HA
(`SweepLeaseTest` 10, `ScheduledSweepsTest` 7) N3 Multi-Kafka
(`ClusterScopedMappingTest` 9, `MultiClusterIngestionTest` 4) và FP-3
(`CompensationAckDeclarationTest` 6).

Bằng chứng đáng chú ý: `OutOfOrderStatusTest.everyDeliveryOrderOfOneRunProducesTheSameReading`
chạy **cả 120 hoán vị** của một lần chạy 5 event và đòi mọi hoán vị cho cùng một
status/startTime/endTime.

---

## 3. Trạng thái experimental verification

| | |
|---|---|
| Thời điểm | **2026-09-07T14:39:22Z** |
| Build | Lượt báo cáo: 411 test (F1, F2, R12, R14). **Đo lại 2026-09-20 trên build sau FP-2** (`90e55e1`, 421 test): **trùng khớp từng chỉ số** — xem L-11 |
| Môi trường | Docker stack đầy đủ, Postgres **sạch** (`down -v`), Flyway chạy lại **V1..V15** |
| Lệnh | `python evaluate.py --full --repro real` |
| Phạm vi | 10 kịch bản `real`, 27 lượt chạy, nhóm A/B/D |
| **Chế độ nạp** | **100 % live ingestion — 0 lượt backlog, 0 lượt replay** |
| Lỗi runtime | **0** |
| Truy vết | 27/27 bản ghi mang `execution_id` dạng `source::rawId`, đối chiếu được với `saga_instances` |

**Ba tập bằng chứng, không được trộn vào nhau:**

| Tập | Nội dung | Dùng để làm gì |
|---|---|---|
| **Chính thức** | 10 kịch bản `real`, 27 lượt, live ingestion | **Con số headline của luận văn** |
| **Bổ sung `synthetic`** | SC09 (2/2), SC20 (2/2), SC05 (1/2 ở lượt gần nhất) | Kiểm ngữ nghĩa, **không** phải phép đo độ chính xác |
| **Liên hệ thống** | 3 repo Saga ngoài; trên `kafka-microservices-with-saga`: 11 execution live, 2 lượt backlog, 3 lượt replay reset offset | Bằng chứng **tổng quát** và **độc lập thứ tự**; **không** gộp vào con số chính thức |

Phương pháp luận KLTN định nghĩa dataset chính thức là `--repro real`. 18 kịch bản
`synthetic` **chưa được đo lại như một tập** trên build hiện tại (chỉ 3 kịch bản kiểm
riêng). Không được trích số của cả nhóm `synthetic`.

---

## 4. 10 real scenarios

| ID | Nội dung | Kỳ vọng | Kết quả |
|---|---|---|---|
| SC01 | Đơn hàng bình thường, mọi bước thành công | `NO_DIAGNOSIS_NEEDED` | ✅ 5/5 |
| SC02 | Hết hàng → bồi hoàn thành công | `FORWARD_STEP_FAILURE` | ✅ 5/5 |
| SC03 | Saga đứng thật, chưa hồi phục | `INSUFFICIENT_EVIDENCE` | ✅ 2/2 |
| SC04 | Đứng ngắn rồi hồi phục | `TRANSIENT_SERVICE_FAILURE` | ✅ 2/2 |
| SC06 | Circuit breaker mở sau retry | `CIRCUIT_BREAKER_OPEN` | ✅ 3/3 (lenient và strict) |
| SC16 | Đứng ~125s rồi hồi phục | `SERVICE_CRASH` | ❌ 0/2 — trả `TRANSIENT_SERVICE_FAILURE` |
| SC17 | Đứng ~150s, không hồi phục | `SERVICE_CRASH` | ❌ 0/2 — trả `INSUFFICIENT_EVIDENCE` |
| SC27 | payment-service bị pause rồi hồi phục | `DOWNSTREAM_TIMEOUT` | ✅ 2/2 |
| SC28 | payment-service bị SIGSTOP rồi hồi phục | `DOWNSTREAM_TIMEOUT` | ✅ 2/2 |
| SC29 | Ngắt mạng payment-service rồi nối lại | `DOWNSTREAM_TIMEOUT` | ✅ 2/2 |

**Tổng: 8 PASS · 0 PARTIAL · 2 FAIL.** Hai kịch bản hỏng là SC16 và SC17, và cả hai đều
quy về đúng một nguyên nhân đã biết: khoảng trống đánh giá
**`SERVICE_CRASH` / `MISSING_TERMINAL_EVENT`** (§7 L-1).

---

## 5. Metrics hiện tại

> **Lượt chính thức hiện nay là lượt settle 2026-09-21**
> (`results/evaluation_report_settled_20260921.json`, 27 lượt, 0 lỗi,
> `detector_settle_seconds: 35`). Các con số 85.2 % / anomaly 1.000 trong những bản trước
> đo **khi harness gọi RCA trước lúc sweep 30 s của detector kịp chạy** — xem L-24. Không
> trích bộ số cũ nữa.

| Chỉ số | Giá trị |
|---|---|
| **RCA accuracy, oracle cũ (nhãn đã tiêm)** | **74.1 %** (20/27) |
| RCA accuracy (strict) | **74.1 %** — *đọc cảnh báo bên dưới trước khi trích* |
| **RCA Precision / Recall / F1** | **0.722 / 0.650 / 0.684** |
| **TP / FP / FN / TN (RCA)** | **13 / 5 / 7 / 7** |
| **Anomaly Precision / Recall / F1** | **0.852 / 0.852 / 0.852** |
| **TP / FP / FN (anomaly)** | **23 / 4 / 4** — cả 4 "FP" đã kiểm, xem L-24 |
| **Reconstruction accuracy** | **27/27 = 100 %** |
| **Correlation coverage** | **27/27 = 100 %** |
| **Time-to-Insight trung bình** | **86.43 s** |
| **Độ trễ RCA trung bình / p95** | **35.7 ms** / **51.4 ms** |
| Kết quả theo kịch bản (oracle cũ) | **6 đúng 100 %** · SC06 66.7 % · **SC04, SC16, SC17 = 0 %** |
| Theo split | train 64.3 % (14 lượt) · test 84.6 % (13 lượt) |
| Theo nhóm | A **53.8 %** · B **88.9 %** · D **100 %** |

### 5b. Oracle bằng chứng — chấm cái observer được phép kết luận

Bộ số trên chấm output bằng **nguyên nhân người thử nghiệm đã tiêm**. Bộ số dưới chấm bằng
**cái bằng chứng của từng lượt cho phép kết luận**, theo nhãn đã khóa trong
`oracle/labels/*.yaml`. Hai bộ, **không gộp**. Đặc tả và lập luận: `docs/ORACLE.md` mục 5.5.

| | A. đọc theo bằng chứng luật viện dẫn | B. đọc theo chính tên nhãn |
|---|---|---|
| kết luận đúng | **18** | **8** |
| bỏ sót | **0** | **0** |
| từ chối phù hợp | **9** | **9** |
| kết luận không được bảo đảm | **0** | **10** |
| tỷ lệ đề cử | 18/27 (66.7 %) | 18/27 (66.7 %) |
| đúng trong phần đã đề cử | 18/18 (100 %) | 8/18 (44.4 %) |

Chín lượt từ chối là đúng chín lượt bằng chứng không đỡ được claim nào (SC01 ×5, SC03 ×2,
SC17 ×2). Mười lượt lệch giữa hai cách đọc là đúng mười lượt ra `DOWNSTREAM_TIMEOUT` —
bằng chứng luật viện dẫn nói "bước vượt ngưỡng đã khai", còn cái **tên** quy độ trễ cho
downstream, trong khi không quan sát nào định vị được độ trễ nằm ở service, ở broker hay
ở đường mạng.

> ⚠️ **Hai điều phải trích kèm, đừng bỏ.**
> **(1) Cách đọc B là diễn giải đã khai báo, không phải phép đo** — không phép đo nào chốt
> nghĩa của một cái tên; tiền đề và giới hạn của nó ghi trong `oracle/claims.yaml` và
> `ORACLE.md` mục 5.5. Lưu ý `SOURCE_SERVICE_UNAVAILABLE` bao cả "bị cô lập mạng", nên ở
> SC29 claim ấy **tình cờ đúng**: nó bất khả thụ vì *bằng chứng*, không vì sai.
> **(2) Cách đọc A có hai bộ số** vì nhãn SC02 bị sửa sau khi đã đọc output:
> có sửa đổi **18 đúng / 0 không được bảo đảm**; không có sửa đổi (bản mù ở `16382e2`)
> **13 đúng / 5 không được bảo đảm**. Chín lượt từ chối không đổi.

**Bảy lượt oracle cũ trừ điểm đều là chỗ nó đòi một claim bằng chứng không đỡ được**, và
ba trong bảy còn đòi một điều **sai sự thật** (SC04 ×2 đòi "service tự hồi phục" cho một
container khởi động lại bằng tay; SC06 lượt 1 đòi "breaker mở" khi breaker đang đóng). Nên
85.2 % → 74.1 % **không phải chẩn đoán kém đi**, mà là platform thôi khẳng định quá bằng
chứng và oracle cũ trừ điểm đúng chỗ đó.

Bản in đầy đủ: `results/oracle-score-settled-20260921.txt`.
Còn nợ: **người thứ hai đọc lại lập luận** (`ORACLE.md` mục 6 bước 4) — mọi file nhãn đang
để `reviewed_by: null`.

> **Không báo cáo precision / recall / F1 cho tương quan.** Dataset chính thức không có
> negative correlation case nào, nên precision không có mẫu số. Chỉ **coverage 27/27** là
> phát biểu được.

> ⚠️ **Chỉ số strict: không được viết "đã tăng từ 74.1 % lên 85.2 %".**
> 74.1 % đo bằng ngữ nghĩa **pre-F1**: khi đó `primaryRootCauses` luôn rỗng, nên harness
> phải dựng lại "cause đã chọn" từ **toàn bộ danh sách ứng viên**, và strict hỏi *"tập ứng
> viên có đúng bằng tập kỳ vọng không"*. F1 điền `primaryRootCauses` bằng đúng ứng viên
> được đề cử, nên strict nay hỏi *"cause được đề cử có đúng bằng nhãn kỳ vọng không"*.
> **Đầu vào của chỉ số đổi, chẩn đoán của động cơ thì không.** Bằng chứng: SC06 là kịch bản
> duy nhất bị ảnh hưởng và danh sách ứng viên của nó giống hệt ở cả hai lượt
> (`R01 STRONG | R02 WEAK | R20 RULED_OUT`). Hoặc trích strict kèm nguyên đoạn này, hoặc
> chỉ dùng lenient.

> ⚠️ **R14 không làm đổi một chỉ số chính thức nào.** Nó không xuất hiện lần nào trong 27
> lượt (0 anomaly `OUT_OF_ORDER`, 0 ứng viên R14): chế độ hỏng của nó chỉ lộ ra khi nạp
> backlog, mà dataset chính thức là 100 % live. R14 chỉ đổi kết luận trên tập kiểm chứng
> liên hệ thống — xem §7 L-15, và `GROUND_TRUTH.md` §4 cho ngữ nghĩa `ClaimSubject`.

Theo từng nhãn root cause:

| Nhãn | Precision | Recall | F1 | Support |
|---|---|---|---|---|
| CIRCUIT_BREAKER_OPEN | 1.000 | 0.667 | 0.800 | 3 |
| DOWNSTREAM_TIMEOUT | 0.600 | 1.000 | 0.750 | 6 |
| FORWARD_STEP_FAILURE | 1.000 | 1.000 | 1.000 | 5 |
| RETRY_EXHAUSTED | 0.000 | 0.000 | 0.000 | **0** |
| TRANSIENT_SERVICE_FAILURE | 0.000 | 0.000 | 0.000 | 2 |
| **SERVICE_CRASH** | **0.000** | **0.000** | **0.000** | **4** |

> Ba dòng 0.000 cuối bảng **không đọc được như "platform hỏng ở ba nhãn đó"**.
> `TRANSIENT_SERVICE_FAILURE` là nhãn **F5 đã xóa khỏi hệ** vì nó khẳng định điều bằng
> chứng không đỡ; support 2 ở đây là oracle cũ vẫn còn đòi nó. `SERVICE_CRASH` support 4
> là SC16/SC17, nơi oracle cũ đòi một claim im lặng không xác lập được. `RETRY_EXHAUSTED`
> support **0** mà precision 0.000 nghĩa là ngược lại: platform ra nhãn đó ở SC06 lượt 1
> và oracle cũ không có nó trong tập kỳ vọng — đúng lượt mà breaker **đang đóng**. Đối
> chiếu §5b trước khi trích bất kỳ dòng nào trong bảng này.

Baseline comparison (accuracy@1 trên **cùng tập ứng viên**, 20 lượt có chẩn đoán cụ thể):
engine 65.0 % · rule-id order 65.0 % · severity only 65.0 % · first anomaly in time 55.0 %
(cùng điều kiện đề cử: cả bốn 65.0 %).
Trên tập này tầng xếp hạng **không** hơn `rule_id_order` (+0.0 pp) — phát biểu đúng là
*"chưa đo được đóng góp của tầng xếp hạng trên 10 kịch bản real"*, tập quá nhỏ và quá ít
lượt có nhiều ứng viên cạnh tranh.

> **Lịch sử, giữ lại để tham chiếu — không phải số hiện hành:**
>
> | Lần chạy | Con số | Nhãn |
> |---|---|---|
> | 2026-05-23 | RCA accuracy **100 %** | **SUPERSEDED · không tái lập được** — build trước V10–V15, trước `EvidenceTimeline`, trước khi danh tính có namespace; harness sinh ra nó không còn chạy được trên build ấy |
> | 2026-09-06T12:08:42Z | lenient **85.2 %** · strict **74.1 %** | **SUPERSEDED · PRE-F1** — phần lenient trùng với build hiện tại; phần **strict không so sánh được** (xem cảnh báo ở trên) |
>
> Không con số nào trong bảng này thuộc headline hiện hành.

---

## 6. Reproducibility

**Hai lượt độc lập trên build hiện tại, cùng điều kiện (clean DB, consumer đã ấm), cho kết
quả trùng khớp hoàn toàn** ở accuracy, P/R/F1, cả hai confusion matrix, per-class, split,
nhóm và baseline. Chỉ TTI và độ trễ RCA dao động (±1.2 s / ±6 ms).

| Lượt | Thời điểm | Runs | Lenient | Strict | TTI | Độ trễ RCA |
|---|---|---|---|---|---|---|
| A | 2026-09-07T13:43:35Z | 27 | **85.2 %** | 85.2 % | 51.15 s | 38.7 / 61.9 ms |
| B | 2026-09-07T14:39:22Z — **lượt được báo cáo** | 27 | **85.2 %** | 85.2 % | 52.28 s | 44.9 / 62.6 ms |

> ⚠️ **Chỉ hai lượt trên build hiện tại**, ít hơn ba lượt của đợt 2026-09-06. Muốn giữ
> nguyên mức khẳng định về tái lập thì cần chạy thêm một lượt nữa. Đây là hạn chế L-13.

**Điều kiện bắt buộc: consumer phải ấm trước khi chạy.** Đợt trước từng có một lượt lệch
(20/27 lượt, 80.0 %) chỉ vì chạy ngay khi container vừa UP — 7 lượt đầu báo
`saga not found` do Kafka consumer chưa join xong consumer group, nên event tới sau
`SAGA_MATCH_WAIT = 30s` của harness. Cả 7 saga đó **có** trong DB, đúng dạng
`default::<uuid>` — độ trễ khởi động, không phải lỗi tương quan.

**Cách chạy lại:**

```bash
mvn clean install
docker compose down -v && docker compose build && docker compose up -d
# đợi 4 service /actuator/health = UP, rồi tạo 1 đơn và đợi nó hiện ra trong
# GET /api/sagas/{sagaId} — đó là dấu hiệu consumer đã ấm
python evaluate.py --full --repro real
```

Mỗi lượt ~28 phút: kịch bản có thời gian chờ thật (SC03 65s, SC16 125s, SC17 150s) và có
stop/start container.

---

## 7. Known limitations

Những giới hạn dưới đây **đã biết, đã ghi, chưa sửa**. Chúng cần được nói rõ trong luận
văn thay vì để hội đồng phát hiện.

**L-1 · `SERVICE_CRASH` / `MISSING_TERMINAL_EVENT` — khoảng trống đánh giá.** *(Đính chính
2026-09-28: bản trước ghi đây là nguyên nhân của **toàn bộ** 14.8 % còn thiếu — sai; sửa cutoff
không vớt được lượt nào, vì R05 chỉ là `SUPPORTING` mà chỉ số chấm trên nhãn đề cử. Xem L-23.)*
R05 đòi `SAGA_STUCK && MISSING_TERMINAL_EVENT && !STEP_TIMEOUT`, mà
`MISSING_TERMINAL_EVENT` không bao giờ fire.
`AnomalyDetectionService.maxStepTimeoutAcrossWorkflows()` lấy timeout lớn nhất trên **mọi**
workflow đang nạp rồi nhân `missing-terminal-timeout-multiplier: 10`. Deployment hiện tại
nạp 5 spec, trong đó `KMS_ORDER_SAGA` có step timeout 120s ⇒ cutoff **1200s**, trong khi
SC16 chỉ chờ 125s và SC17 chờ 150s. (Đợt 2026-09-06 nạp ít spec hơn nên cutoff là 600s —
vẫn quá lớn: cùng một thất bại, biên độ nhỏ hơn.)
Đây là **coupling giữa các workflow trong một cutoff dùng chung**, không phải hồi quy.
Hệ quả đo được: SC16 ×2 và SC17 ×2 sai; nhãn `SERVICE_CRASH` có P/R/F1 = 0/0/0, support 4.
**Chưa sửa, và cố ý chưa sửa** — sửa để nâng số mà không có lý do kỹ thuật độc lập là làm
hỏng chính phép đo.

**L-2 · Time-domain còn lệch ở 3 detector theo lịch.**
`saga.lastUpdated` là **thời điểm nghiệp vụ của nguồn**, nhưng `detectMissingTerminalEvents`,
`detectCompensationMissing` và `detectMissingCompensationAck` so nó với `Instant.now()` của
observer. `SAGA_STUCK` đã được sửa để đo từ `receivedAt`; ba chỗ này thì chưa. Hệ quả: nguồn
backfill/replay hoặc lệch clock sinh absence claim ngay lập tức. **Chưa sửa.**

**L-3 · Absence claim không được thu hồi khi observation thiếu thực sự tới.**
`retractAbsenceClaims` chỉ có hai trigger: alias merge và late workflow resolution. Một saga
đã resolve workflow, bị raise `SAGA_STUCK`/`MISSING_TERMINAL_EVENT`, rồi nhận event muộn thì
finding vẫn standing. (`ORPHAN_EVENT` có đường thu hồi riêng nên không dính.) **Chưa sửa.**

**L-4 · Tập anomaly không bất biến theo thứ tự ở mức dòng DB.**
Status, startTime, endTime thì bất biến (chứng minh bằng 120 hoán vị). Nhưng một số thứ tự
sinh ra finding rồi thu hồi, thứ tự khác không sinh. Đây là hệ quả **cố ý** của cơ chế
retraction — cần phát biểu chính xác, đừng nói "kết quả hoàn toàn độc lập thứ tự".

**L-5 · `ORPHAN_EVENT` raise tạm thời cho execution chưa phân giải workflow.**
Là quan sát thiếu của observer, được thu hồi khi workflow được phân giải. Hợp lệ nhưng là
một false positive tạm thời có thật.

**L-6 · `SAGA_STUCK` là phát biểu về im lặng quan sát được, không phải về hệ nguồn.**
R05/R06 có phân biệt "đã hồi phục", nhưng ranh giới này cần nói rõ.

**L-7 · `schema-h2.sql` lệch entity.**
Thiếu `source_name`, `correlation_basis`, `trace_id`, `retracted_at`, bảng
`execution_aliases`. `ApplicationContextTest` vẫn xanh vì chỉ assert bean tồn tại, nhưng
scheduled task ném SQL error trong test đó. Schema thật (Postgres/Flyway) đã kiểm chứng
live. **Chưa sửa.**

**L-8 · Không có test concurrency thật.**
`lockInOrder` + `@Version` mới chỉ có bằng chứng ở mức unit.

**L-9 · Compensation retry mới phủ ở mức `DUPLICATE_COMPENSATION`.**

**L-10 · 18 kịch bản `synthetic` chưa đo lại như một tập.**
Chỉ 3 kịch bản được kiểm riêng (SC09, SC20, SC05). Nhiều kịch bản còn lại bơm event với
timestamp lùi quá khứ (`ts_ago`), mà `SAGA_STUCK` nay đo từ `receivedAt` — kỳ vọng của
chúng có thể đã lỗi thời. **Không được trích số của cả nhóm `synthetic`.**

**L-11 · FP-2 `STATE_REGRESSION` — ĐÃ SỬA (commit `90e55e1`).**
Triệu chứng cũ: `STATE_REGRESSION` là claim loại `OCCURRENCE` nên **không thu hồi được**,
nhưng lại có thể được raise trên một **tập bằng chứng chưa đầy đủ** rồi thắng đề cử (thành
R04 `TERMINAL_STATE_OVERWRITE`), che mất nguyên nhân đúng. Phát hiện ở kiểm chứng liên hệ
thống: cùng một tập bản ghi, nạp live thì đề cử R04, nạp backlog thì đề cử R10. Sau đó
**tái hiện trên chính domain demo**: SC05 ra R04 ở một lượt và R10 ở lượt khác trên cùng
dữ liệu bơm vào.

Nguyên nhân: detector coi **mọi** bước tụt hạng trên `progressRank` là bằng chứng, kể cả
giữa hai trạng thái **chưa kết thúc**. Nhưng một run đang chạy tụt hạng một cách hợp lệ —
nhánh song song (stock) báo thành công sau khi nhánh payment đã đẩy run sang
`COMPENSATING` — và việc hai quan sát đó có đứng cạnh nhau hay không phụ thuộc vào phần
dữ liệu đã về.

Bản sửa: `EvidenceTimeline#firstRegression` chỉ báo khi **trạng thái trước là terminal**.
Trạng thái terminal là tuyệt đối, nên vị từ trở thành **đơn điệu**: bằng chứng về thêm
không thể rút lại kết luận, nên claim `OCCURRENCE` không còn phải rút — đúng ngữ nghĩa mà
R04 mang tên. Cái đánh đổi là bỏ mất bước lùi **không terminal** dọc một cạnh phụ thuộc;
chưa hệ nào trong ba hệ đã kiểm chứng sinh ra ca đó, và để khẳng định nó thì cần bằng
chứng rằng các bước ở giữa *không chỉ là chưa quan sát được* — thứ mà một observer đọc
luồng bộ phận không có.

Kiểm chứng: `results/eval-fp2-sc05-sc20-20260908-163642.log` — SC20 (regression thật)
**2/2 giữ R04**, SC05 (trước đây không ổn định) **2/2 chuyển sang R10
`COMPENSATION_STEP_FAILURE`**, đúng nguyên nhân thật. Full suite **421 test, 0 failure**.
`RegressionEvidenceCompletenessTest` (10 test) chốt điều kiện terminal.

**Đã đo lại 27 lượt chính thức** (2026-09-20T12:21:28Z, build `90e55e1`, DB dựng lại từ
rỗng, Flyway V1..V15, consumer ấm): **85.2 % lenient/strict, P/R/F1 0.889/0.800/0.842,
anomaly 1.000/0.852/0.920** — **trùng khớp từng chỉ số** với lượt báo cáo, kể cả per-class,
split, nhóm và bảng baseline. Phù hợp với dự đoán, vì dataset có 0 `STATE_REGRESSION`.
Artefact: `results/evaluation_report_n13_post_fp2_20260920-122200.json`.

**L-12 · FP-3 `MISSING_COMPENSATION_ACK` — ĐÃ SỬA.**
Triệu chứng cũ: `detectMissingCompensationAck` chỉ hỏi "có thấy event bồi hoàn hoàn tất
không", **không hỏi đặc tả có khai event đó không**. Đúng trên demo saga (có khai
`completed`); sai trên `kms`, nơi `ROLLBACK_STOCK` khai `started` và **không** khai
`completed` — đúng bản chất hệ đó — nên finding fire ở **mọi** lần chạy bồi hoàn, vĩnh viễn.

Bản sửa: câu hỏi được đặt **theo từng bước**, và chỉ với bước mà đặc tả nói là **có báo
hoàn tất** (`WorkflowSpec.declaresCompletionEvent`). Một bước chưa bao giờ hứa xác nhận thì
không thể thiếu xác nhận — đúng I10: absence claim cần đặc tả nói event đó đáng lẽ phải có.

Cái đánh đổi, ghi rõ trong javadoc: mất khả năng phát hiện bồi hoàn treo **trên hệ không
công bố completion**. Điều đó vốn chưa bao giờ quan sát được ở đây, vì hai ca đó **giống
hệt nhau trong bằng chứng**; muốn phân biệt thì cần thứ đặc tả không cung cấp, không phải
đọc im lặng gắt hơn.

Hệ quả cho ground truth vẫn giữ: SC05 mang cách đọc "bồi hoàn không được xác nhận **là**
`COMPENSATION_STEP_FAILURE`", **chỉ đúng cho một hệ** — phải dán nhãn phụ thuộc hệ.
`CompensationAckDeclarationTest` (6 test) — 2 ca trong đó **fail trên build trước bản sửa**,
nên đây là regression thật chứ không phải test viết theo hành vi sẵn có.
**Không xuất hiện trong 27 lượt chính thức**, nên không kỳ vọng đổi con số headline —
và **chưa đo lại** sau bản sửa.

**L-13 · Ba lượt tái lập, nhưng trên hai build khác nhau.**
2026-09-07 13:43Z và 14:39Z (build trước FP-2) và 2026-09-20 12:21Z (build sau FP-2,
`90e55e1`). Cả ba trùng khớp headline và mọi bảng con. Được phép nói *"con số đứng vững
qua ba lượt độc lập và qua một thay đổi code có thật"*; **không** được nói *"ba lượt trên
cùng một build"*.

**L-14 · Không có precision/recall/F1 cho tương quan.**
Dataset chính thức không có negative correlation case — không lượt nào mà hai execution
*đáng lẽ không được* gộp — nên precision không có mẫu số. Chỉ **coverage 27/27** phát biểu
được. Không được bịa ra con số P/R/F1 cho tương quan.

**L-15 · Dataset chính thức không có lượt replay/backlog.**
27/27 lượt là live ingestion; harness không đụng tới offset. Chế độ hỏng mà R14 nói tới
**vắng mặt về mặt cấu trúc** khỏi bộ số chính thức — đó chính là lý do R14 không làm đổi
con số nào. Bằng chứng replay/backlog chỉ có ở tập liên hệ thống (2 lượt backlog, 3 lượt
replay reset offset về 0, 0 anomaly trùng lặp), và **là bằng chứng bổ trợ, không phải số
chính thức**.

**L-16 · Môi trường Docker/Windows chập chờn.**
Ba lượt chạy bị huỷ ngay ở health check vì read timeout 5s trên những cổng mà vài giây sau
trả lời trong ~20ms; một lượt bổ sung mất 3/6 inject cùng kiểu. Đây là vấn đề phía host,
không phải hành vi của platform — nhưng nghĩa là **phải phân biệt "lượt chạy hỏng" với
"kết quả xấu"**, và nên gọi ấm các endpoint trước khi chạy.

**L-17 · Phạm vi phát biểu: 27 lượt / 10 kịch bản / 3 trên 5 nhóm.**
Con số chỉ đỡ được phát biểu *"chẩn đoán đúng 85.2 % trên mười kịch bản hạ tầng thật này"*.
**Không** phải một độ chính xác tổng quát, không phải một phát biểu về mọi hệ Saga.

**L-19 · `FORBIDDEN_POST_TERMINAL` phụ thuộc thứ tự đến — I9 chưa đúng ở mọi finding.**
Đã tái hiện trực tiếp (2026-09-20). Cùng ba sự kiện, chỉ đổi thứ tự đến:
`[e1,e2,e3]` → `FORBIDDEN_POST_TERMINAL, STATE_REGRESSION, MULTI_TERMINAL_CONFLICT,
INVALID_TRANSITION`; `[e1,e3,e2]` → `STATE_REGRESSION, OUT_OF_ORDER,
MULTI_TERMINAL_CONFLICT, INVALID_TRANSITION`. `OUT_OF_ORDER` lệch là hợp lệ (I9 miễn trừ
đúng nó); `FORBIDDEN_POST_TERMINAL` biến mất thì không. Nguyên nhân: câu hỏi chỉ được đặt
**một lần, về sự kiện vừa đến** — cùng hình dạng với lỗi FP-2 đã sửa.
**Hệ quả cho luận văn: không được viết "kết quả độc lập thứ tự đọc" không kèm giới hạn.**
Phát biểu đúng hiện nay: *status, startTime, endTime bất biến dưới 120 hoán vị; tập finding
thì chưa.* Xem F3 trong `ROADMAP.md`.

**L-20 · R01 `CIRCUIT_BREAKER_OPEN` gán `STRONG` từ một chuỗi con trong log.**
`payload.contains("circuit breaker") || payload.contains("retry exhausted")` là đủ để ra
`STRONG_EVIDENCE`, và comment ngay cạnh trong code thừa nhận breaker state **chưa được quan
sát**. Bằng chứng đó cùng lắm chỉ đỡ được "retry đã cạn". Vì chỉ `STRONG` mới được đề cử,
claim này đi thẳng lên primary root cause. Ảnh hưởng SC06 trong bộ chính thức.

**L-21 · R06 `TRANSIENT_SERVICE_FAILURE` không phân biệt được hai thế giới.**
`SAGA_STUCK` + terminal + không có vài anomaly khác ⇒ `STRONG_EVIDENCE`, kết luận "service
tự hồi phục". Nhưng `SAGA_STUCK` vốn là phát biểu về **im lặng quan sát được** (L-6), nên
"hệ nguồn tự hồi phục" và "observer nhận dữ liệu muộn" cho **cùng một bằng chứng**.

**L-22 · Nguồn của mốc thời gian không được giữ lại.**
`EventTimeResolver` phân biệt PAYLOAD/TRANSPORT nhưng `EventIngestionService` chỉ dùng nó để
ghi WARN; không cột nào giữ. Downstream đo thời gian mà không biết đang đo đồng hồ nào —
I3 đòi mọi giá trị suy luận mang nhãn xuất xứ.

**L-23 · Giải thích cho 14.8 % thiếu là chưa đủ.**
Tài liệu quy toàn bộ về cutoff làm `MISSING_TERMINAL_EVENT` không fire. Nhưng R05
`SERVICE_CRASH` được gán `SUPPORTING_EVIDENCE`, mà `RcaSelector.promote` **chỉ đề cử
`STRONG`**. Và **cả hai chỉ số đều chấm trên thứ đã được đề cử**, không trên tập ứng viên:
`rca_expectation_matches` nhận `rca_root_cause` + `rca_primary_roots`, `selected_root_cause`
đã bỏ fallback xuống candidate list, ứng viên SUPPORTING nằm riêng ở `rca_evidence_roots`.
Nên sửa cutoff **không vớt được lenient lẫn strict**. Thêm nữa, R06 đòi
`!MISSING_TERMINAL_EVENT`, nên cutoff đúng còn **làm mất** ứng viên STRONG duy nhất đang
được đề cử ở SC16. Hai nguyên nhân độc lập, tài liệu mới kể một.

> Ghi lại vì nó là bài học về phương pháp: nhận định "cutoff có thể vớt lenient" từng được
> viết vào đây ngày 2026-09-20 và **sai**, do đọc docstring của
> `expected_root_matches_strict` (còn nói "candidate list") thay vì lần theo đối số thật.
> Docstring đó đã được sửa.

**L-24 · "anomaly precision 1.000" một phần là hệ quả của việc đọc quá sớm.**
Harness gọi RCA ngay sau khi saga đạt trạng thái cuối, còn `detectStuckSagas` chạy theo
chu kỳ 30 s. Một finding **có thật** nhưng chưa tới lượt sweep thì bị chấm là false
negative, và — quan trọng hơn cho con số precision — detector chưa kịp sinh ra các anomaly
mà kịch bản không liệt kê trong `expected_anomalies`. Thêm `detector_settle_seconds: 35`
vào lượt chạy 2026-09-21 làm precision tụt **1.000 → 0.852**, đúng 4 anomaly mới.

**Cả bốn đều đã kiểm và đều không phải false positive**: `STEP_TIMEOUT` trên SC04 ×2 và
SC16 ×2, với bằng chứng *"Step 'RESERVE_INVENTORY' took 72184ms (timeout: 5000ms)"*,
`thresholdSource = workflow:RESERVE_INVENTORY.timeout` — ngưỡng lấy từ khai báo, hai mốc
thời gian đều nằm trong evidence, và inventory-service lúc đó đang bị `docker stop`. Thiếu
sót nằm ở **danh sách `expected_anomalies` của kịch bản**, không ở detector.

Hệ quả phải viết đúng trong luận văn: **không trích "precision 1.000" nữa.** Con số dùng
được là 0.852, kèm ghi chú rằng 4 FP đã được kiểm từng cái và là thiếu sót của nhãn kỳ
vọng. Và bài học phương pháp: *một chỉ số đọc trước khi hệ kịp phản ứng thì đang đo thời
điểm đọc, không đo hệ.*

**L-18 · Một lượt chạy có thể hỏng từ đầu mà không hề báo lỗi.**
`docker compose down -v` xoá luôn volume Kafka, nên observer khởi động khi **chưa có topic
nào**; consumer subscribe theo pattern, nhận `partitions assigned: []` và **không tự phục
hồi**. Quan sát trực tiếp ngày 2026-09-20: 14 phút, 4 đơn đã tạo, **0 saga được ingest** —
nhìn từ ngoài giống hệt một lượt 0 %. Cùng loại với L-16: **phải phân biệt "lượt hỏng" với
"kết quả xấu"**. Quy trình chạy lại đã được sửa cho đúng (xem `GROUND_TRUTH.md`, mục "Cách
chạy lại"): sinh topic trước → restart observer → kiểm `partitions assigned` khác rỗng →
dựng lại DB quan sát từ rỗng. Bằng chứng lượt hỏng:
`results/eval-n13-20260920-121720-ABORTED-no-partition-assignment.log`.

---

## 8. Capability chưa làm nhưng không block KLTN

Đây là công việc **production hardening**, không phải đóng góp khoa học. Chúng nằm ở
`NEXT`/`LATER` trong `ROADMAP.md` và không ảnh hưởng tới ba đóng góp ở §1.

| | Vì sao không block |
|---|---|
| **L1 DLQ cho Kafka** | Mất event khi DB xuống lâu là rủi ro vận hành, không phải lỗi ngữ nghĩa observer |
| **L2 Consumer lag / backpressure** | Đo lường vận hành |
| **L3 9/18 luật RCA còn trong Java** | Ranh giới đã có lý do rõ (§4 `GROUND_TRUTH.md`); không chặn việc gắn hệ mới |
| **L4 `AnomalyType` là enum Java** | Chưa có use case thật |
| **L5 Xác thực cho `/api/ingest/**`** | Đã ghi rõ là hạn chế; môi trường thực nghiệm là local |
| **L6 `DeduplicationCache` per-instance** | UNIQUE constraint mới là ranh giới đúng đắn |
| RabbitMQ / SQS / gRPC / OTLP ingestion | `RawObservation` đã là seam; thêm transport là viết adapter, không sửa core |

---

## 9. Kết luận

**KLTN-READY WITH KNOWN LIMITATIONS.**

- Các đóng góp ở §1 có design (ADR), implementation và test — **457 test xanh**, gồm một
  property test 120 hoán vị cho luận điểm độc lập thứ tự.
- Trạng thái đã kiểm chứng của từng đóng góp nằm ở **§1.D**: ba đóng góp **HOLDS**
  (tương quan dựa trên bằng chứng · dựng lại workflow độc lập thứ tự đọc · gắn hệ mới
  không cần sửa hệ nguồn), hai đóng góp **PARTIALLY HOLDS** (phát hiện bất thường và RCA
  có ý thức về bằng chứng — **FP-2 đã sửa (L-11)**, còn vướng FP-3, L-12).
- Bằng chứng thực nghiệm **tái lập được trên build hiện tại**: clean DB, migration
  V1..V15 từ đầu, Docker stack, 10/10 kịch bản `real`, 0 lỗi runtime, **hai lượt** cùng
  điều kiện trùng khớp hoàn toàn, mỗi số đo truy được về đúng dòng trong `saga_instances`.
- Con số headline là **85.2 % lenient (23/27)**, không phải 100 %. Phần sụt có nguyên nhân
  xác định, đã đo, đã ghi (L-1), và **cố ý chưa sửa** — sửa để nâng số mà không có lý do
  kỹ thuật độc lập là làm hỏng chính phép đo.
- **Ba tập bằng chứng phải giữ tách bạch** (§3): con số chính thức (27 lượt, live),
  kiểm chứng `synthetic` bổ sung, và kiểm chứng liên hệ thống. Chỉ tập đầu là headline.
- Các giới hạn ở §7 (L-1 … L-23) đều đã biết và ghi rõ. Hai giới hạn đáng nói nhất với hội
  đồng: **L-12 FP-3** (`MISSING_COMPENSATION_ACK` phụ thuộc workflow) và **L-15** —
  dataset chính thức không có lượt replay/backlog nào. **L-11 FP-2**, trước đây là giới
  hạn nặng nhất, **đã sửa và đã đo lại**: con số headline không đổi.
