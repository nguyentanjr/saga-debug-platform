# Đặc tả oracle — chấm cái observer được phép kết luận

> **Trạng thái 2026-09-21: hoàn tất lượt sửa oracle L10/L11; chưa có review mù 6.4.**
> Kiểm chứng đóng task: `oracle/VERIFICATION-20260921.txt`. Trạng thái này không chứng nhận
> tính độc lập của nhãn và không đóng phần đổi tên R07 của F10.
> Cấu trúc bốn trường và bốn quy tắc chấm đã chốt từ trước F4/F5. Nhãn của 27 lượt trong
> `results/evaluation_report_settled_20260921.json` nay nằm trong `oracle/labels/*.yaml`,
> từ vựng claim dùng chung trong `oracle/claims.yaml`, bằng chứng đóng băng trong
> `oracle/evidence/*.json`. **Việc còn nợ: người thứ hai đọc lại lập luận** (mục 6 bước 4).
>
> Đặc tả này được khóa **trước** khi sửa F4/F5, để bản sửa không được chấm bằng chính
> tiêu chuẩn nó vừa làm thay đổi. Nhãn thì dán **sau** F4/F5, nên với 6 kịch bản người dán
> nhãn chưa từng đọc output (SC01, SC02, SC03, SC27, SC28, SC29) việc dán là **mù ở mức
> từng lượt**; với 4 kịch bản còn lại là **hồi cứu**. Từng file ghi rõ nó thuộc loại nào.

---

## 1. Vì sao cần oracle thứ hai

Oracle hiện tại (`expected_rca` trong `evaluate.py`) chấm output của platform bằng
**nguyên nhân người thử nghiệm đã tiêm**. Hai thứ đó không cùng một câu hỏi.

`docker_stop(INVENTORY_CONTAINER)` xác lập nguyên nhân thật của thí nghiệm. Nó **không**
xác lập việc observer được phép kết luận gì: inventory-service bị dừng trước khi order
được tạo, nên nó chưa từng phát ra một event nào, và tập observation không chứa gì phân
biệt được "container bị dừng" với "network partition", "consumer lag", "mapping sai topic"
hay "observer không nhận được".

Chấm bằng nhãn tiêm là đang đòi observer kết luận quá bằng chứng — tức đòi nó vi phạm
đúng luận điểm trung tâm của đề tài. Oracle này tách hai câu hỏi ra.

**Nó không thay oracle cũ.** Mọi lượt chấm theo oracle mới đều phải kèm điểm theo oracle
cũ để đối chiếu. Hai bộ số, không gộp.

---

## 2. Đơn vị đánh giá

Một **ca** gồm ba thứ, cả ba cố định:

- **spec**: workflow declaration đang có hiệu lực cho execution đó.
- **tập observation**: đúng những gì observer thực sự nhận được, không phải những gì hệ
  nguồn đáng lẽ phát ra.
- **thời điểm chấm**: một mốc cố định. Cùng một run chấm ở hai mốc là **hai ca khác nhau**
  — điều kết luận được phụ thuộc mốc đó, và đây chính là thứ đề tài đang nói.

Oracle **không được** dùng kiến thức nằm ngoài ba thứ trên để quyết định cái gì là kết
luận có căn cứ.

---

## 3. Bốn trường của một ca

| Trường | Nội dung | Ai xác lập |
|---|---|---|
| `true_cause` | Lỗi được tiêm, hoặc được xác minh độc lập từ hệ nguồn | Người thử nghiệm |
| `available_evidence` | Observation và metadata observer có trước thời điểm chấm | Đọc từ DB/artifact, không chép từ mô tả scenario |
| `admissible_claims` | Những kết luận đủ bằng chứng, kèm **giả thiết** và **mức cụ thể tối đa** được phép | Người dán nhãn, có lập luận viết ra |
| `abstention_required` | Có bắt buộc từ chối nêu nguyên nhân cụ thể hay không, kèm lý do | Suy ra từ `admissible_claims` |

**`admissible_claims` là một tập, không phải một nhãn.** Đây là lý do không dùng một
trường `determinable_cause`: bằng chứng có thể cho phép "bước RESERVE_INVENTORY không
hoàn tất trong ngưỡng" mà **không** cho phép "service chết". Hai phát biểu khác độ sâu,
và chỉ phát biểu thứ nhất có căn cứ.

`admissible_claims` rỗng ⇔ `abstention_required = true`.

---

## 4. Quy tắc chấm

| Điều kiện bằng chứng | Output của platform | Kết quả |
|---|---|---|
| `admissible_claims` không rỗng | Đề cử một claim **nằm trong** tập đó | **Kết luận đúng** |
| `admissible_claims` không rỗng | Không đề cử gì | **Bỏ sót** |
| `admissible_claims` rỗng | Không đề cử nguyên nhân cụ thể | **Từ chối phù hợp** |
| bất kỳ | Đề cử sai, **hoặc cụ thể hơn mức `admissible_claims` cho phép** | **Kết luận không được bảo đảm** |

Hai điều quan trọng trong bảng này:

- **Đúng tên chưa phải đúng.** Một đề cử tình cờ trùng `true_cause` nhưng vượt quá mức
  `admissible_claims` cho phép vẫn rơi vào nhóm 4. Trùng tên không phải bằng chứng.
- **Nhóm 4 gộp hai lỗi khác nhau** (sai hẳn, và đúng-hướng-nhưng-quá-mức). Khi báo cáo
  phải tách hai, vì chúng dẫn tới hai cách sửa khác nhau.

### Cách báo cáo

**Không gộp bốn nhóm thành một headline mới.** Báo riêng:

- số lượng từng nhóm trong bốn nhóm;
- **tỷ lệ đề cử** — bao nhiêu phần trăm số ca platform chịu nêu một nguyên nhân cụ thể;
- **độ chính xác trong phần đã đề cử** — chỉ tính trên những ca nó đã nêu;
- **khả năng nhận diện trên các ca đủ thông tin** — chỉ tính trên các ca
  `admissible_claims` không rỗng.

Ba chỉ số sau phải đi cùng nhau. Tỷ lệ đề cử thấp mà độ chính xác cao là một hệ từ chối
quá nhiều; tỷ lệ đề cử cao mà độ chính xác thấp là một hệ đoán bừa. Một con số duy nhất
không phân biệt được hai bệnh đó, và đó là lý do 85.2 % không nói được điều đề tài cần nói.

---

## 5. Hồ sơ từng ca — **ĐÃ KHÓA 2026-09-21** (trừ bước 6.4)

> **Nhãn không còn nằm trong file này.** Chúng nằm trong `oracle/labels/<SCENARIO>.yaml`,
> mỗi file khóa vào một fixture bằng chứng có SHA256, và `oracle/score.py` từ chối áp nhãn
> nếu `raw_events` của artefact khác fixture. Phần dưới đây giữ lại **lập luận** dẫn tới
> các nhãn đó và **những chỗ bản dự thảo trước đã sai**, vì cách chúng sai là một luận
> điểm về phương pháp. Kết quả chấm: mục 5.5.

### 5.0 Trạng thái dữ liệu — đã giải quyết, giữ lại vì lập luận

> **Cập nhật 2026-09-21.** Phần cảnh báo "THIẾU DỮ LIỆU" bên dưới đã được giải quyết:
> artefact `evaluation_report_settled_20260921.json` lưu `raw_events` (eventId, eventType,
> payload, timestamp, receivedAt, correlationBasis, transportRef, quan hệ nhân quả),
> `source_state`, `scored_at`, `workflow_type_at_scoring` và `evidence_capture_gaps` cho
> **cả 27 lượt**. `oracle/evidence_view.py` tách artefact thành hai kênh — evidence của
> observer và `source_state` chỉ dành cho oracle — và **cắt bỏ mọi trường `rca_*`,
> `anomalies_*`** để người dán nhãn không thể đọc câu trả lời của hệ trong lúc viết nhãn.
> Còn nợ: nguồn timestamp PAYLOAD/TRANSPORT vẫn chưa được lưu (F6).

**`anomalies_actual` không phải `available_evidence`.** Nó là kết quả suy luận của chính
detector đang được chấm. Dùng nó làm bằng chứng gốc thì oracle phụ thuộc vào
implementation nó phải đánh giá — đúng cái vòng lặp đặc tả này sinh ra để cắt.

Đã kiểm tra `results/evaluation_report_n13_post_fp2_20260920-122200.json`: **artifact không
lưu một event gốc nào**. Mỗi lượt chỉ có `execution_id`, `saga_status`, `saga_duration_ms`,
`anomalies_actual`, và các trường `rca_*`. Không có payload, không có timestamp từng event,
không có nguồn timestamp, không có correlation metadata. Postgres của lượt chạy đó cũng
không còn (stack không chạy).

Nên với **mọi** ca dưới đây, `available_evidence` hiện ở trạng thái **THIẾU DỮ LIỆU**.
Không suy ngược từ `anomalies_actual` hay `saga_status` để lấp chỗ trống.

**Phải bổ sung vào `evaluate.py` trước khi khóa nhãn** — với mỗi lượt, dump:

- danh sách event observer thực sự nhận: `eventId`, `eventType`, `payload`, `timestamp`;
- **nguồn timestamp** (PAYLOAD / TRANSPORT — `EventTimeResolver` đã phân biệt nhưng chưa
  lưu, xem F6);
- `CorrelationBasis` và `execution_id` của từng observation;
- `workflowType` đang có hiệu lực **tại thời điểm chấm**, và chính thời điểm chấm đó.

Chừng nào chưa có, mọi nhãn dưới đây là **dự thảo dựa trên đặc tả hệ nguồn**, không phải
trên quan sát đã ghi lại.

### 5.0b Hai ranh giới phải giữ khi trình bày

**Instrumentation phục vụ thí nghiệm ≠ năng lực tích hợp của observer.**

Để đọc `true_cause` của SC06, payment-service phải mở thêm `circuitbreakerevents` và
`health.show-details`. Đó là **sửa hệ được quan sát**, và nó hợp lệ vì nó phục vụ *người
làm thí nghiệm*, không phục vụ *observer*: correlation-service không gọi một endpoint nào
trong số đó, và kết luận của R01 về breaker được rút từ payload của event, không từ đây.

Khi trình bày tuyên bố "gắn hệ mới không cần sửa hệ nguồn", **không được** gộp hai thứ:

| | Sửa gì | Ai cần |
|---|---|---|
| Tích hợp observer | **không sửa gì** — chỉ YAML phía platform | observer |
| Lấy ground truth | mở endpoint của chính hệ nguồn | người làm thí nghiệm |

Tuyên bố zero-touch chỉ áp cho dòng thứ nhất. Dòng thứ hai là chi phí của việc **kiểm
chứng**, và một triển khai thật sẽ không có nó — đó cũng là một giới hạn của bộ thực
nghiệm, phải ghi ra.

**Điều kiện tái lập, phát hiện ngày 2026-09-21.** Hai lỗi khởi động làm hỏng lượt chạy mà
**không** báo lỗi — chúng hiện ra dưới dạng kết quả sai, không phải exception:

1. **Record không có type header trong topic đang dùng.** payment-service kẹt vĩnh viễn ở
   `order-created-0` offset 15 (`No type information in headers`), không bao giờ đọc tới
   order mới. Triệu chứng: saga treo ở `ORDER_CREATED`, `SAGA_STUCK`,
   `INSUFFICIENT_EVIDENCE` — **trông y hệt một ca từ chối đúng**. Chỉ `bufferedCalls: 0`
   trong `source_state` mới cho thấy hệ nguồn chưa từng chạy. Record độc đến từ đợt kiểm
   chứng N3 multi-cluster, vốn publish record thô vào đúng topic này.
2. **`partitions assigned: []`.** correlation-service subscribe bằng topic-pattern; trên
   một Kafka trắng thì topic chưa tồn tại lúc subscribe, và pattern chỉ quét lại sau
   `metadata.max.age.ms` (mặc định 5 phút). Triệu chứng: saga không xuất hiện, harness báo
   `not found`. Repo đã có `results/eval-n13-20260920-121720-ABORTED-no-partition-assignment.log`
   — cùng bẫy, đã gặp trước đây.

**Kiểm tra trước mỗi lượt đo**, thay vì chữa sau:

- `partitions assigned` **không rỗng** trong log correlation-service;
- `source_state.unavailable` **rỗng**, và bộ đếm của hệ nguồn nhúc nhích qua các lượt.

> ⚠️ **`docker compose down -v` không phải cách chữa mặc định.** Nó xoá sạch dữ liệu của môi
> trường thử nghiệm và ở đây còn kéo theo lỗi thứ 2. Dùng nó khi cần một mốc khởi đầu sạch
> **đã định trước**, không phải để dọn một sự cố. Sự cố record độc lẽ ra được xử lý bằng
> cách chuyển offset của đúng consumer group đó.

---

### 5.1 Mức độ độc lập của người dán nhãn

**Không ca nào dưới đây được dán nhãn mù.** Người viết bản dự thảo này đã đọc output của
platform cho SC06, SC16 và SC17 trước khi viết. Đây là **gán nhãn hồi cứu**, và phải được
ghi đúng như vậy trong luận văn.

Việc còn làm được, và phải làm:

1. Lập luận đi từ **đặc tả hệ nguồn và evidence thô**, không từ output — ghi rõ từng bước.
2. Nhờ người **chưa biết kết quả** rà lại lập luận.
3. Phần kiểm tra độc lập thật sự đến từ **ca mới chưa từng chạy qua platform** (mục 7),
   không từ việc dán nhãn lại dữ liệu cũ.

Không dừng công việc vì không làm mù được dữ liệu cũ. Nhưng cũng không gọi nó là mù.

---

### 5.2 SC06 — `CIRCUIT_BREAKER_REAL`

> **Đã đo, không còn là dự thảo.** Chạy 2026-09-21T06:53Z trên môi trường sạch
> (`docker compose down -v`), build sau F3. Artefact:
> `results/evaluation_report_sc06_evidence_20260921.json` — lượt đầu tiên có `raw_events`
> và `source_state`.
>
> **Bản dự thảo trước của mục này sai hai chỗ.** Chúng được ghi lại ở cuối mục, vì cách
> chúng sai là một lập luận về phương pháp: cả hai đều là suy diễn từ code và cấu hình mà
> không đo, và cả hai đều nghe hợp lý.

#### Quan sát observer thực sự nhận được

Cả ba lượt có **cùng một hình dạng** 5 event:

```
ORDER_CREATED    CREATE_ORDER
PAYMENT_RETRY    PROCESS_PAYMENT   ×3
PAYMENT_FAILED   PROCESS_PAYMENT
```

`anomalies_actual = ['RETRY_BURST']` cả ba lượt. Ứng viên RCA **giống hệt nhau từng
dòng**:

| | |
|---|---|
| R01 `CIRCUIT_BREAKER_OPEN` | `STRONG_EVIDENCE` |
| R02 `TIMEOUT_RETRY_STORM` | `WEAK_EVIDENCE` |
| R20 `OBSERVABILITY_GAP` | `RULED_OUT` |

Output: `CIRCUIT_BREAKER_OPEN` cả ba. Oracle cũ chấm **3/3 đúng**.

#### Trạng thái breaker thật, đọc từ hệ nguồn

| Lượt | `reason` trong payload | Breaker lúc chấm | Transition trong lượt |
|---|---|---|---|
| **1** | `... : Payment gateway unavailable (simulated failure)` | **CLOSED**, buffered 3 | không có. `HALF_OPEN_TO_CLOSED` lúc 06:53:10.074, **ngay trước** lần gọi đầu |
| **2** | `... : CircuitBreaker 'paymentProcessing' is OPEN and does not permit further calls` | **OPEN**, buffered 5 | **`CLOSED_TO_OPEN`** lúc 06:53:25.324 |
| **3** | `... : CircuitBreaker 'paymentProcessing' is OPEN and does not permit further calls` | **OPEN**, buffered 3 | `OPEN_TO_HALF_OPEN` 06:53:35, **`HALF_OPEN_TO_OPEN`** 06:53:40.494 |

**SC06 không phải một ca. Nó là hai ca khác nhau bị gộp dưới một id.**

- **Lượt 1**: breaker **đóng suốt lượt**. Payment thất bại vì gateway từ chối, đúng lỗi đã
  tiêm. `CIRCUIT_BREAKER_OPEN` là **sai về mặt sự kiện**, không chỉ thiếu căn cứ.
- **Lượt 2 và 3**: breaker **thật sự mở** trong lượt, và payload mang **chính thông điệp
  `CallNotPermittedException` của Resilience4j** — đây là hệ nguồn báo trạng thái breaker,
  không phải nhãn tự đặt. `CIRCUIT_BREAKER_OPEN` **đúng và có căn cứ**.

Ba lượt **không độc lập**: cửa sổ trượt của breaker cộng dồn qua warm-up và qua các lượt
trước, nên breaker mở ở lượt 2 và giữ mở sang lượt 3. `n_runs: 3` đang được đọc như ba mẫu
của một ca, trong khi đó là **một chuỗi có trạng thái mang theo**. Đảo thứ tự ba lượt sẽ
cho ba nhãn khác.

#### Vì sao platform vẫn đúng 3/3

Vị từ của R01 ([RcaService.java:506](../correlation-service/src/main/java/com/example/correlation/service/RcaService.java:506)):

```java
payload.contains("circuit breaker") || payload.contains("retry exhausted")
```

Tiền tố `"Circuit breaker / retry exhausted: "` là **hằng số** mà
`handleOrderCreatedFallback` ghi ở **mọi** lần fallback ([PaymentService.java:158](../payment-service/src/main/java/com/example/payment/service/PaymentService.java:158)).
Nên vị từ khớp ở cả ba lượt, kể cả lượt breaker đang đóng. Phần **thật sự phân biệt được**
ba lượt nằm ở **hậu tố** `t.getMessage()` — và R01 không đọc tới đó.

Platform **đúng hai lần vì lý do sai, và sai một lần**, với lập luận giống hệt nhau cả ba
lần. Oracle cũ không nhìn thấy điều nào trong số đó.

#### Bốn trường

| Trường | Lượt 1 | Lượt 2, 3 |
|---|---|---|
| `true_cause` | Gateway từ chối (`failMode`). **Breaker đóng** | Gateway từ chối, **và** breaker đã mở nên chặn lời gọi |
| `available_evidence` | 5 event; `reason` mang thông điệp lỗi mô phỏng | 5 event; `reason` mang thông điệp từ chối của breaker |
| `admissible_claims` | `PROCESS_PAYMENT` thất bại; retry đã cạn theo policy đã khai. **Không tới được** bất kỳ claim nào về breaker | Thêm: **"lời gọi này bị một breaker đang mở từ chối"** |
| `abstention_required` | false, nhưng ở **mức nông hơn** nhãn hiện tại | false |

#### Claim được phép dừng ở đâu — và nó *không* phải root cause

Thông điệp từ chối chứng minh đúng một điều: **breaker đã chặn lời gọi ấy**. Nó **không**
tự chứng minh:

- **vì sao** breaker mở — cần lịch sử lỗi trước đó, thứ observer không có trong tập này;
- rằng breaker là **nguyên nhân gốc sâu nhất** của execution. Ở đây nó không phải: breaker
  mở *vì* gateway đã từ chối từ trước. Breaker là **cơ chế thất bại trực tiếp**, còn nguyên
  nhân nằm dưới nó một tầng.

Nên phát biểu đúng của con số 2/3 là:

> **Hai lượt có evidence hỗ trợ claim "lời gọi bị một breaker đang mở từ chối"; một lượt
> không có.**

Không phải *"hai lượt chẩn đoán đúng nguyên nhân gốc"*.

**Ràng buộc cho oracle:** nếu vẫn dùng nhãn `CIRCUIT_BREAKER_OPEN` làm output RCA thì oracle
phải định nghĩa nó là **cơ chế thất bại trực tiếp**, không mặc nhiên là nguyên nhân gốc cuối
cùng. Thiếu định nghĩa đó thì ngay cả lượt 2 và 3 cũng đang được chấm cho một câu hỏi khác
với câu hỏi mà bằng chứng trả lời được.

Và kể cả sau khi định nghĩa lại: **chất lượng giải thích ở cả ba lượt vẫn lỗi**, vì R01 viện
dẫn **cùng một chữ ký nhập nhằng** cho cả ba. Lượt 2 và 3 đúng kết luận nhưng sai lập luận —
và oracle phải chấm được sự khác biệt đó, nếu không thì "đúng vì lý do sai" vẫn ăn điểm.

**Chấm theo oracle mới:**

| Lượt | Output | Kết quả | Lập luận |
|---|---|---|---|
| 1 | `CIRCUIT_BREAKER_OPEN` STRONG | **Kết luận không được bảo đảm** — vượt evidence, và trái sự thật | chữ ký nhập nhằng |
| 2 | `CIRCUIT_BREAKER_OPEN` STRONG | **Kết luận đúng** ở mức *cơ chế trực tiếp* | chữ ký nhập nhằng |
| 3 | `CIRCUIT_BREAKER_OPEN` STRONG | **Kết luận đúng** ở mức *cơ chế trực tiếp* | chữ ký nhập nhằng |

Oracle cũ: 3/3. Oracle mới: **2/3 kết luận đúng, 1/3 không được bảo đảm, 3/3 lập luận lỗi**.

#### Hệ quả cho F4

F4 vẫn đứng, nhưng **phát biểu phải đổi**. Không phải *"R01 luôn kết luận quá bằng chứng"*
— ở lượt 2 và 3 bằng chứng **có** đủ. Phát biểu đúng:

> R01 khớp trên tiền tố hằng số nên **không phân biệt được** payload có báo trạng thái
> breaker hay không, và gán cùng một tier `STRONG` cho cả hai.

**Hình dạng bản sửa — và hai cách sửa hỏng phải tránh:**

| Cách | Vì sao hỏng |
|---|---|
| Hạ toàn bộ R01 xuống `WEAK` | Sửa quá tay: mất hai kết luận đúng và có căn cứ |
| Đổi sang `payload.contains("OPEN")` trên toàn payload | Vẫn là dò chuỗi trên cả bản ghi. `"OPEN"` có thể đến từ bất cứ trường nào, và nó hard-code cách diễn đạt của **Resilience4j** thành ngữ nghĩa chung cho mọi hệ Saga |

Bản sửa đúng: nhận diện **thông điệp lỗi cụ thể**, ở **trường do mapping xác định**, theo
**quy tắc khai báo cho từng nguồn**. Cùng nguyên tắc I7 mà `SourceMapping` đã áp cho
correlation: hệ nguồn nói ở đâu và nói bằng từ gì là **chuyện của hệ nguồn**, phải nằm trong
khai báo. `"CircuitBreaker ... is OPEN and does not permit further calls"` là cách nói của
một thư viện Java cụ thể; một hệ Go hay .NET sẽ nói khác, và core không được biết trước.

Khi hậu tố không nói gì về breaker, claim phải rơi về mức mà evidence đỡ được —
"retry đã cạn ở bước này" — chứ không phải biến mất.

#### So sánh trước/sau F4 phải chạy lại trên **cùng tập observation**

Ba tập `raw_events` đã lưu trong artefact là đủ để làm việc này. Chạy build **trước** và
**sau** F4 trên **nguyên ba tập đó**, cùng spec, cùng mốc chấm. Khi ấy chênh lệch là chênh
lệch của thuật toán, không bị trạng thái breaker live làm nhiễu.

Chỉ **sau** đó mới chạy kiểm chứng live, và phải chọn dứt khoát một trong hai thiết kế:

1. **Mỗi lượt độc lập** — reset về trạng thái khởi đầu **đã xác minh** trước mỗi lượt
   (breaker CLOSED, bộ đếm 0, xác nhận bằng `source_state` chứ không giả định).
2. **Chuỗi có trạng thái** — giữ trạng thái, coi **cả ba lượt là một thí nghiệm**, và lặp
   lại **cả chuỗi** khi muốn thêm mẫu.

⚠️ **Cố định thứ tự ba lượt là chưa đủ.** Trạng thái khởi đầu và thời gian chờ vẫn đổi giữa
các lần chạy — lượt 1 hôm 2026-09-21 gặp `HALF_OPEN_TO_CLOSED` xảy ra **28 ms trước** lời
gọi đầu tiên của nó, tức nó suýt rơi vào một ca khác hẳn. Thứ tự cố định mà trạng thái khởi
đầu trôi thì vẫn là ba ca khác nhau mang một tên.

#### Hai chỗ bản dự thảo trước đã sai


**Sai 1 — "chuỗi reason là hằng số viết cứng, không đọc trạng thái breaker".**
Đúng với *tiền tố*, sai với *cả chuỗi*. Hậu tố là `t.getMessage()`, và khi breaker mở thì
đó là thông điệp của `CallNotPermittedException` — một báo cáo trạng thái breaker thật.
Đọc code thấy `"Circuit breaker / ..." + t.getMessage()` mà không hỏi `t` là gì.

**Sai 2 — "`minimumNumberOfCalls: 5` nên một lượt SC06 không đủ mở breaker, nhiều khả năng
breaker đóng suốt SC06".**
Đúng cho *một lượt tính riêng*, sai cho *scenario*. Breaker mở ở lượt 2 vì cửa sổ cộng dồn
qua warm-up và các lượt trước. Tôi có nêu cảnh báo "cửa sổ không reset giữa các scenario"
nhưng vẫn đi tới kết luận như thể nó reset.

Cả hai sai theo cùng một kiểu: **suy từ code và cấu hình rồi dừng lại ở chỗ nghe hợp lý.**
Cả hai đều bị một artefact chứa `raw_events` và `source_state` bác bỏ trong vài giây. Đó
chính là lập luận cho mục 5.0 — và nó áp cho SC16/SC17 y như vậy: **phần phác thảo của hai
ca đó chưa được đo, nên chưa được tin.**

### 5.3 SC17 — stuck vĩnh viễn, không hồi phục *(phác thảo — đã thay bằng nhãn đo được)*

> Nhãn chính thức: `oracle/labels/SC17_STUCK_DEAD_PERMANENT.yaml`, dán từ evidence thật
> chứ không từ mô tả. Phác thảo dưới đây **đoán đúng** `admissible_claims` rỗng và
> `abstention_required = true`; giữ lại để so đoán với đo. Hai thứ nhãn đo được nói thêm
> mà phác thảo không có: im lặng tại mốc chấm là **186.1 s**, và ở mốc đó bộ dữ liệu
> **không** có phản ví dụ hồi phục nào dài hơn — trong khi ở SC03 (101 s) thì có.

| Trường | Nội dung |
|---|---|
| `true_cause` | inventory-service bị `docker stop` **trước khi** tạo order, nên chưa từng phát event nào |
| `available_evidence` | **THIẾU DỮ LIỆU** (5.0). Biết được: saga ở `IN_PROGRESS`, không event nào từ inventory-service |
| `admissible_claims` | *(dự kiến rỗng)* — im lặng không phân biệt được crash / partition / lag / mapping sai / observer chậm |
| `abstention_required` | *(dự kiến true)* |
| Output thực tế | `INSUFFICIENT_EVIDENCE`, không đề cử. Ứng viên: `STUCK_UNKNOWN_CAUSE`, `OBSERVABILITY_GAP` |
| Chấm sơ bộ | **Từ chối phù hợp** |

> ⚠️ **Không dùng SC17 làm bằng chứng rằng tầng bằng chứng biết từ chối.** R05 đòi
> `hasStuck && hasMissingTerminal`, mà `MISSING_TERMINAL_EVENT` không fire (cutoff 1200s,
> SC17 chờ 150s). R05 **chưa từng thành ứng viên** — engine không cân nhắc rồi từ chối; nó
> vắng mặt. Verdict đúng, cơ chế không phải cái muốn trưng ra. Đây là lý do đối chứng 3 ở
> mục 7 bắt buộc.

### 5.4 SC16 — stuck rồi hồi phục *(phác thảo — đã thay bằng nhãn đo được)*

> Nhãn chính thức: `oracle/labels/SC16_STUCK_RECOVERED_LONG.yaml`. Câu hỏi bỏ ngỏ ở cuối
> mục này — *"hoàn tất sau một khoảng im lặng có cho phép kết luận service tự hồi phục
> không?"* — nay đã trả lời: **không**, và `admissible_claims` dừng ở
> `STEP_EXCEEDED_DECLARED_TIMEOUT` + `EXECUTION_COMPLETED_AFTER_SILENCE`. Đó cũng đúng
> chỗ F5 đưa R06 về. Lưu ý output thực tế trong artefact mới **không còn** là
> `TRANSIENT_SERVICE_FAILURE` như ghi dưới đây.

| Trường | Nội dung |
|---|---|
| `true_cause` | `docker stop` inventory-service, rồi **người thử nghiệm `docker start` bằng tay** |
| `available_evidence` | **THIẾU DỮ LIỆU** (5.0). Biết được: saga về `COMPLETED` sau một khoảng im lặng |
| `admissible_claims` | *(chưa quyết)* |
| Output thực tế | `TRANSIENT_SERVICE_FAILURE` (R06, `STRONG_EVIDENCE`) |
| Chấm sơ bộ | *(chưa quyết)* |

Câu hỏi phải trả lời trước khi khóa: **"hoàn tất sau một khoảng im lặng" có cho phép kết
luận "service tự hồi phục" không?** Bằng chứng không phân biệt được *hệ nguồn tự hồi phục*
với *observer nhận muộn* — và ở SC16 sự thật là **không cái nào**: có người khởi động lại
bằng tay. Ba thế giới, một tập observation.

Nếu `admissible_claims` chỉ chứa mức rộng ("bước này từng vượt ngưỡng rồi sau đó hoàn
tất") thì đề cử hiện tại là **kết luận không được bảo đảm**, không phải "sai tên". Đó là
nội dung F5.

---

### 5.5 Kết quả chấm 27 lượt theo nhãn đã khóa

Lệnh:

```
python oracle/evidence_view.py results/evaluation_report_settled_20260921.json --freeze oracle/evidence
python oracle/score.py results/evaluation_report_settled_20260921.json --labels oracle/labels
```

> **Khóa danh tính ca (ROADMAP L10, đã làm 2026-09-21).** Mục 2 định nghĩa ca bằng
> spec + tập observation + mốc chấm. Bản đầu của `validate_evidence` chỉ đối chiếu
> `raw_events`, nên một file nhãn vẫn áp được cho lượt chấm ở mốc khác dưới workflow khác —
> review đã tái hiện đúng vậy. Nay fixture giữ cả ba và scorer từ chối, nêu tên thành phần
> lệch. Thành phần yếu nhất vẫn là spec: artefact chỉ lưu **tên** workflow, nên hash của
> file khai báo tại freeze chỉ là mốc để đối chiếu thủ công, chưa được validator so;
> **không** chứng minh bytes nào đã có hiệu lực lúc chạy. Đây là giới hạn đã chấp nhận
> khi đóng phạm vi L10; chưa phải khóa nội dung spec runtime.

Artefact: `results/evaluation_report_settled_20260921.json`
(sha256 `64eb5652…b4368`, 27 lượt, 0 lỗi, settle 35 s). Bản in đầy đủ:
`results/oracle-score-settled-20260921.txt`. Coverage oracle **27/27**, 0 nhãn lạ.

#### Hai cách đọc một nhãn, không gộp

Một nhãn của platform mang **hai** phát biểu: cái mà bằng chứng luật viện dẫn xác lập, và
cái mà **chính tên nhãn** khẳng định. `DOWNSTREAM_TIMEOUT` viện dẫn *"bước X mất 72184 ms,
ngưỡng khai báo 5000 ms, thresholdSource = workflow:RESERVE_INVENTORY.timeout"* — một phép
so hai mốc thời gian với một ngưỡng, đúng mức observation. Nhưng cái tên quy độ trễ ấy cho
**downstream**, và suggestion đầu tiên là "Check downstream service performance".

Chọn một trong hai cách đọc là tự chọn một con số. `score.py` in cả hai:

| | A. đọc theo bằng chứng | B. đọc theo tên nhãn |
|---|---|---|
| kết luận đúng | **18** | **8** |
| bỏ sót | 0 | 0 |
| từ chối phù hợp | **9** | **9** |
| kết luận không được bảo đảm | **0** | **10** |
| tỷ lệ đề cử | 18/27 (66.7 %) | 18/27 (66.7 %) |
| đúng trong phần đã đề cử | 18/18 (100 %) | 8/18 (44.4 %) |

> ⚠️ **Cách đọc B là một diễn giải đã khai báo, không phải một phép đo.** Không phép đo nào
> chốt được nghĩa của một cái tên. Tiền đề của nó — rằng `DOWNSTREAM_TIMEOUT` nói nhiều hơn
> khoảng thời gian mà chính R07 viện dẫn — dựa trên chữ "downstream" và suggestion
> *"Check downstream service performance"* mà luật ấy kèm theo. Bác tiền đề đó thì chỉ còn
> cách đọc A. Cái **không** phụ thuộc lựa chọn: một khi đã nhận cái tên khẳng định điều gì
> đó ở mức nguyên nhân, map vào claim mức `cause` nào cũng cho 10, vì không ca nào trong
> năm kịch bản đó có claim mức `cause` nào được bằng chứng đỡ. Xem ROADMAP F10.
>
> **SC29 không tự bác bỏ claim đã map.** `SOURCE_SERVICE_UNAVAILABLE` bao gồm cả "bị cô lập
> mạng", nên ở SC29 nó tình cờ **đúng**. Nó bất khả thụ vì **bằng chứng**, không vì sai —
> đúng như mục 4: trùng tên không phải bằng chứng.

> ⚠️ **Hai bộ số cho cách đọc A, vì nhãn SC02 bị sửa sau khi đã đọc output.**
> Có sửa đổi: **18 đúng / 0 không được bảo đảm**. Không có sửa đổi (bản mù ở `16382e2`):
> **13 đúng / 5 không được bảo đảm**, 9 từ chối giữ nguyên. Trích cả hai.
> Chi tiết và lý do giữ sửa đổi: khối `amendments` trong
> `oracle/labels/SC02_COMPENSATION_HAPPY_PATH.yaml`.

Chín lượt từ chối là **đúng chín lượt** mà bằng chứng không đỡ được claim nào: SC01 ×5
(không có gì hỏng), SC03 ×2 và SC17 ×2 (im lặng). Không lượt nào từ chối nhầm chỗ, và
không lượt nào bỏ sót.

Mười lượt lệch giữa hai cách đọc là **đúng mười lượt** ra `DOWNSTREAM_TIMEOUT`:
SC04 ×2, SC16 ×2, SC27 ×2, SC28 ×2, SC29 ×2.

#### Đối chiếu với oracle cũ: 20/27, và bảy lượt lệch nói lên điều gì

| | |
|---|---|
| oracle cũ (nhãn đã tiêm) | **20/27** |
| oracle bằng chứng, đọc A | **18 đúng + 9 từ chối đúng, 0 không được bảo đảm** |

Bảy lượt oracle cũ chấm **sai** — và cả bảy đều là chỗ oracle cũ **đòi một claim mà bằng
chứng không đỡ được**:

| Lượt | Platform ra | Oracle cũ đòi | Vấn đề của cái oracle cũ đòi |
|---|---|---|---|
| SC04 ×2 | `DOWNSTREAM_TIMEOUT` | `TRANSIENT_SERVICE_FAILURE` | Nhãn này **F5 đã xóa vì nó sai**: container được khởi động lại bằng tay |
| SC06 lượt 1 | `RETRY_EXHAUSTED` | `CIRCUIT_BREAKER_OPEN` | **Không lời gọi nào bị từ chối**: cả bốn lần thử đều có kết quả trong tập observation và đều báo lỗi gateway. ("Breaker đóng suốt lượt" là suy luận có giả thiết — xem dưới) |
| SC16 ×2 | `DOWNSTREAM_TIMEOUT` | `SERVICE_CRASH` | Service không crash: nó bị `docker stop` rồi `docker start` bằng tay |
| SC17 ×2 | *(không đề cử)* | `SERVICE_CRASH` | Im lặng không phân biệt được crash với bốn thế giới khác |

Nên phát biểu đúng của việc 85.2 % → 74.1 % **không phải** "chẩn đoán kém đi", mà:

> Ba lượt platform thôi khẳng định một điều **sai sự thật**, bốn lượt nữa thôi khẳng định
> một điều **không có bằng chứng**, và oracle cũ trừ điểm đúng bảy lượt đó.

#### Đối chứng 1 của mục 7 đã có sẵn trong dữ liệu

SC27 (`docker pause`), SC28 (`SIGSTOP` vào PID 1), SC29 (ngắt Docker network) là **ba
nguyên nhân thật khác hẳn nhau** sinh ra **một hình dạng quan sát**:
`ORDER_CREATED` → im lặng 46–49 s → `PAYMENT_COMPLETED` → `INVENTORY_RESERVED`, COMPLETED.

> ⚠️ **Đã thu hẹp sau review (ROADMAP L11).** Bản trước viết "không một trường nào phân
> biệt được ba ca" — nói quá. Sáu tập observation **khác nhau**: khác eventId, timestamp,
> transportRef và độ dài khoảng im lặng. Theo mục 2 chúng là **sáu ca khác nhau**, không
> phải "cùng một tập observation", nên đây chưa phải đối chứng hai input đồng nhất.

Phát biểu đo được, và là phát biểu đủ cho lập luận:

1. Tập tên trường trong payload **giống nhau** ở cả ba kịch bản, và không trường nào mang
   giá trị đặc trưng cho loại sự cố — không event nào nói về tiến trình, tín hiệu hay mạng.
2. Đại lượng duy nhất khác nhau là độ dài im lặng, và nó **không** phân biệt được ba kịch
   bản: độ tản **trong** một kịch bản lên tới **2.586 s** (SC27: 46.257 / 48.843), lớn hơn
   chênh lệch trung bình lớn nhất **giữa** hai kịch bản bất kỳ (**1.502 s**).

Platform trả **cùng một nhãn** cho cả sáu lượt. Kết luận rút ra không phải "cùng bằng chứng
thì cùng kết luận" (vì bằng chứng không đồng nhất), mà: **không tập nào trong sáu tập chứa
dấu hiệu của loại sự cố đã gây ra nó**, nên mọi nhãn nêu tên loại sự cố đều vượt bằng chứng.

#### Một khuyết tật mới, cùng họ với F4 và F5

F4 sửa R01 vì **vị từ** khớp trên tiền tố hằng. F5 sửa R06 vì **claim** nói về sức khỏe hệ
nguồn trong khi bằng chứng chỉ nói về execution. R07 bây giờ là chỗ thứ ba của cùng một
họ: **bằng chứng đúng mức, cái tên thì không**. Nó không đổi một dòng logic nào của R07 —
thứ phải đổi là nhãn `DOWNSTREAM_TIMEOUT` và suggestion đi kèm, sang một tên chỉ nói đúng
điều nó đo: *bước vượt ngưỡng đã khai báo*. Chưa mở mục roadmap; ghi ở đây trước.

#### Giới hạn của kênh ground truth, phát hiện khi dán nhãn

`breaker_transitions` đọc từ `/actuator/circuitbreakerevents`, mà Resilience4j giữ trong
vòng đệm `eventConsumerBufferSize: 10` **dùng chung cho mọi loại event**. Bằng chứng cho
tính mất mát nằm ngay trong artefact: danh sách của SC06 lượt 3 không còn `CLOSED_TO_OPEN`
lúc 11:47:26 mà danh sách của lượt 2 có. Nên **danh sách transition rỗng không chứng minh
là không có transition** — phải có nguồn thứ hai độc lập với vòng đệm, như
`state` + `notPermittedCalls` trong snapshot, mới kết luận được.

> ⚠️ **Đã thu hẹp sau review (ROADMAP L11).** Bản trước nói snapshot `CLOSED` +
> `notPermittedCalls = 0` đủ để kết luận breaker đóng **suốt** lượt 1. Không đủ: snapshot là
> một thời điểm (35 s sau quan sát cuối), danh sách transition thì mất mát, và
> `notPermittedCalls` bị reset mỗi lần đổi trạng thái — một lần mở rồi đóng lại đọc ra y hệt
> một lần chưa bao giờ mở. Phần **đã đo** là: không lời gọi nào trong lượt bị từ chối, vì cả
> bốn lần thử đều có kết quả trong tập observation và đều báo lỗi gateway. Phần **suy ra**
> là trạng thái breaker. Nhãn chỉ cần phần đã đo.

---

## 6. Phải làm gì trước khi khóa nhãn

Không scenario nào được khóa chỉ bằng mô tả trong `evaluate.py`. Với **từng ca**:

1. ~~Đọc `available_evidence` từ DB / artifact thật, không từ trường `description`.~~
   **XONG 2026-09-21** — `oracle/evidence_view.py`, fixture trong `oracle/evidence/`.
2. ~~Viết `admissible_claims` **kèm lập luận**~~ **XONG** — `oracle/labels/*.yaml`, 27 ca.
3. ~~Ghi rõ giả thiết đang dựa vào~~ **XONG** — trường `assumptions` ở từng ca.
4. **CÒN NỢ.** Người thứ hai đọc lại lập luận, không đọc output của platform.
   Mọi file nhãn đang để `reviewed_by: null`, và `score.py` in cảnh báo đó ở mỗi lần chấm.

Bước 4 là thứ giữ cho oracle khỏi bị uốn theo hành vi hiện có của hệ. Nếu không làm được
thì phải **viết `admissible_claims` xong trước khi xem output**, và ghi ngày.

Điều kiện thay thế đó đã được làm, và làm bằng cơ chế chứ không bằng lời hứa:

- `evidence_view.py` liệt kê **tường minh** các trường là output của platform và loại
  chúng khỏi mọi bản in lẫn fixture, nên bản nhãn không thể đọc câu trả lời của hệ.
- `admissible_claims` được commit ở `16382e2` **trước** commit điền `platform_labels`.
  Thứ tự đó nằm trong lịch sử git, không nằm trong lời kể. **Trừ SC02**: tập claim của
  5 lượt đó bị sửa sau khi đã đọc output (`d75fb21`), nên câu "toàn bộ tập claim hiện tại
  được khóa trước output" **không đúng** — xem gạch đầu dòng cuối và mục 5.5.
- Sáu kịch bản (SC01, SC02, SC03, SC27, SC28, SC29) người dán nhãn chưa từng đọc output
  từng lượt: đó là **mù ở mức lượt**, nhưng **không** mù ở mức bộ — người dán nhãn đã biết
  các con số tổng và biết 5 lượt nào đổi kết quả sau khi settle. Ghi đúng như vậy, đừng
  gọi nó là dán nhãn mù.
- Một claim được thêm **sau** khi đọc tập nhãn của platform (`FORWARD_STEP_FAILED`).
  Ghi chú ban đầu nói việc thêm là trung lập vì không có nó thì 5 lượt sẽ là UNSCORED.
  **Ghi chú đó sai, đã sửa sau review (ROADMAP L11):** bỏ claim khỏi ca mà vẫn giữ nó
  trong từ vựng thì 5 lượt thành **kết luận không được bảo đảm**; UNSCORED chỉ xảy ra khi
  bỏ luôn phép map khỏi `claims.yaml`. Nghĩa là sửa đổi ấy **đổi điểm**: 13 → 18 kết luận
  đúng. Nó được giữ vì claim đứng được trên chính bằng chứng, nhưng cả hai bộ số phải
  được báo cáo, và SC02 nay mang nhãn `blind_then_amended_after_output`. Cơ chế được chốt
  bằng test `test_dropping_a_claim_from_a_case_is_unwarranted_not_unscored`.

---

## 7. Ba đối chứng bắt buộc

Thiếu bất kỳ cái nào thì bộ số không phát biểu được điều nó định phát biểu.

**Đối chứng 1 — cùng evidence, khác `true_cause`. ✅ ĐÃ CÓ, không phải thiết kế thêm.**
Hai ca cho observer **đúng cùng một tập observation** nhưng nguyên nhân thật khác nhau.
Chứng minh: observer không thể phân biệt, nên từ chối là đáp án đúng, không phải né tránh.
SC27 / SC28 / SC29 thỏa điều này với **ba** nguyên nhân thật (pause, SIGSTOP, ngắt mạng)
trên một hình dạng quan sát duy nhất — xem mục 5.5. Ngoài ra SC03 / SC17 là một đối chứng
thứ hai loại này, khác nhau ở **mốc chấm** chứ không ở nguyên nhân (mục 2).

**Đối chứng 2 — có evidence phân biệt rõ. ✅ ĐÃ CÓ.**
Ca mà hệ nguồn **có** phát event lỗi tường minh. Chứng minh: hệ không từ chối mọi thứ.
Thiếu cái này thì một hệ luôn trả `INSUFFICIENT_EVIDENCE` vẫn trông tốt trên mọi chỉ số.
SC06 lượt 2 và 3 (hệ nguồn báo `CallNotPermittedException` trong payload) và SC02 ×5
(`INVENTORY_FAILED` mang lý do của chính inventory-service) là các ca đó: 18/27 lượt có đề
cử, nên hệ **không** phải một cỗ máy từ chối.

**Đối chứng 3 — R05 thực sự xuất hiện ở `SUPPORTING_EVIDENCE`.**
Ca mà `MISSING_TERMINAL_EVENT` **có** fire, nên R05 vào được tập ứng viên ở mức SUPPORTING,
và selector **không** đề cử nó. Chứng minh: việc từ chối đến từ **tầng bằng chứng**, chứ
không phải từ việc detector chưa kịp tạo ứng viên. SC17 hôm nay **không** làm được việc này.

---

## 8. So sánh baseline

Bảng `baseline_comparison` hiện tại chấm 20/27 lượt, lọc theo `rca_expected_status ∈
{EVIDENCE_AVAILABLE, SELECTED}` (`evaluate.py:2336`) — tức theo **trạng thái kỳ vọng**, không
theo output engine. SC17 **có** trong mẫu; cả engine lẫn `rule_id_order` đều bị chấm sai
vì oracle cũ đòi `SERVICE_CRASH`. Vấn đề nằm ở oracle, không ở việc loại mẫu.

Hai yêu cầu khi chấm lại:

1. **Từ chối phải là một lựa chọn chấm được**, không phải lý do bị loại khỏi mẫu. Một
   chính sách baseline luôn nêu một tên phải bị tính là kết luận không được bảo đảm ở
   đúng những ca đòi từ chối.
2. **Muốn đo riêng tầng xếp hạng thì mọi phương án phải dùng cùng điều kiện đủ tư cách
   đề cử.** Hiện engine chỉ đề cử `STRONG_EVIDENCE` còn baseline chọn trên tập ứng viên
   rộng hơn, nên chênh lệch đang trộn hai thứ: *ai xếp hạng tốt hơn* và *ai chịu nêu tên
   nhiều hơn*. Tách ra thì mới biết tầng xếp hạng đóng góp gì.

---

## 9. F9 nằm trong phạm vi đánh giá này

F9 (cặp evidence mà `FORBIDDEN_POST_TERMINAL` trích dẫn còn phụ thuộc tiền tố) **không**
chỉ là chuyện trích dẫn, nên nó ảnh hưởng tới oracle này. Đường truyền:

```
Anomaly.evidenceEventIds
  → RcaService.evidence(Anomaly)               RcaService.java:919
  → Evidence.eventIds()
  → anchorCandidates: lấy event sớm nhất       RcaService.java:267-283
  → setAnchorAt + setAnchorStepId
  → RcaSelector.comparator:
       statusRank → anchorPosition → anchorAt → severity → ruleId
```

`anchorPosition` và `anchorAt` đứng **trên** severity, và R04 `TERMINAL_STATE_OVERWRITE`
(`STRONG_EVIDENCE`) ăn thẳng `FORBIDDEN_POST_TERMINAL`.

**Phát biểu được giữ, đúng mức này và không hơn:**

> Evidence thay đổi theo thứ tự giao, và truyền tới thuộc tính được dùng để xếp hạng.
> **Có nguy cơ** đổi thứ hạng và đổi đề cử.

Đã đo: một run có ba cặp hậu-terminal cho **3 dạng `(evidenceEventIds, measuredAt)` khác
nhau** trên 120 hoán vị, với event sớm nhất khác nhau ⇒ **anchor khác nhau**.
**Chưa đo được** thứ hạng có đổi hay không — điều đó cần một lượt chạy có **hai ứng viên
cạnh tranh cùng tầng**, và chưa dựng được ca đó. "Anchor khác" **không phải** "thứ hạng
khác"; giữa hai câu đó còn thiếu một phép đo.

Hệ quả cho oracle: mọi ca đối chứng sinh `FORBIDDEN_POST_TERMINAL` hoặc `STATE_REGRESSION`
nên được chấm trên **thứ tự giao cố định**, hoặc chấm trên mọi hoán vị. Đó là **kiểm soát
một yếu tố gây nhiễu**, không phải cách đóng F9.

---

## 10. Ràng buộc quy trình

Ba thay đổi sau **phải tách rời**, không gộp rồi gọi chênh lệch là cải thiện thuật toán:

| Thay đổi | Đổi cái gì |
|---|---|
| Sửa F4 / F5 | Hành vi chẩn đoán |
| Khóa oracle này | Câu hỏi đánh giá |
| Chạy lại | Con số |

Trình tự bắt buộc:

1. Khóa đặc tả này và nhãn từng ca — **trước khi** xem kết quả của bản sửa F4/F5.
2. Sửa F4/F5.
3. Chấm **cả build trước lẫn build sau** bằng **cùng một** oracle.
4. Giữ thêm điểm theo oracle cũ, cho cả hai build.

Không có bước 3 thì không phân biệt được "thuật toán tốt lên" với "câu hỏi dễ đi".
