# Roadmap — correlation-service

Chỉ implement item ở `IN PROGRESS` hoặc `NEXT`, và chỉ khi được yêu cầu.
Item `LATER` / `OUT OF SCOPE` không được tự kéo vào.

---

## DONE

**L10 — Khóa ca oracle theo dữ liệu artefact hiện có (đóng phạm vi 2026-09-21)**
Problem: validator cũ chỉ so raw_events, chấp nhận ca đã đổi mốc chấm/workflow.
Kết quả: commit 963d4b6 khóa raw_events, scored_at và workflow_type_at_scoring;
fixture thiếu trường bị từ chối. Kiểm chứng đóng task sửa thêm CLI để in cảnh báo
thành phần null (validator đã trả cảnh báo nhưng CLI bỏ quên), kèm regression test.
Ca SC03 đổi riêng từng thành phần đều bị từ chối; 27/27 ca gốc vẫn chấm được.
Giới hạn được chấp nhận khi đóng task: spec chỉ khóa TÊN; hash file tại freeze là mốc
đối chiếu thủ công, chưa được validator so và không chứng minh spec runtime. Đóng dấu
hash/version từ engine không thuộc lượt sửa này. Không coi đây là khóa đầy đủ spec bytes.

**L11 — Đính chính quy trình gán nhãn và đối chứng (đóng phạm vi 2026-09-21)**
Kết quả 963d4b6: SC02 ghi blind_then_amended_after_output và công khai sửa đổi làm
5 ca đổi điểm; SC27/28/29 chỉ cùng hình dạng, không phải input đồng nhất;
SC06 tách observation về lời gọi khỏi suy luận breaker đóng suốt lượt.
Kiểm chứng: giữ vocabulary hiện tại, đọc admissible_claims từ 16382e2 cho A 13 đúng,
5 không được bảo đảm, 9 từ chối; nhãn hiện tại cho A 18/0/9. Không đổi nhãn khi đóng task.
Review mù 6.4 chưa thực hiện, reviewed_by vẫn null: giới hạn công bố, không chặn việc
đóng phần sửa code/tài liệu này. Báo cáo lưu tại oracle/VERIFICATION-20260921.txt.

**Identity có namespace theo source** (ADR 0002, V13)
Problem: `ORD-A` của hai hệ khác nhau trộn thành một execution, sinh bốn anomaly bịa.
Expected: danh tính là `(source, rawId)` ở cả execution id, event id và alias key.

**Correlation theo thang bằng chứng** (ADR 0003)
Problem: hệ không có id chung xuyên suốt thì không quan sát được.
Expected: `DECLARED_KEY → TRACE_CONTEXT → ALIAS_LINK → từ chối`, có `linked_by_event_id`.

**Workflow resolution theo source** (ADR 0004)
Problem: gắn thêm hệ B làm run của hệ A thành `UNRESOLVED` vì trùng tên sự kiện.
Expected: thu hẹp theo source trước khi xét nhập nhằng; không đoán.

**Transaction boundary của identity** (ADR 0005)
Problem: alias merge commit độc lập, có thể trỏ tới event chưa được lưu.
Expected: resolve → merge → persist trong một transaction local; mark dedup sau commit.

**Anomaly retraction khi correlation đổi** (ADR 0006, V14)
Problem: merge để lại absence claim đã bị chính nó bác bỏ.
Expected: `OCCURRENCE` vs `ABSENCE`; thu hồi có lý do, detector raise lại nếu còn đúng.

**Transport seam đầy đủ cho HTTP**
Problem: `IngestController` bỏ key/headers, cùng một mapping outbox chạy trên Kafka
nhưng bị từ chối trên HTTP.
Expected: một mapping đọc được như nhau trên cả hai transport.

**Khai báo nạp được từ ngoài jar**
Problem: quan sát hệ mới phải rebuild service.
Expected: `correlation.declarations.dir` cho `ingestion/`, `workflows/`, `rules/`.

**N2 — ScenarioService ra khỏi observer** (ADR 0001)
Problem: correlation-service gọi `POST /api/orders` và bật/tắt fail-mode của
payment-service — observer phát business command lên hệ được quan sát, vi phạm I1.
Expected: capability demo giữ lại nhưng nằm ngoài observer; core không phụ thuộc ngược.
Kết quả: module `demo-scenarios` (port 8085), không phụ thuộc gì từ correlation-service
và ngược lại. Dashboard gọi thẳng qua `environment.demoUrl`.

**traceId đi qua mapping**
Problem: trace id đọc bằng tên cứng `traceId`, hệ để chỗ khác thì mất link Jaeger.
Expected: đọc qua `SourceMapping`, lưu thành cột.

**N2b — demo-scenarios chạy thật + dashboard**
Problem: N2 mới build jar, chưa chạy; dashboard chưa xác nhận.
Kết quả: `demo-scenarios` chạy trên 8085 (phải bổ sung `repackage` goal còn thiếu trong
pom), `GET /api/scenarios` và `POST /{id}/run` hoạt động, tạo đơn thật qua order-service.
`correlation-service/api/scenarios` trả 404 — endpoint đã rời observer. CORS cho
`http://localhost:4200` đúng. Dashboard build production thành công với `demoUrl`.

**N5 — workflowType là inference có thể cải thiện, không phải giá trị cố định lúc tạo**
Problem: `CorrelationService` chỉ gán `workflowType` trong nhánh `saga == null`. Đọc lệch
thứ tự (nhiều topic, nhiều thread) khiến saga bị tạo bởi sự kiện không phải start event,
nhận `UNRESOLVED` và không bao giờ được nâng lên — dù `WorkflowRegistry.resolve` trả đúng.
Hậu quả: false `SAGA_STUCK`, false `ORPHAN_EVENT`, bồi hoàn không đạt `COMPENSATED`.
Expected: `UNRESOLVED` → resolved thì persist; đã resolved thì không downgrade; mâu thuẫn
thì báo chứ không tự đổi.
Kết quả: `adoptWorkflowIfNowKnown` chỉ điền chỗ trống, log ERROR khi hai khai báo mâu
thuẫn; `statusOverWholeSequence` replay theo thời điểm sự kiện; absence claim được thu hồi
qua cơ chế sẵn có của ADR 0006. Live: `UNRESOLVED/IN_PROGRESS` → `ORDER_SAGA/COMPENSATED`,
`ORPHAN_EVENT` retracted, `OUT_OF_ORDER` giữ nguyên.

**N4 — Kiểm chứng live**
Problem: các thay đổi trên mới chỉ có bằng chứng unit test.
Expected: tái hiện trên Postgres/Docker thật đúng các case từng fail.
Kết quả (2026-09-06, v14): identity theo source, HTTP transport seam, workflow resolution
theo source, alias merge muộn, anomaly retraction, traceId — tất cả xác nhận. Chi tiết
lệnh và output trong lịch sử phiên.

**N6 — BLOCKER-1: bộ thực nghiệm KLTN chạy lại được trên build hiện tại**
Problem: từ V13 mọi `saga_id` lưu dưới dạng `source::rawId`, nhưng read API truyền thẳng
path variable vào `findById`. Caller cầm khoá do platform phát (dashboard) thì đúng; caller
cầm định danh do hệ nguồn phát thì 404 trên một run vẫn nằm trong DB. `evaluate.py` dùng
`sagaId` order-service trả về nên mất dấu execution ở cả `GET /api/sagas/{id}`,
`POST /rca/analyze/{id}` và `POST /api/admin/analyze/{id}`. Hậu quả: không tái lập được số
liệu thực nghiệm, và số đang có là của build 2026-05-23 — trước V10–V14 và trước
`EvidenceTimeline`.
Expected: sửa ở đúng boundary (API contract), không đụng identity model của ADR 0002,
không hard-code tiền tố, không fallback lookup mơ hồ.
Kết quả: `ExecutionLookup` phân giải xác định — khoá đầy đủ tra thẳng; id thô dựng tập khoá
ứng viên hữu hạn từ `SourceMappingRegistry.knownSourceNames()` cộng bước tra alias; nhập
nhằng thì trả 409 chứ không chọn bừa (I2/I4/I8 giữ nguyên). `evaluate.py` ghi thêm
`execution_id` để mỗi số đo truy được về đúng dòng trong `saga_instances`.
Đo lại trên clean DB + Flyway V1..V14 + Docker stack: 10/10 kịch bản `real`, 27 lượt,
0 lỗi runtime, RCA accuracy **85.2 %** lenient / 74.1 % strict. Ba lượt cùng điều kiện cho
kết quả giống hệt nhau. Số liệu và cách chạy lại: `docs/GROUND_TRUTH.md` §10; tổng hợp cho
luận văn: `docs/KLTN_STATUS.md`.

**N7 — F1: RCA dat duoc ket luan nhung khong noi ra**
Problem: kiem chung tren ba he saga doc lap (semotpan/saga-orchestration,
sylleryum/kafka-microservices-with-saga, uuhnaut69/saga-pattern-microservices) deu cho
cung mot ket qua: selector xep hang dung, gan STRONG_EVIDENCE cho mot ung vien, cong bo no
trong `candidates`, bao `rcaStatus = EVIDENCE_AVAILABLE` — nhung `RcaResult` luu ve van
`rootCause = null` va `ruleId = null`. `RcaService` ghim cung ca ba truong ve null tu khi
mo hinh score-free thay cho mo hinh single-primary cu (V5/V6). Chan doan da dat duoc, chi
la khong duoc phat bieu.
Expected: sua o dung boundary — noi da xep hang — khong ha nguong, khong them rule engine,
khong dung lai confidence score.
Ket qua: `RcaSelector.SelectionResult` mang them `primary`, doc tu dau danh sach da xep
hang va chi nhan `STRONG_EVIDENCE`; `RcaService` doc `selection.getPrimary()` thay cho ba
hang null, `primaryRootCauses` phan chieu `root_cause` dung nhu V5 dinh nghia. Ranking,
RULED_OUT, provenance va retraction giu nguyen; `confidence` van null.
`RcaPromotionTest` (13 test) chot ca hai chieu: ung vien manh duoc de cu; ung vien
RULED_OUT / SUPPORTING / WEAK thi khong; bang chung da thu hoi khong the thanh root cause.
Toan bo suite 370 test pass. Xac nhan truc tiep tren saga-pattern-microservices dang chay:
truoc han ack compensation R10 con WEAK -> khong de cu; sau han R10 STRONG ->
`rootCause = COMPENSATION_STEP_FAILURE`, `ruleId = R10`. Tai dung 4/4 execution khong doi.

**N8 — F2: replay cua observer bi quy cho he nguon**
Problem: `EXACT_DUPLICATE_EVENT` nam sau mot lan tra DB chi chay khi
`DeduplicationCache` truot — ma cache truot dung luc observer restart hoac reset offset.
Nen detector ban dung luc he nguon khong lam gi (doc lai topic -> bao duplicate delivery
cho moi execution tren do), va im dung luc he nguon that su gui hai lan (duplicate trong
cung mot lan chay bi cache nuot, khong bao gi ca). Sau khi F1 bat promotion, phat hien
gia nay thanh root cause: 3/4 saga tren saga-pattern-microservices bao
`DUPLICATE_DELIVERY (R13)`.
Expected: phan biet bang du lieu that co trong duong ingest, khong dung heuristic thoi
gian, khong ha do chinh xac cua dedup, khong che R13 trong ranking.
Ket qua: ban ghi tren transport duoc luu cung su kien. Kafka dat ten mot record bang
topic-partition-offset va `SagaEventConsumer` von da tinh no roi vut di; nay
`saga_events.transport_ref` (V15) giu lai. Doc lai = cung mot record -> khong bao gi;
cung eventId tren hai record khac nhau -> van bao, va hai ban ghi di kem lam bang chung.
`DeduplicationCache` nho record thay vi mot co boolean, nen duplicate that trong cung mot
lan chay khong con bi nuot. Khong doi ranking, khong doi F1 promotion. Transport khong co
dinh danh (HTTP, hang cu) -> giu nguyen hanh vi cu, khong doan.
`ObserverReplayTest` (12 test) + noi rong 2 assertion san co. Toan bo suite 382 test pass.
Xac nhan truc tiep: reset offset ve 0, doc lai ca 17 record -> 17 dong
"observer-side replay", van 17 su kien luu, **0** `EXACT_DUPLICATE_EVENT`, R13 bien mat
khoi moi saga; trang thai tai dung khong doi.

**N9 — R12: nhanh song song khong con sinh `INVALID_TRANSITION` gia** (`3fb7c5e`)
Problem: trang thai duoc dung lai theo thu tu record ve, nen hai nhanh khai bao la song
song bi so sanh nhu mot chuoi: nhanh den sau doc nhu mot buoc lui va bi bao
`INVALID_TRANSITION`. Do la tie-break cua observer hien ra nhu loi cua he nguon.
Expected: chi ket luan tren cap ma bang chung sap thu tu duoc; cap khong sap duoc thi im.
Ket qua: `EvidenceTimeline` sap thu tu theo dac ta workflow (`WorkflowSpec`), `SagaStatus`
tach UNKNOWN khoi INVALID. `OutOfOrderStatusTest` chay ca **120 hoan vi** cua mot lan chay
5 su kien va doi moi hoan vi cho cung status/startTime/endTime.

**N10 — R14 khong duoc de cu lam nguyen nhan: `ClaimSubject = OBSERVATION_PROCESS`** (`ed06917`)
Problem: `OUT_OF_ORDER` noi ve duong doc cua observer, khong phai ve he duoc quan sat.
Sau khi F1 bat promotion, no thang de cu va tro thanh "nguyen nhan" cua he nguon.
Expected: anomaly tu khai chu the ma no noi ve; chi claim ve he nguon moi duoc de cu.
Ket qua: `ClaimSubject`; R14 van sinh, van nam trong `candidates` voi `STRONG_EVIDENCE`
va thu hang cu — chi khong duoc de cu lam primary root cause. `PromotionAttributionTest`
(13 test). Doi ket luan tren **2/11 execution** cua he ngoai; **khong** xuat hien lan nao
trong 27 luot chinh thuc nen **khong doi con so headline**.

**N11 — Do lai bo thuc nghiem KLTN tren build hien tai** (`e5f8d60`)
Problem: `evaluate.py` con fallback loi thoi doc sai ket qua platform; ky vong cua SC09
con theo ngu nghia R14 cu, nay khong the thoa man theo thiet ke.
Ket qua: don fallback, sua ngu nghia SC09. Do lai: **85.2 % lenient**, 27 luot, 10 kich
ban `real`, 100 % live ingestion, 0 loi runtime, **hai luot tai lap trung khop hoan toan**.
`KLTN_STATUS.md` va `GROUND_TRUTH.md` cap nhat theo. Ba tap bang chung giu tach bach:
so chinh thuc, kiem chung `synthetic`, kiem chung lien he thong.

**N12 — FP-2: `STATE_REGRESSION` doi ban ghi truoc phai terminal** (`90e55e1`)
Problem: moi buoc tut hang tren `progressRank` deu bi coi la bang chung, ke ca giua hai
trang thai chua ket thuc. Nhung mot run dang chay tut hang mot cach hop le (nhanh stock
bao thanh cong sau khi nhanh payment day run sang `COMPENSATING`), va viec hai quan sat do
co dung canh nhau hay khong phu thuoc vao phan du lieu da ve. Vi la claim `OCCURRENCE` nen
khong thu hoi duoc: raise tren bang chung chua day du roi thang de cu thanh R04.
Expected: vi tu phai don dieu — bang chung ve them khong duoc rut lai ket luan.
Ket qua: `EvidenceTimeline#firstRegression` chi bao khi trang thai truoc la terminal.
`RegressionEvidenceCompletenessTest` (10 test); toan bo suite **421 test** pass. Kiem
chung: SC20 (regression that) **2/2 giu R04**, SC05 **2/2 chuyen sang R10
`COMPENSATION_STEP_FAILURE`** — log `results/eval-fp2-sc05-sc20-20260908-163642.log`.
Da do lai 27 luot chinh thuc (xem N13): con so khong doi.

**N13 — Do lai 27 luot chinh thuc tren build sau FP-2**
Problem: con so headline 85.2 % duoc do tren build **truoc** FP-2. Dataset khong co
`STATE_REGRESSION` nao nen du doan la khong doi, nhung do moi la du doan.
Ket qua (2026-09-20T12:21:28Z, build `90e55e1`, 421 test, DB dung lai tu rong, Flyway
V1..V15, consumer am): **trung khop tung chi so** voi luot bao cao — 85.2 % lenient va
strict, P/R/F1 0.889/0.800/0.842, TP/FP/FN/TN 16/2/4/7, anomaly 1.000/0.852/0.920, cung
per-class (`SERVICE_CRASH` van 0/0/0 support 4), cung split, cung nhom, cung bang baseline
(80.0/80.0/80.0/65.0 %). Chi TTI va do tre dao dong (51.08 s, 41.1/65.5 ms). Du doan da
duoc kiem chung, khong con la du doan. Artefact:
`results/evaluation_report_n13_post_fp2_20260920-122200.json`.

Phat hien kem theo, da ghi thanh G-10 / L-18: `docker compose down -v` xoa luon volume
Kafka, nen observer khoi dong khi chua co topic nao va consumer nhan
`partitions assigned: []` ma **khong tu phuc hoi** — 14 phut, 0 saga ingest, nhin tu ngoai
giong het mot luot 0 %. Quy trinh chay lai trong `GROUND_TRUTH.md` da duoc sua cho dung.

**N14 — N1 Scheduler HA: dung mot instance chay moi luot quet**
Problem: **8** job `@Scheduled` (khong phai 6 nhu ghi truoc day) khong co khoa phan tan.
Hai replica la nhan doi moi luot quet, ma cac luot quet deu la check-then-act tren cung
mot tap dong: `existsBySagaIdAndAnomalyType` doc "chua co finding" o ca hai ben roi ca hai
cung ghi. Tu ADR 0006 con mot dang te hon: mot luot raise lai dung cai finding ma luot kia
vua thu hoi, va ban thu hoi co ly do con ban raise lai thi khong. Ca hai deu khong phai su
that ve he duoc quan sat — do la concurrency cua chinh observer hien ra nhu loi cua nguoi
khac.
Expected: dung mot instance chay moi luot quet tai mot thoi diem.

Quyet dinh (N1 de ngo hai phuong an): **khong** dung ShedLock, **khong** tach profile.
Profile chi la rang buoc van hanh — sai cau hinh la race quay lai, khong dong duoc
invariant. ShedLock thi them phu thuoc ngoai cho mot co che ma ban than no du ngan de viet
va giai thich duoc.

Ket qua: `scheduler_lease` (V16) + `SweepLease`. Gianh khoa la **mot cau UPDATE co dieu
kien**, nen database quyet dinh ai thang va khong instance nao phai thoa thuan voi ai.
Ba tinh chat dang noi:
- **Khong co dong ho host nao di vao quyet dinh.** `lease_expires_at` vua duoc so sanh vua
  duoc tinh bang `EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)` ngay trong cau lenh. Host lech
  gio chi duoc luot ngan hon hoac dai hon, khong bao gio duoc them mot ban sao cua luot
  quet. Day chinh la I9 ap cho chinh observer.
- **Crash tu phuc hoi.** Khoa het han theo thoi gian nen mot holder bi giet giua chung
  khong khoa vinh vien; duong phuc hoi va duong binh thuong la cung mot cau lenh.
- **Tra khoa doi dung token.** Mot luot quet chay qua han khong the tra nham khoa cua
  instance da tiep quan.

Gioi han co y, da ghi trong javadoc: luot quet chay **lau hon** lease thi van co the bi
mot instance khac nhap vao. Lease dat rong hon nhieu lan thoi gian quet that; dong han nua
thi phai cho chinh cac luot quet mang token vao moi lan ghi — lon hon nhieu so voi cai
race no dong.

`@Scheduled` roi khoi ba service sang `ScheduledSweeps`, nen detector khong con biet gi ve
lich chay lan ve viec co bao nhieu instance; test goi thang `detectStuckSagas()` nhu cu,
khong phai sua mot test nao. `RetentionService` van la bean tuy chon: tat retention thi
luot purge khong chay va **cung khong ghi gi vao bang khoa**.
`SweepLeaseTest` (10 test, chay that tren H2 gom mot ca 8 luong tranh nhau) +
`ScheduledSweepsTest` (7 test). Toan bo suite **438 test**, 0 failure.

Kiem chung live (2026-09-20): V16 chay tren Postgres that, roi dung **hai instance
correlation-service** tren cung DB va cung Kafka. Bang `scheduler_lease` cho thay moi luot
quet do **dung mot instance** giu (`auto-analyze-anomalies` thuoc instance 2, sau con lai
thuoc instance 1). Dung `inventory-service`, tao 6 don -> 6 saga treo: **6 saga, dung 6
finding `SAGA_STUCK`, 0 cap (saga, loai) trung**, khong co bang khoa nao cho luot purge vi
retention dang tat. **Chua do** doi chung: cung tinh huong do tren build **truoc** N1 se
sinh bao nhieu ban trung — nen phat bieu dung la *"hai instance khong sinh finding trung"*,
chua phai *"da do duoc so ban trung ma khoa da ngan"*.

**N15 — N3 Multi-Kafka: so cluster la cau hinh, khong phai mot ban phat hanh**
Problem: mot `@KafkaListener`, mot `topic-pattern`, mot `bootstrap-servers`. Moi thu khac
ve viec gan mot he moi da la file tren dia; rieng **doc o dau** thi khong. Hai he tren hai
cluster la hinh dang binh thuong cua mot he thong that, va no lam tuyen bo zero-touch sai
o dung mot tang ben duoi cho tuyen bo duoc phat ra: "tich hop la ba file YAML" chi dung
chung nao he moi tinh co publish vao dung cluster ma ban build nay da tro toi.
Expected: khai bao nhieu nguon Kafka bang configuration, sinh listener container theo do.

Ket qua: `correlation.kafka.clusters[]` (`KafkaSourceProperties`) +
`KafkaClusterListeners` dung mot container cho moi cluster khai bao. Moi cluster co
consumer factory rieng (bootstrap servers, group, offset policy la chuyen rieng cua tung
cluster); moi thu phia sau van dung chung — mot `SagaEventConsumer`, mot duong ingest, mot
error handler. Cluster thu hai khong duoc phep tro thanh ban cai dat thu hai cua viec doc
quan sat.

**Phan quan trong hon, va la phan neu hong se hong im lang:** ten topic chi duy nhat
**trong pham vi mot cluster**. Hai cluster cung co `order-created` la chuyen binh thuong.
Neu cluster khong di den duoc source mapping thi platform doc ca hai bang mot mapping, don
ca hai he vao mot khong gian danh tinh, roi bao cao khac biet giua hai he khong lien quan
nhu loi cua mot trong hai — dung cai I4 sinh ra de chan, di vao bang mot cua khac. Nen:
`RawObservation.origin` mang ten cluster, `SourceMapping.cluster` cho mapping noi no noi
thay cluster nao, va `forTopic(origin, topic)` bo qua mapping thuoc cluster khac du pattern
co khop ten topic.

Mot quan sat **khong co origin** (HTTP post, hoac deployment doc mot cluster) chi khop
mapping **khong khai cluster**. De no roi xuong mapping da gan cluster khi do la mapping
duy nhat la mot phep doan — I8 cam — nen no nhan identity mapping.

Tuong thich nguoc: khong khai cluster nao thi van doc `spring.kafka.bootstrap-servers` voi
`correlation.kafka.topic-pattern`, record den **khong co origin**, va moi mapping dang co
(khong cai nao khai cluster) phan giai y nguyen nhu truoc. `RawObservation` giu nguyen dang
6 tham so nen khong mot call site cu nao phai sua.

Khai bao thieu truong, hoac hai cluster trung ten, bi **bo qua va noi ra** thay vi chay
bang mot gia tri tu bia: doc mot cluster duoi pattern tu doan se sinh finding ve mot he
khong ai yeu cau quan sat.

Mot quy tac nua, phat hien trong luc kiem chung live: mapping **co khai cluster** thang
mapping **khong khai**, bat ke thu tu file. Day khong phai tie-break thong thuong — giua
"mapping cho order-created tren retail-eu" va "mapping cho order-created o bat ky dau", chi
mot trong hai duoc viet ve ban ghi dang cam. De filename quyet dinh thi mot khai bao dung
va cu the co the bi mot khai bao chung chung sap truoc che mat, va trieu chung se la hai he
bi don im lang vao mot. Giua hai mapping cung gan mot cluster thi van la thu tu khai bao.

`MultiClusterIngestionTest` (4 test) + `ClusterScopedMappingTest` (9 test, gom mot ca
chinh no bat duoc loi: ban dau quan sat khong co origin van khop mapping da gan cluster).

Kiem chung live (2026-09-20): dung mot **Kafka cluster thu hai** (`kafka-second`, KRaft
rieng, khong lien quan gi den cluster demo), tao tren do dung topic **`order-created`** —
trung ten voi cluster demo. Khai bao hai cluster bang `SPRING_APPLICATION_JSON`, them mot
file `declarations/ingestion/second-estate-orders.yaml` co `cluster: second-estate`, khong
sua mot dong code nao. Ket qua: observer dung **hai listener container**, ca hai deu duoc
gan partition. Gui **cung mot ban ghi** (cung `orderRef: SHARED-ORDER-1`) vao `order-created`
cua ca hai cluster -> **hai execution tach biet**: `default::SHARED-ORDER-1` va
`second-estate::SHARED-ORDER-1`, khong bi gop. Do dung la ca ma I4 sinh ra de chan.

**F3 — Cau hoi hau-terminal duoc dat cho lan chay, khong cho lan den**
Problem: cung mot tap ba su kien, chi doi thu tu den, tap finding khac nhau — da tai hien:
- den [e1, e2, e3]: `FORBIDDEN_POST_TERMINAL`, `STATE_REGRESSION`, `MULTI_TERMINAL_CONFLICT`,
  `INVALID_TRANSITION`
- den [e1, e3, e2]: `STATE_REGRESSION`, `OUT_OF_ORDER`, `MULTI_TERMINAL_CONFLICT`,
  `INVALID_TRANSITION`
(`e1` ORDER_CREATED t0, `e2` INVENTORY_RESERVED t10 la terminal, `e3` PAYMENT_FAILED t20.)
`OUT_OF_ORDER` lech nhau la hop le — I9 mien tru dung no. `FORBIDDEN_POST_TERMINAL` bien mat
thi khong.
Nguyen nhan: `EvidenceTimeline` doc lap thu tu, nhung **cau hoi dat cho no thi khong**.
`endedBefore(event)` quet moi bearing terminal nhung chi doi chieu voi **mot ve phai duy
nhat** — su kien vua den. Nen su kien terminal den **sau cung** khong bao gio duoc dem ra
do voi nhung gi no da ket thuc. Cung hinh dang voi FP-2 (quyet dinh tren mot tien to roi
khong xem lai), di qua mot vi tu khac.

Ket qua: `endedBefore(SagaEvent)` -> `postTerminal()` khong tham so, quet **moi cap**
(bearing terminal x observation trong `ordered`) tren toan bo ban dung lai, tra record
`PostTerminal(terminal, endedAs, late)`. Cung khuon voi `regression()` da co — hoi ve lan
chay, khong hoi ve lan den. Van chi bao cap `relate(...) == BEFORE`, nen cap khong xac dinh
duoc thu tu van khong thanh finding. `statusAt()` thanh thua, da bo.

**Pham vi da chung minh, va chi den do.** Hai test hoan vi chot ca hai chieu: run co su kien
hau-terminal that thi **moi thu tu den deu raise**, run khong co thi **moi thu tu den deu im
lang**. Da xac nhan test fail tren code truoc ban sua, va no tai hien dung tung nhan cua
phan vi du tren. Them mot run co **ba cap** hau-terminal: 120/120 hoan vi deu raise.
**Khong duoc suy ra I9 da HOLDS toan bo** — cap evidence duoc trich dan van phu thuoc tien
to, do duoc 3 dang khac nhau tren 120 hoan vi. Phan con lai tach thanh **F9**.

**Khong doi mot con so nao cua bo chinh thuc**: `FORBIDDEN_POST_TERMINAL`,
`STATE_REGRESSION` va `MULTI_TERMINAL_CONFLICT` deu xuat hien **0 lan** trong 27 luot. Giong
FP-2: sua dung, khong doi headline — va dieu do tu no la mot phat bieu ve **ranh gioi cua
dataset**, khong phai ve bo detector.

---

## IN PROGRESS

*(trống)*

---

## NEXT

Sau dot audit doc lap 2026-09-20 (tren muc 58ea076). Moi muc duoi day **da duoc kiem chung
lai truc tiep**, khong phai chep lai nhan dinh cua nguoi review.

**F9 — Cap evidence ma `FORBIDDEN_POST_TERMINAL` trich dan con phu thuoc tien to — DA SUA 2026-09-22** · Priority: **cao**
Problem: tach ra tu F3. Sau F3, **viec finding co duoc raise hay khong** da doc lap thu tu
den. Nhung `detectForbiddenPostTerminal` giu finding dau tien cua loai do cho mot run roi
bo moi lan sau, nen **cap event duoc ghi vao evidence va `measuredAt`** la cap dau tien tro
nen chung minh duoc — mot thuoc tinh cua viec da nhan duoc bao nhieu phan cua run.
Da do, khong phai suy tu code: mot run co ba cap hau-terminal cho **120/120 hoan vi deu
raise finding**, nhung **3 dang `(evidenceEventIds, measuredAt)` khac nhau**
(`butWhichPairItCitesStillDependsOnHowMuchOfTheRunHadArrived`).
Expected: quyet dinh ro — hoac chuan hoa cap duoc trich dan theo business order cua toan bo
ban dung lai, hoac ghi vao dac ta rang finding chi cam ket ve su ton tai, khong ve cap cu
the. I9 phu ca hai nua; F3 moi dong nua dau.

**Nang len cao sau khi tra duong truyen:** day khong chi la chuyen trich dan. Cap evidence
di vao xep hang cua RCA:
`Anomaly.evidenceEventIds` -> `RcaService.evidence(Anomaly)` (`RcaService.java:919`) ->
`Evidence.eventIds()` -> `anchorCandidates` lay event **som nhat** (`RcaService.java:267`) ->
`setAnchorAt` + `setAnchorStepId` -> `RcaSelector.comparator`:
`statusRank -> anchorPosition -> anchorAt -> severity -> ruleId`.
`anchorPosition`/`anchorAt` dung **tren** severity, va R04 `TERMINAL_STATE_OVERWRITE`
(`STRONG_EVIDENCE`) an thang `FORBIDDEN_POST_TERMINAL`. Ba dang evidence da do co event som
nhat khac nhau, nen **anchor khac nhau**.
Phat bieu duoc giu, dung muc nay: *evidence doi theo thu tu giao va truyen toi thuoc tinh
dung de xep hang; co NGUY CO doi thu hang va de cu.* **Chua do duoc** thu hang co doi hay
khong — can mot luot co **hai ung vien canh tranh cung tang**, chua dung duoc ca do.
"Anchor khac" khong phai "thu hang khac". Co dinh thu tu giao chi kiem soat yeu to gay
nhieu, khong dong F9.

**Mot cho doc lech code, phat hien cung luc:** javadoc `RcaService.java:252` viet rang khi
evidence khong co event id thi "the anomaly's own measurement time still positions it in
time", nhung code la `if (earliest == null) continue;` — `measuredAt` **khong bao gio**
duoc dung de anchor. Hoac sua doc, hoac lam dung nhu doc noi.

**Ket qua F9 (2026-09-22):** giu cap dai dien do `EvidenceTimeline.postTerminal()` chon
tren toan bo reconstruction: terminal dau tien co cap hop le, roi observation dau tien
chung minh duoc la sau terminal do. Tie-break event id chi chon citation on dinh, khong
duoc dung de chung minh business order. Detector cap nhat finding dang dung khi citation
doi, giu ID va claim OCCURRENCE; description, observedValue, evidenceEventIds va measuredAt
cung doi. Khong doi schema, predicate, tier hay ranking.

`detectedAt` cua finding nay la thoi diem phat hien **ban evidence hien tai**: chi tang
khi citation doi, de auto-RCA san co nhan ra ket qua cu da stale, ke ca khi event den muon
hon cua so hai phut. No khong con giu thoi diem phat hien dau tien sau mot lan revision;
`measuredAt` van la business time. Cap khong doi va duplicate khong ghi lai finding.
Javadoc anchor RCA da sua dung theo code; khong them fallback tu measuredAt.

Kiem chung: test equality fail tren code cu voi **3** dang citation, pass sau sua voi
**1** dang `[e3, e4] @2026-01-01T00:00:03Z` tren **120/120 hoan vi**. Them 6 test cho
terminal den muon, observation chen giua, duplicate/no-op, timestamp bang nhau va tu choi
cap khong chung minh duoc, RCA anchor/ranking/primary, va auto-RCA sau revision.
Ca RCA giu evidence canh tranh R03 co dinh de co lap F9; R03/R04 deu STRONG, anchor R04
va ket qua de cu R03 on dinh tren 120 hoan vi. Day la test voi repository trong bo nho,
khong phai phep do live hay chung minh ACID/concurrent ingestion tren Postgres.

Tests: focused **62 pass**; full reactor `mvn test` **474 pass** (469 correlation-service
+ 5 saga-events), 0 failure/error/skipped; Python oracle **14 pass**; `git diff --check`
pass. L9 van in loi scheduler H2 thieu source_name. Khong do lai headline KLTN, khong doi
oracle. I9 chi duoc kiem chung cho citation F9 va duong RCA da co lap; phan con lai F11.

**F4 — R01 `CIRCUIT_BREAKER_OPEN` gan STRONG tu mot chuoi con trong log** · Priority: **cao**
Problem: `payload.contains("circuit breaker") || payload.contains("retry exhausted")` ->
`STRONG_EVIDENCE`, va chinh comment ngay canh thua nhan "breaker state itself was not
observed". Bang chung do cung lam chi dung duoc "retry da can", khong dung duoc ten claim.
Deu di thang vao promotion vi chi STRONG moi duoc de cu.
Expected: hoac ha xuong muc tuong xung voi bang chung, hoac doi ten claim ve dung thu quan
sat duoc. Anh huong SC06 trong bo chinh thuc, nen phai xu ly cung voi ground truth.

**DA DO 2026-09-21 — phat bieu phai doi.** Chay SC06 tren moi truong sach, co ghi
`raw_events` va `source_state` (trang thai breaker doc tu chinh payment-service):

| Luot | reason trong payload | Breaker that | CIRCUIT_BREAKER_OPEN |
|---|---|---|---|
| 1 | `... : Payment gateway unavailable (simulated failure)` | **CLOSED** ca luot | **SAI ve su kien** |
| 2 | `... : CircuitBreaker 'paymentProcessing' is OPEN...` | **CLOSED_TO_OPEN** trong luot | dung, co can cu |
| 3 | `... : CircuitBreaker 'paymentProcessing' is OPEN...` | **HALF_OPEN_TO_OPEN** trong luot | dung, co can cu |

Tien to `"Circuit breaker / retry exhausted: "` la hang so ghi o **moi** lan fallback; phan
phan biet duoc ba luot nam o **hau to** `t.getMessage()`, va R01 khong doc toi do. Nen ca
ba luot nhan cung mot tier STRONG voi lap luan giong het nhau: **dung hai lan vi ly do sai,
va sai mot lan.**

Nen **khong** phai "R01 luon ket luan qua bang chung" — luot 2 va 3 bang chung co du.

**Hinh dang ban sua, va hai cach sua hong phai tranh:**
- Ha toan bo R01 xuong WEAK -> sua qua tay, mat hai ket luan dung va co can cu.
- Doi sang `contains("OPEN")` tren toan payload -> van la do chuoi tren ca ban ghi, va
  hard-code cach dien dat cua **Resilience4j** thanh ngu nghia chung cho moi he Saga.

Ban sua dung: nhan dien **thong diep loi cu the**, o **truong do mapping xac dinh**, theo
**quy tac khai bao cho tung nguon** — cung nguyen tac I7 ma `SourceMapping` da ap cho
correlation. `"CircuitBreaker ... is OPEN and does not permit further calls"` la cach noi
cua mot thu vien Java cu the; he Go hay .NET se noi khac, core khong duoc biet truoc.
Khi hau to khong noi gi ve breaker, claim phai roi ve muc evidence do duoc ("retry da can
o buoc nay"), khong phai bien mat.

**Muc claim:** thong diep tu choi chi chung minh **breaker da chan loi goi ay**. No khong
chung minh **vi sao** breaker mo, cung khong chung minh breaker la nguyen nhan goc sau nhat
— o day no khong phai: breaker mo *vi* gateway da tu choi tu truoc. Neu van dung nhan
`CIRCUIT_BREAKER_OPEN` thi oracle phai dinh nghia no la **co che that bai truc tiep**.
Va ke ca vay, **ca ba luot van loi ve chat luong giai thich**: R01 vien dan cung mot chu ky
nhap nhang cho ca ba. Luot 2 va 3 la dung ket luan nhung sai lap luan.

**So sanh truoc/sau phai chay tren cung tap observation.** Ba tap `raw_events` da luu trong
artefact la du: chay build truoc va sau F4 tren nguyen ba tap do, cung spec, cung moc cham.
Chi sau do moi kiem chung live, va phai chon dut khoat mot trong hai thiet ke: (1) moi luot
doc lap, reset ve trang thai khoi dau **da xac minh**; hoac (2) giu trang thai, coi ca ba
luot la **mot** thi nghiem va lap lai ca chuoi. Co dinh thu tu thoi la **chua du** — trang
thai khoi dau va thoi gian cho van troi: luot 1 ngay 2026-09-21 gap `HALF_OPEN_TO_CLOSED`
xay ra **28 ms truoc** loi goi dau tien cua no.

**Mot phat hien kem theo, anh huong ca dataset:** ba luot SC06 **khong doc lap**. Cua so
truot cua breaker cong don qua warm-up va cac luot truoc, nen breaker mo tu luot 2. `n_runs`
dang duoc doc nhu N mau cua mot ca, trong khi day la **mot chuoi mang trang thai**. Dao thu
tu ba luot se cho ba nhan khac. Phai co dinh thu tu, hoac reset breaker giua cac luot, truoc
khi so sanh truoc/sau ban sua F4.

**Chuan bi replay F4 (2026-09-21, chua sua production R01):**
`oracle/replay_sc06.py` + `Sc06ReplayTest` chay lai 15 observation da luu qua
CorrelationService, detector inline va RCA that, repository trong bo nho. Bao toan payload,
hai moc thoi gian, metadata nhan dang/nhan qua; thu tu giao theo receivedAt, tu choi tie.
Khong doc source_state hay ket qua RCA cu de lam input. Khong phai kiem chung Kafka,
mapping ingestion, ACID hay scheduler. Ca SC06 da ket thuc, anomaly RETRY_BURST la inline.
Da doi chieu truoc F4: ca 3 luot khop status, anomaly, primary, thu tu ung vien/tier/anchor
voi artifact live. Log/report moi co hash input, resource va source thuc te (ke ca chua
commit); khong ghi de report cu. Huong dan: `oracle/REPLAY.txt`.

Oracle v2 sua cau noi sai cua v1: R02 TIMEOUT_RETRY_STORM doi evidence timeout, khong
dong nghia STEP_FAILED_AFTER_RETRY_EXHAUSTION. Claim can retry chua co nhan platform da
kiem chung tuong ung. scorer tach nhan la thanh UNSCORED (truoc day in "khong tinh" nhung
van cong vao sai), bao coverage va tu choi cham neu raw evidence khac fixture da khoa.
reasoning_defects trong YAML la ghi nhan lich su, khong tu dong danh gia build sau F4.
Full `mvn clean install`: 467 test / 46 lop, 0 failure/error/skipped; 7 test Python pass.
Loi scheduler H2 source_name cu van con (L9); suite xanh khong phai runtime sach loi.
F4 van mo; oracle van retrospective, reviewed_by=null. Khong doi invariant hay schema.

**F4 — DA SUA 2026-09-21, cho quyet dinh mot diem ve oracle**

Ket qua: `ruleCircuitBreakerOpen` -> `ruleDeclaredFailureSignal`, doc qua khai bao thay vi
do chuoi tren toan payload.

- `FailureSignal` — hai **pham tru** core hieu: `BREAKER_REJECTED_CALL`, `RETRY_EXHAUSTED`.
  Core biet pham tru; **cach dien dat** thuoc he nguon.
- `FailureSignalDeclaration` — khai bao theo tung source: truong nao mang reason, va chuoi
  nao bao hieu tung pham tru. Nguon **khong co khai bao thi khong sinh ung vien nao** —
  khong phai muc yeu hon, ma la khong co. Doan bua chinh la loi dang go bo (I8).
- `FailureSignalRegistry` — nap tu `failure-signals/*.yaml` canh cac khai bao khac. Hai file
  cung khai mot source thi **tu choi ca hai**, khong de thu tu file quyet dinh chan doan.
- `declarations/failure-signals/demo-order-saga.yaml` — noi duy nhat cach noi cua
  Resilience4j duoc phep ton tai.
- R01 giu `CIRCUIT_BREAKER_OPEN` nhung chi khi hau to bao **trang thai breaker**. Hau to
  khong noi gi ve breaker -> **R23 `RETRY_EXHAUSTED`**, claim dung muc evidence do duoc.
  Khong ha toan bo R01, khong `contains("OPEN")` tren toan payload.

**Do tren fixture dong bang** (`oracle/sc06-observations.json`, cung spec, cung moc cham):

| Luot | Truoc F4 | Sau F4 | Breaker that |
|---|---|---|---|
| 1 | `CIRCUIT_BREAKER_OPEN` STRONG | **`RETRY_EXHAUSTED`** STRONG | CLOSED |
| 2 | `CIRCUIT_BREAKER_OPEN` STRONG | `CIRCUIT_BREAKER_OPEN` STRONG | OPEN |
| 3 | `CIRCUIT_BREAKER_OPEN` STRONG | `CIRCUIT_BREAKER_OPEN` STRONG | OPEN |

Loi "ket luan khong duoc bao dam" o luot 1: **1 -> 0**.

**Phan giai thich cung doi**, khong chi nhan. Truoc: ba luot trich dan cung mot chu ky nhap
nhang. Sau: moi ung vien trich dan **dung mau khai bao da quyet dinh no**
(`Matched declared BREAKER_REJECTED_CALL wording for source 'default': "is OPEN and does
not permit further calls"`), va `statusReason` noi ro gioi han ("Why the breaker was open is
not established by this evidence").

**CHUA QUYET — mot diem thuoc ve oracle, khong phai code.** Bo cham hien bao:
`coverage oracle: 2/3; unknown labels: 1` — luot 1 bi loai khoi mau vi
`STEP_FAILED_AFTER_RETRY_EXHAUSTION` co `platform_labels: []`, va `RETRY_EXHAUSTED` la nhan
**moi**, chua ton tai luc oracle v2 duoc viet. Them cau noi do **phai la quyet dinh rieng**,
lap luan tu **dinh nghia cua R23**, khong tu viec no lam diem dep hon. Dung quy tac da chot:
khong dung ket qua ban sua de quyet dinh lai dap an cua chinh no.
Truoc khi co quyet dinh do, con so dung la **2/2 tren mau duoc cham, 1 luot chua cham**.

---

**F5 — DA SUA 2026-09-21**
Problem: `SAGA_STUCK && terminal && !MISSING_TERMINAL_EVENT && !STEP_TIMEOUT truoc do` ->
`STRONG_EVIDENCE`, ket luan `TRANSIENT_SERVICE_FAILURE` kem goi y "Service self-healed".
Toan bo bang chung la `SAGA_STUCK` — mot phat bieu ve **im lang quan sat duoc** (L-6) —
cong mot trang thai terminal den sau. **Ba the gioi** cho dung tap bang chung do: nguon tu
hoi phuc; nguon van khoe nhung observer nhan muon; co nguoi khoi dong lai.

Va the gioi thu ba chinh la cai da xay ra trong hai kich ban rule nay duoc viet cho: **SC04
va SC16 deu `docker stop` roi `docker start` bang tay**. Ket luan "self-healed" khong chi
la chua chung minh — no **sai**, va duoc phat bieu o tang danh cho bang chung da khep van de.

Cho lo ra: `ruleStuckUnknownCause` gap **cung mot im lang** tren run **chua** ket thuc va
tra loi `STUCK_UNKNOWN_CAUSE` o muc WEAK — dung. Mot trang thai terminal den sau noi rang
run da ket thuc; no khong noi gi ve **vi sao** co im lang. Mot quan sat them khong duoc phep
dua mot claim tu "khong ro nguyen nhan" thanh mot chan doan tu tin ve he nguon.

Ket qua: claim doi tu **he nguon** sang **execution** —
`TRANSIENT_SERVICE_FAILURE` -> **`RECOVERED_AFTER_SILENCE`**. Dieu kien guard giu nguyen.
Tang van la STRONG, vi claim hep hon nay **that su duoc bang chung khep lai**: im lang da
duoc do, ket thuc da duoc quan sat. No van duoc de cu, nen platform van noi duoc dieu co
ich — chi thoi khong goi ten mot nguyen nhan no khong nhin thay.
`statusReason` noi thang phan khong ket luan duoc: *"Whether the source recovered, was
restarted, or was reachable all along while its events arrived late is not established by
this evidence."*

**`TRANSIENT_SERVICE_FAILURE` bi bo han, khong phai ha tang.** Tang la phat bieu ve **do
chac**; van de o day la **chinh cai claim**, va mot phien ban de dat cua claim ma bang chung
khong voi toi van la claim do. Neu mot he nguon **tu bao** minh loi tam thoi, cho cua no la
mot pham tru `FailureSignal` duoc khai bao — dung duong F4 da mo: doc tu cai he thong noi
ra, khong suy tu cai no khong noi.

Test: `RcaPromotionTest.silenceFollowedByAnEndingClaimsNothingAboutTheSourcesHealth` —
assert **vang mat tren toan bo ket qua** (`SERVICE_FAILURE|SELF_HEAL|CRASH`) chu khong phai
mot nhan bi thay bang mot nhan khac, nen dua claim quay lai duoi ten moi **hoac** o tang
thap hon deu fail. Build: **468 test, 0 failure**.

**Anh huong len bo chinh thuc — la SO HOC, chua phai do.** Guard khong doi, chi nhan doi,
nen SC04 (2 luot, dang duoc chan doan dung bang R06) se doi sang `RECOVERED_AFTER_SILENCE`
va oracle **cu** cham sai ca hai: **23/27 -> 21/27**. SC16 (2 luot) truoc gio da sai, chi
doi nhan. **Chua co fixture raw evidence cho SC04/SC16** (chi SC06 co), nen con so nay phai
duoc **lan chay cuoi xac nhan**, khong duoc trich truoc.

Day la truong hop oracle cu doi mot claim ma bang chung khong do duoc, nen diem giam la
**dau hieu dung**, khong phai thoai lui. Phai bao ca hai oracle canh nhau.

**F6 — Nguon cua moc thoi gian khong di theo quan sat — DA SUA 2026-09-23** · Priority: trung binh-cao
Problem: `EventTimeResolver` phan biet PAYLOAD/TRANSPORT va tra ve `Resolved.source()`,
nhung `EventIngestionService` chi dung no de **ghi WARN** roi vut di; khong cot nao giu lai.
Downstream (`SAGA_STUCK`, `STEP_TIMEOUT`) do thoi gian ma khong biet minh dang do dong ho
nao. I3 doi moi gia tri suy luan mang nhan xuat xu.
Expected: giu nguon thoi gian cung su kien, nhu `CorrelationBasis` da lam cho tuong quan.

Ket qua: chuyen enum nguon thoi gian san co tu resolver ve domain `EventTimeSource`,
de persistence khong phu thuoc lop messaging. `DECLARED` thay ten `PAYLOAD` vi mapping
da ho tro event time trong header/key, khong chi payload; `TRANSPORT` la fallback.
`Resolved` -> `IngestedEvent` -> `SagaEvent.eventTimeSource` -> event trong read API,
cung transaction voi timestamp/payload. Admin injection cung ghi nguon ro rang.

V17 them cot nullable `event_time_source` va CHECK chi chap nhan DECLARED/TRANSPORT
hoac NULL. Khong backfill, khong dat default: du lieu cu va constructor cu khong khai
nguon van NULL (khong biet), ke ca khi payload co timestamp hoac timestamp = receivedAt.
Nguon thoi gian khong chung minh dong ho he nguon chinh xac. Can apply V17 truoc khi
chay binary moi; binary cu van ghi duoc sau migration vi cot moi nullable.

Kiem chung 2026-09-22, chot task 2026-09-23: **46 focused test pass**, them **15 test**
(12 persistence/provenance, 2 admin, 1 migration). Full reactor `mvn test`: **489 pass**
(484 correlation-service + 5 saga-events), 0 failure/error/skipped; Python oracle
**14 pass**; `git diff --check` pass. Tren PostgreSQL 16-alpine rieng: Flyway apply
**V1..V17**, Hibernate `validate` thanh cong, **12/12 test persistence pass**, gom rollback
event/nguon thoi gian cung nhau, retry sau rollback, replay khong doi nguon, va CHECK
tu choi gia tri khong hop le. Test upgrade row cu/old writer chay migration V17 tren H2.
Log: `correlation-service/target/f6-focused.log`, `f6-postgres.log`, `f6-full-suite.log`.

Pham vi I3: da giu va cong bo nguon timestamp, khong thay thuat toan timeout, business
order, tier hay RCA. Chua khang dinh moi phep do downstream da phan biet mixed clocks;
phan do ghi F12. Khong doi oracle/headline KLTN. L9 van in loi scheduler H2 source_name.

**F7 — Khai bao workflow xung dot thi giu cai dau — DA SUA 2026-09-23** · Priority: trung binh
Problem: execution da resolved gap bang chung tro toi workflow khac -> log ERROR, **giu
nguyen cai cu**. Comment lap luan day la loi khai bao chu khong phai bat thuong cua he
nguon, va no noi to ra — nhung lan chay van tiep tuc duoc do bang thuoc co the sai, dung
canh ma I8 canh bao ("mot dac ta sai khong loi ra ngoai").
Expected: quyet dinh ro — tu choi ket luan cho execution do, hay giu nguyen va danh dau moi
finding sinh ra tu no la dang ngo.

Ket qua: giu `workflowType` ban dau de truy vet (ADR 0004), nhung luu tap ten
`workflowConflicts` sap xep, khong trung lap. Khi co bat dong duong tinh giua workflow
da resolve va evidence moi, hoac giua source-scoped spec va mapping ngay event dau,
observer giu observation nhung ngung suy ket cuc/detector theo workflow. Mapping
khong ro hay event khong nhan dien khong tu no la bang chung xung dot.

Alias merge hop nhat co xung dot va huy terminal/endTime cu ngay trong transaction,
ke ca observation noi alias la replay va bo qua projection. Co khong tu mat khi event
sau lai khop workflow cu. Trang thai luu dung `IN_PROGRESS` trung lap nhu unresolved;
API tra them `workflowConflicts`, dashboard hien `UNDETERMINED`, khong khang dinh
execution dang chay hoac da ket thuc. RCA abstain `INSUFFICIENT_EVIDENCE`, khong alert;
read API khong phat lai RCA cu nhu ket luan hien tai. Anomaly cu van con de audit,
duoc gan canh bao xung dot o read response; khong xoa hay rut lai OCCURRENCE (ADR 0006).
Dashboard khong dem cac chan doan/anomaly do nhu bang chung hien tai, va khong tinh
execution xung dot vao cac bo dem trang thai (ke ca active); van tinh vao tong execution.

V18 them `workflow_conflicts JSONB NOT NULL DEFAULT '[]'` voi CHECK array rong hoac
it nhat hai phan tu. Row cu/old writer nhan [] = chua ghi nhan conflict, KHONG co nghia
da chung minh nhat quan; khong backfill tu payload. Apply migration truoc binary moi.
Writer cu van tuong thich schema, nhung binary/client cu khong hieu co moi: can nang
cap dong bo observer va dashboard de co semantics F7, khong coi mixed-version la an toan.
Chua co API xoa co hay re-evaluate sau sua declaration; khong tu dong chon lai spec.

Kiem chung: them **12 regression test Java**, gom hai delivery order, late conflict
sau terminal, source/mapping bat dong, alias merge, reload/replay, rollback/retry,
concurrent stale writer (optimistic lock san co), API/RCA abstention va scheduled sweep.
**69 focused pass**, **501 full reactor pass**, **14 Python oracle pass**; dashboard
production build pass (van canh bao CSS budget cua saga-detail, khong sua CSS trong F7).
PostgreSQL 16-alpine rieng: Flyway **V1..V18**, Hibernate validate, **15 persistence test
pass**; migration test tren TEMP table xac nhan row cu, old writer, CHECK tu choi
singleton/non-array, chap nhan tap conflict. Hai script
`src/test/resources/workflow-conflicts-postgres-{before,after}.sql` bao quanh V18,
cung connection, rollback sau test. Container tam da xoa. Khong doi schema DB dang chay.
Log trong `correlation-service/target/f7-{focused,postgres,full-suite,dashboard}.log`.
I3/I8: khong dung spec tranh chap lam tham quyen; I5/I6: conflict va observation cung
commit/rollback; I9: tap conflict va refusal khong phu thuoc thu tu den.
Khong thay tier/oracle/ADR. L9 (scheduler H2 source_name) van la issue da ghi.

**F8 — Giai thich cho 14.8% thieu la chua du — PHAN GIAI THICH DA SUA 2026-09-28** · Priority: trung binh-cao
`docs/KLTN_STATUS.md` L-1 con khang dinh cutoff la nguyen nhan cua **toan bo** 14.8%, mau thuan L-23
ngay trong file. Da dinh chinh cau do va tro sang L-23; khong doi so lieu (`docs/` bi gitignore — file chi
o local, khong nam trong commit). Cutoff chua sua (neu sua phai
xu ly ca muc bang chung R05, nhu duoi).
Problem: tai lieu quy toan bo phan thieu ve cutoff lam `MISSING_TERMINAL_EVENT` khong fire.
Nhung R05 `SERVICE_CRASH` duoc gan `SUPPORTING_EVIDENCE`, ma `RcaSelector.promote` **chi de
cu STRONG**. Nen sua cutoff co the vot duoc chi so lenient (nhan co mat trong tap ung vien)
nhung **khong** dua `SERVICE_CRASH` len primary root cause — strict van sai. Hai nguyen nhan
doc lap, tai lieu moi ke mot.
Dinh chinh (sau khi doc lai `evaluate.py`): **ca hai chi so deu cham tren thu da duoc de
cu**, khong tren tap ung vien — `rca_expectation_matches` nhan `rca_root_cause` va
`rca_primary_roots`, `selected_root_cause` da bo fallback xuong candidate list, ung vien
SUPPORTING nam rieng o `rca_evidence_roots`. Nen sua cutoff **khong vot duoc luot nao, o ca
lenient lan strict**. Va R06 doi `!MISSING_TERMINAL_EVENT`, nen cutoff dung con **lam mat**
ung vien STRONG duy nhat dang duoc de cu o SC16.
Expected: sua lai phan giai thich; neu sua cutoff thi phai xu ly ca muc bang chung cua R05,
khong chi cutoff.

**F13 — Saga FAILED do buoc tu bao that bai bi ghi la "completed successfully" — DA SUA 2026-09-27** · Priority: **cao**
Problem (chay live he khach san saga-orchestration, khong sua code nguon, 2026-09-21):
`ROOM_REJECTED` chi mang `{"status":"REJECTED"}`. R01/R23 khong co truong ly do de doc,
khong luat nao doc event `failed` da khai bao, va `determineOutcome` doi "FAILED khong
anomaly khong ung vien" thanh `NORMAL` — ket qua ghi "Saga completed successfully with 2
events" cho mot run o trang thai FAILED. Tai hien tren 2 run that.
Da sua: R24 `STEP_REPORTED_FAILURE` (muc execution-state, doi xung R11 cho run FAILED khong
bu tru) chi chay khi R01/R23 khong doc duoc gi, de khong tranh promotion voi chung tren
cung mot event; outcome cua run FAILED luon la `FAILED`. Giu nguyen quyet dinh cua selector:
FAILED khong co bang chung van la `NO_DIAGNOSIS_NEEDED`. Test:
`DeclaredStepFailureTest` (4 ca). SC06 replay: nhan de cu khong doi (R23, R01, R01).
Chua lam: oracle chua co claim cho R24 (se ra UNSCORED neu mot ca co nhan gap no); khong
tu them vao `claims.yaml` vi tu vung da khoa 2026-09-21 — can quyet dinh rieng.

---

**S1 — Do lai tap `synthetic` (17 kich ban inject) tren build hien tai — 2026-09-28/29** · Priority: cao
Lan dau do lai ca tap tu L-10 (KLTN_STATUS). A/B voi build `93ac939` (truoc L1-L3/L5): **17/17 giong het**,
10/17 dung (58.8 %) — thay doi L1/L2/L3/L5 khong doi ket qua. 7 kich ban sai la co tu truoc; da truy tung ca:
- **F16 — SC13**: event sau terminal bi ghi hai claim (FORBIDDEN_POST_TERMINAL va INVALID_TRANSITION), R12
  thang nho vi tri buoc. Da sua `judgeTransitions` bo cap co event sau ket thuc (`EvidenceTimeline.isAfterEnding`,
  cung chung minh voi `postTerminal()`). Test 24 hoan vi + sua 4 test cu (L21/L22: ket luan do thi giu o
  `WorkflowSpecTest`, tang run gio la FORBIDDEN_POST_TERMINAL). Do live: SC13 0/2 -> 2/2, 16 kich ban khac khong doi.
- **F18 — SC21**: sau F16, R24 lai trich PAYMENT_FAILED xay ra sau khi run da COMPLETED va thang cung cach.
  R24 va R01/R23 bo qua event FAILED sau ket thuc. Test `CompensationAnchorTest` (fail tren code cu).
- **F17 — SC23, SC24**: R10/R11 khong trich event nao -> khong co anchor -> thua moi ung vien co anchor bat ke
  severity. R10 trich yeu cau bu tru chua duoc xac nhan; R11 trich event FAILED cua buoc thuan. Test 2 (fail
  tren code cu). Tap 27 luot chinh thuc: R10/R11/R12 chi xuat hien o SC02 va la ung vien duy nhat -> suy ra
  headline khong doi (tu tap ung vien da ghi, chua do live).
- **Harness — SC14, SC26**: `evaluate.py` dat traceId trong payload; tu ed06917 detector khong mo payload nua,
  `inject-event` doc truong `traceId` rieng -> khong event nao co trace, MISSING_TRACE_CONTEXT khong the bat.
  Da sua harness gui traceId o truong rieng (loi do luong, khong phai loi platform).
- **SC10 — (DA CHOT o S2)** nhan loi thoi, khong sua platform: ORDER_CREATED -> INVENTORY_RESERVED (thieu payment) theo thiet
  ke hien tai la THIEU QUAN SAT, khong phai chuyen buoc sai (`aMissingIntermediateObservationRaisesNoInvalidTransition`).
  Ky vong INVALID_TRANSITION cua kich ban mau thuan voi quyet dinh do. Can quyet dinh rieng: doi kich ban (vd.
  mot buoc di lui that) hay giu va ghi ro. Khong tu doi nhan (khong lam dep so).
- **SC24 — (DA CHOT o S2, con F20)** du lieu kich ban tu chua mot STEP_TIMEOUT that, khong sua platform: sau F17, R11 neo dung o
  INVENTORY_FAILED, nhung ORDER_CREATED (T-80s) -> PAYMENT_COMPLETED (T-60s) la 20s > 10s timeout cua
  PROCESS_PAYMENT, nen R07 neo som hon va thang. Platform bao dung su that co trong du lieu; khoang tre do
  khong phai y do cua D5. Can quyet dinh rieng co sua timestamp kich ban hay khong (khong tu sua).
Ket qua sau F16-F18 + sua harness: **15/17 (88.2 %)**, con SC10 va SC24 (ca hai la van de kich ban o tren).
Artefact: `results/evaluation_report_synthetic_{baseline_93ac939_20260928,post_l3_20260928,post_f16_20260929,post_f18_20260929}.json`.
Tap synthetic van KHONG gop vao headline (chi kiem ngu nghia luat).
Tap `real` do live sau F16-F18 (2026-09-29, `results/evaluation_report_real_post_f18_20260929.json`): **74.1 %
lenient / 74.1 % strict, tung kich ban trung khop** `evaluation_report_settled_20260921.json` (SC04/16/17 0 %,
SC06 66.7 %, con lai 100 %). Du doan "headline khong doi" da duoc do, khong con la suy luan.

**S2 — Chot SC10 va SC24 (tap synthetic) — 2026-09-30** · Priority: cao
Nguyen tac: khong doi nhan cho khop dau ra; sua INPUT de kich ban kiem dung dieu ten no noi.
- **SC10**: input cu (ORDER_CREATED -> INVENTORY_RESERVED, payment vang) la thieu quan sat theo I10, khong
  phai chuyen buoc sai. Nhan giu INVALID_TRANSITION; input doi thanh chuyen buoc bi cam that: sau
  PAYMENT_COMPLETED, run phat ORDER_CREATED moi (eventId rieng) — quay lui ve buoc dau. Do live: **2/2
  strict** (R12 la ung vien STRONG duy nhat). Kem SAGA_STUCK vi run dung o IN_PROGRESS (dung, khong phai loi).
  Lan chay dau thieu `eventId: USE_UNIQUE` -> harness sinh trung id -> EXACT_DUPLICATE_EVENT: loi harness,
  da sua.
- **SC24**: timestamp cu co khoang 20s cho PROCESS_PAYMENT (timeout 10s) — mot STEP_TIMEOUT kich ban khong co
  y. Nhan giu FORWARD_STEP_FAILURE; ORDER_CREATED -80s -> -64s, PAYMENT_REFUNDED -5s -> -20s; bo STEP_TIMEOUT
  khoi expected_anomalies. Do live: **van 0/2** — con STEP_TIMEOUT, nhung nay la loi platform F20, khong con
  la loi kich ban.
Artefact: `results/evaluation_report_synthetic_sc10_rewritten_20260930.json`,
`results/evaluation_report_synthetic_sc24_retimed_20260930.json`. Moi truong: stack build tu code sau F19;
inventory-service chay o cong host 18083 (8083 bi container khac chiem), token ingest cuc bo.
Tap synthetic du kien 16/17 sau S2 (SC10 da do, 15 kich ban con lai chua do lai tren build nay).

**F20 — STEP_TIMEOUT (legacy adjacency) do mot buoc tu event TRUOC no toi event BAT DAU cua chinh no — DA SUA 2026-09-30** · Priority: **cao**
Da sua (cau hinh, khong them abstraction): `step-timeout-mode` mac dinh SHADOW -> **PROJECTION** (application.yml +
DetectionProperties). Projection do buoc tu event bat dau da quan sat; khong co thi suy tu buoc truoc va ghi
nhan nguon (INFERRED_FROM_PREDECESSOR). SHADOW/LEGACY van con de doi chieu.
Bang chung truoc khi doi: `StepTimeoutModeOnRecordedRunsTest` chay hai cach do tren **27/27 luot real** da ghi
(oracle/evidence): 10 luot co timeout, **0 luot lech** (khong luot nao quan sat event started) -> headline khong
doi theo phep do nay. Test: `StepTimeoutDetectionTest` +3 (ca SC24: legacy bao REFUND_PAYMENT, projection
khong; buoc bu tru cham that van bao voi start=OBSERVED_START; mac dinh la PROJECTION). Full suite **578 pass**.
Live SC24 sau sua: khong con STEP_TIMEOUT; nguyen nhan cong bo doi sang DELAYED_COMPENSATION (xem F21).
Artefact `results/evaluation_report_synthetic_sc24_post_f20_20260930.json`.
Problem (SC24 live 2026-09-30, sau S2): "Step 'REFUND_PAYMENT' took 35529ms. Previous event: INVENTORY_FAILED".
REFUND_PAYMENT la event `started` da khai bao cua buoc; buoc thuc chay REFUND_PAYMENT -> PAYMENT_REFUNDED =
5.5s. Khoang 35s la thoi gian TRUOC khi buoc bat dau — dung la cai DELAYED_COMPENSATION do — nen cung mot
khoang bi dem hai lan, va R07 (HIGH) thang R11 cung anchor nho severity. Log SHADOW xac nhan:
`legacy=REFUND_PAYMENT=35529ms(>10000ms) projection=no-finding`. O SHADOW, legacy la authoritative.
Expected: mot buoc khong the qua han truoc khi bat dau khi event started cua no DA duoc quan sat. Huong sua
can quyet dinh: chuyen authoritative sang PROJECTION (anh huong R07 o SC27/28/29 tap real — phai do lai
headline) hoac sua adjacency. Khong tu sua trong S2.

**S3 — Do lai tap synthetic sau F19/F20/S2; sua thoi gian settle cua harness — 2026-09-30** · Priority: cao
Do live toan tap synthetic (18 kich ban, 36 luot; build sau F20, settle cu 35s): **32/36 (88.9 %) lenient = strict**,
16/18 kich ban dung. Moi: SC10 2/2 (S2). Sai: SC24 0/2 (F21), SC23 0/2.
SC23 **khong phai hoi quy platform**: harness doc luot o 42.2s sau event cuoi, MISSING_COMPENSATION_ACK fire o
43.0s (han 30s + chu ky quet 15s -> roi trong 30–45s); ket luan cuoi cua platform mot giay sau la R10 (dung, log
06:10:55). Cac lan truoc SC23 dung/sai tuy pha scheduler. Goc: `DETECTOR_SETTLE_SECONDS=35` tinh "mot chu ky
luot quet cham nhat", thieu han chot dung truoc no. Da sua: **50s** = han dai nhat 30s + chu ky 15s + bien, van
dong deu moi kich ban (giu nguyen tac chong thien lech precision cua trigger_rca). MISSING_TERMINAL_EVENT
(10x timeout) co y khong phu; kich ban can no tu cho rieng.
Artefact: `results/evaluation_report_synthetic_post_f20_20260930.json` (settle 35s). Can do lai hai tap voi
settle 50s truoc khi trich so.

**F21 — Nhan dinh ve khoang thoi gian hoa anchor voi event mo khoang; severity quyet dinh** · Priority: trung binh
Problem (SC24 live sau F20): R22 DELAYED_COMPENSATION trich khoang INVENTORY_FAILED -> REFUND_PAYMENT; anchor =
event som nhat da trich = INVENTORY_FAILED, trung anchor voi R11 FORWARD_STEP_FAILURE. Vi tri va anchorAt hoa,
severity MEDIUM (R22) > LOW (R11) -> cong bo DELAYED_COMPENSATION. Nhan kich ban la FORWARD_STEP_FAILURE (theo ghi
chu taxonomy: R11 la nguyen nhan, R22 la dac trung do tre). Ca hai nhan dinh deu dung voi du lieu.
Cau hoi thiet ke can quyet dinh (khong tu doi): (a) anchor cua nhan dinh khoang la dau khoang (nhu R07 hien nay)
hay diem vuot nguong / buoc bi tre; (b) mot nhan dinh ma bang chung bao trum bang chung cua nhan dinh khac co nen
xep sau khong. Khong doi nhan cho khop. SC24 van 0/2 strict.
Bang chung de can nhac (bao cao da ghi, post-F18): tap real 0 luot co hai ung vien STRONG hoa anchor; synthetic
4 luot, severity pha hoa DUNG ca 4 (SC21: R03 CRITICAL > R04 HIGH; SC23: R10 HIGH > R13 LOW). Moi thay doi quy tac
pha hoa phai giu SC21/SC23 va khong dua vao mot ca duy nhat (SC24).

**F22 — Buoc bat dau truoc khi buoc truoc no hoan thanh khong duoc phat hien** · Priority: trung binh
Problem (probe offline khi chot SC10, 2026-09-30): ORDER_CREATED(t0) -> INVENTORY_STARTED(t1) ->
PAYMENT_COMPLETED(t2), cach nhau 1s theo dong ho nguon (> clockSkewTolerance 500ms): khong finding nao, run
IN_PROGRESS, INSUFFICIENT_EVIDENCE. Tuong tu: PAYMENT_STARTED sau INVENTORY_STARTED. Ly do:
`CorrelationService.judgeTransitions` chi xet cap co event truoc la COMPLETED/FAILED (dung: event started chua
ban giao gi), nen cap started(downstream) -> completed(upstream) khong bao gio duoc xet; va CREATE_ORDER ->
RESERVE_INVENTORY duoc doc la thieu quan sat (I10, dung).
Can can nhac truoc khi lam: (1) hai dau la dong ho cua hai service — I9 va clockSkewTolerance; (2) nhanh song
song (N9) khong duoc bi bao; (3) day la nang luc phat hien moi — chi lam khi co use case that (mot he quan sat
duoc sinh thu tu nay), khong phai vi probe tim ra. Khong co trong 27 luot real va 17 kich ban synthetic.

**E1 — Ablation tang chon nguyen nhan (RcaSelector), offline — 2026-09-30** · Priority: cao
Muc dich: tra loi "bo quyet dinh X cua selector thi ket luan nao sai". `ablate_selection.py`
giu co dinh tap ung vien engine da sinh trong tung luot (`rca_candidates`), chi thay MOT quyet dinh,
cham bang chinh `rca_expectation_matches(_strict)` tren ca cac luot ky vong khong ket luan (ma
`baseline_comparison` bo qua). Bien the `engine` tai hien 27/27 real va 34/34 synthetic — kiem tra
cho viec suy ClaimSubject tu ruleId (chi R14 la OBSERVATION_PROCESS trong code).
Ket qua strict (real 27 luot / synthetic 34 luot), engine 74.1 % / 88.2 %:
- Bo guard "chi de cu STRONG": real **-2** (SC03 x2: stuck khong ro nguyen nhan bi goi ten
  STUCK_UNKNOWN_CAUSE thay vi INSUFFICIENT_EVIDENCE). SC17 doi ket luan nhung sai ca hai phia.
- Bo guard ClaimSubject: synthetic **-2** (SC09 x2: OUT_OF_ORDER_DELIVERY duoc cong bo la nguyen nhan).
- Cong bo moi ung vien STRONG thay vi mot nguyen nhan chinh: synthetic strict **-6** (SC20/21/23),
  lenient +2 (SC24) — vi du cua viec lenient thuong cho cong bo nhieu nhan.
- Xep theo thoi diem anchor thay vi vi tri buoc: synthetic **-2** (SC20).
- Xep theo severity / rule id: **0** tren ca hai tap. Tren real, xep hang khong phan biet duoc (moi
  luot chi co <=1 ung vien STRONG dung ve y nghia, hoac ca hai lua chon deu sai: SC04/SC16).
Gioi han: chi tang selection, tap ung vien la dau ra cua rule/detector va duoc giu co dinh; so hoc tren
dau vao da ghi, khong phai do live; synthetic khong gop vao headline. Artefact:
`results/ablation_selection_e1.json`.

**E2 — Ablation cac co che correctness (namespace, bat bien thu tu, rut lai finding) — 2026-09-30** · Priority: cao
`CorrectnessAblationTest`: cap dau vao doi chung qua CorrelationService + detector inline + RcaService +
RcaSelector that (repo in-memory, nhu `Sc06ReplayTest`). Co che bi go o DAU VAO hoac o bien repository,
khong them co bat/tat vao production.
- **A. Namespace identity.** Hai run healthy `ORD-1` cua `shop-a` va `shop-b`: doi chung -> 2 execution,
  0 anomaly, NO_DIAGNOSIS_NEEDED ca hai. Go namespace (gan chung mot source) -> 1 execution,
  FORBIDDEN_POST_TERMINAL, nguyen nhan cong bo **TERMINAL_STATE_OVERWRITE** cho hai run khong co loi.
- **B. Bat bien thu tu giao.** Moi hoan vi cua SC01/SC02/SC06/SC27 that (252 thu tu): chan doan
  (trang thai run + nguyen nhan cong bo) **khong doi o ca 4 kich ban**. R14 bat o 122 thu tu; go guard
  ClaimSubject tren cung dau ra -> nguyen nhan doi o **122/122** (OUT_OF_ORDER_DELIVERY thay cho
  RETRY_EXHAUSTED o SC06). Phat hien: `rcaStatus` KHONG bat bien o SC27 (xem F19).
- **C. Rut lai absence claim.** Run doc ngoai thu tu (event dau khong khai bao, bi goi stuck, roi
  ORDER_CREATED toi va resolve workflow): doi chung rut 2 finding (SAGA_STUCK, ORPHAN_EVENT), khong cong bo
  nguyen nhan. `retractClaims` no-op -> cong bo **WRONG_SAGA_CORRELATION_KEY** (R15) va them R06 tu SAGA_STUCK
  da bi bac bo.
Gioi han: khong chay sweep theo dong ho (R07/SAGA_STUCK tu timeout khong co trong B — SC27 o day ra
COMPLETED/null, khac live); kich ban A va C dung dung, khong lay tu luot live; thang correlation
(TRACE_CONTEXT/ALIAS_LINK) chua ablate — con lai la E3. Full suite **568 pass**.

**F19 — `rcaStatus` phu thuoc thu tu giao khi chi co ung vien OBSERVATION_PROCESS — DA SUA 2026-09-30** · Priority: trung binh-cao
Da sua: `RcaSelector.resolveStatus` hoi cung cau voi `promote` — chi ung vien STRONG/SUPPORTING co subject
OBSERVED_SYSTEM la bang chung; anomaly chi duoc gat ra khi MOI ung vien trich no deu la OBSERVATION_PROCESS
(anomaly khong ai trich, hoac duoc ung vien ve he nguon trich, van tinh). R14 giu STRONG va vi tri trong
candidates. Run ket thuc chi co R14 -> NO_DIAGNOSIS_NEEDED; run dang chay -> INSUFFICIENT_EVIDENCE.
Theo I9 (ngoai le cho `OUT_OF_ORDER` chi ap cho chinh finding, khong cho trang thai chan doan).
Test: `RcaSelectorTest` +4 (fail tren code cu), `CorrectnessAblationTest` B them assert rcaStatus bat bien
(252 thu tu: 0 kich ban lech). Doi ky vong co chu dich cua N10/L18 trong `PromotionAttributionTest` va
`AlertContentTest` (EVIDENCE_AVAILABLE -> INSUFFICIENT/NO_DIAGNOSIS), ghi ly do trong Javadoc test.
Anh huong so lieu: tap real khong co luot nao co R14; synthetic chi SC09 va co R12 STRONG -> headline va
15/17 khong doi (suy tu tap ung vien da ghi, chua do live). Doi trang thai tren cac run he ngoai kieu N10.
Full suite **572 pass**.
Problem (E2-B, SC27): cung tap event, 4/6 thu tu giao cho EVIDENCE_AVAILABLE, 2/6 cho NO_DIAGNOSIS_NEEDED;
nguyen nhan cong bo null o ca hai. `RcaSelector.resolveStatus` dem R14 STRONG la "actionable evidence" du
`promote` bo qua no vi subject. Ket qua: run COMPLETED khong loi bi bao "co bang chung" nhung khong ten
nguyen nhan; scorer strict se cham sai tuy thu tu. Cung thay o E2-C (doi chung).
Expected: `resolveStatus` va `promote` dung chung dinh nghia ve dieu gi la bang chung ve he duoc quan sat;
co regression test hoan vi. Chua sua (ngoai scope E2).

**E3 — Ablation thang correlation (PROPAGATED_CONTEXT / ALIAS_LINK) — 2026-09-30** · Priority: trung binh
`CorrelationLadderAblationTest`: raw observation qua `EventIngestionService` that (source mapping, ladder,
alias resolver) roi `ReplayPipeline` (correlation + detector + RCA that; harness tach tu E2 de dung chung).
Bac bi go dung nhu mot he dang onboard thieu no: khong truyen trace, hoac mapping khong khai bao id thu hai.
- **A. PROPAGATED_CONTEXT.** He khong co business id, chi truyen W3C trace: doi chung 3/3 observation
  ACCEPTED, 1 execution COMPLETED, NO_DIAGNOSIS_NEEDED. Bo trace -> **3/3 NOT_CORRELATABLE, 0 execution**:
  platform tu choi thay vi doan. Bac nay quyet dinh he do quan sat duoc hay khong.
- **B. ALIAS_LINK.** Order/inventory khoa theo `orderNo`, payment theo `paymentRef`, event payment mang ca
  hai: doi chung 1 execution COMPLETED, khong chan doan. Bo khai bao `aliasKey` -> 2 manh; manh payment
  IN_PROGRESS voi ORPHAN_EVENT, cong bo **WRONG_SAGA_CORRELATION_KEY**; manh order COMPLETED khong finding
  (buoc payment vang mat duoc doc la thieu quan sat, khong phai loi — dung I10, nhung la mot run "healthy"
  co mot buoc khong ai thay).
- **C. ALIAS_LINK den muon -> fold (bo sung 2026-09-30).** Payment bao PAYMENT_STARTED chi voi `paymentRef`,
  `orderNo` toi o PAYMENT_COMPLETED: hai manh da ton tai -> merge. Doi chung: 1 execution COMPLETED, ORPHAN_EVENT
  cua manh bi rut (retracted=1), khong cong bo nguyen nhan. Bo alias -> 2 manh, manh payment bi cong bo
  WRONG_SAGA_CORRELATION_KEY. Fold nhung tat rut lai -> run DAY DU bi cong bo **WRONG_SAGA_CORRELATION_KEY** tu
  mot nhan dinh lam tren manh.
- **D. Gioi han cua bac trace (bo sung 2026-09-30).** Hai run khoi cung luc duoi mot trace (mot request khoi hai
  saga), khong co khoa khai bao: gop thanh **1 execution COMPLETED, khong chan doan**; dau vet duy nhat la
  OUT_OF_ORDER (subject OBSERVATION_PROCESS, khong duoc de cu). Gop sai gan nhu im lang. Co khoa khai bao -> 2
  execution dung. Ghi nhan la gioi han, khong phai loi: I2 cho phep trace la bang chung; can noi ro trong KLTN.
Gioi han: kich ban dung, khong lay tu luot live. Full suite **580 pass**.

## NEXT (con lai)

*(trong — N1 va N3 da xong; nhung muc khac nam o LATER)*

---

## LATER

**L17 — Auto-RCA bo qua run ket thuc ma khong co anomaly — DA SUA 2026-09-27** · Priority: trung binh-cao
Da sua: sweep rieng `autoAnalyzeEndedRuns` (lease rieng, cung chu ky 30s) phan tich run
FAILED/COMPENSATED chua co RCA nao moi hon observation nhan gan nhat. So tren `received_at`
(dong ho platform), khong phai `last_updated` (dong ho nguon): run doc tu backlog — nhu run
hotel dau tien, stamp 16:03, nhan 16:05 — se lot mot cua so tren `last_updated`. COMPLETED
khong nam trong sweep nay; phan tich giu nguyen (FAILED khong bang chung van NO_DIAGNOSIS_NEEDED).
Test: `SagaIdsAwaitingAnalysisTest` (H2 va Postgres 15 that voi Flyway + validate),
`EndedRunAutoAnalysisTest`, `ScheduledSweepsTest`. Live: R24 va R11 tu co RCA ~10s sau khi
run ket thuc, khong goi tay; 3 chu ky sau khong phan tich lai.
Luu y: query duyet moi run FAILED/COMPENSATED moi chu ky (anti-join theo index co san), chi
phi tang theo lich su khi retention tat. Lan dau chay tren DB cu se phan tich bu cac run cu
theo lo 500 — dashboard (dem RCA theo created_at 24h) tang mot lan.
Van de goc ben duoi giu lai de doi chieu.
Problem (live F13, 2026-09-27): `autoAnalyzeNewlyDetectedAnomalies` chi quet saga co anomaly moi
trong 2 phut. Run FAILED/COMPENSATED khong co anomaly (ROOM_REJECTED, SC02-kieu bu tru) khong bao
gio duoc phan tich tu dong: R24 va R11 chi hien khi goi `POST /rca/analyze`. Da thay tren run
guest 21: FAILED > 5 phut, khong co RCA nao cho den khi goi tay. Dashboard dem RCA vi vay thieu.
Expected: xac dinh co can kich hoat RCA khi run dat trang thai ket thuc hay khong; neu co thi xet
lai quyet dinh "business rejection khong can chan doan" cung luc, khong va rieng mot duong.

**L21 — Workflow co `validEventTypes` im lang voi event started/retry/mot so failed — DA SUA 2026-09-28** · Priority: trung binh-cao
Da sua: `getExpectedStepForEvent` hoi map truoc; map khong nhac toi event thi hoi `events`/
`outcomes` cua buoc. Noi map co noi, map van quyet dinh (khong spec nao dang co map va buoc bat
dong). Test: `WorkflowSpecTest` (2), `DetectorEvidenceTest` (2: SAGA_STUCK sau PAYMENT_STARTED do
bang `workflow:PROCESS_PAYMENT.timeout`; PAYMENT_FAILED roi INVENTORY_STARTED -> INVALID_TRANSITION);
ba test loi fail truoc khi sua. Full suite sach: 523/523.
Danh gia: replay 27 run settled (correlation + detector inline + sweep `detectStepTimeouts` + RCA),
code cu (L16+L22) so voi moi: **khong run nao khac**. Sweep that su chay (STEP_TIMEOUT o SC04/16/
27/28/29). Ly do: he demo khong phat `*_STARTED`; chi SC06 co event ngoai map (`PAYMENT_RETRY`),
khoang cach lon nhat 4.1s < 10s cua PROCESS_PAYMENT, va ORDER_CREATED -> PAYMENT_RETRY hop le.
SAGA_STUCK khong replay duoc (phu thuoc dong ho hien tai) — ket luan khong doi cho no la suy tu
cung khoang cach 4.1s, chua do.
Problem (phat hien khi sua L16): map la tap con cua `events:`. ORDER_SAGA thieu PAYMENT_STARTED,
INVENTORY_STARTED, PAYMENT_RETRY, ORDER_FAILED, REFUND_FAILED; FULFILLMENT/LOAN/PARALLEL_CHECKOUT
tuong tu. Voi cac event do: SAGA_STUCK dung nguong mac dinh thay vi nguong buoc, adjacency
STEP_TIMEOUT va judgeTransitions bo qua. Vi du: run im sau PAYMENT_STARTED bi do bang 60s, khong
phai 10s cua PROCESS_PAYMENT.
Expected: bo map (hoac chi dung khi buoc khong noi gi) — doi hanh vi ORDER_SAGA, nen phai chay lai
danh gia va bao ca hai bo so. Khong gop vao L16.

**F14 — INVALID_TRANSITION ghi trung cung mot cap — DA SUA 2026-09-28** · Priority: trung binh-cao
Da sua: `detectInvalidTransition` khong ghi khi da co finding dung cung observedValue va event trich
dan. Test: `anInvalidHandoffIsFiledOnceHoweverOftenItIsReJudged` (120 thu tu den; fail truoc khi sua).
Full suite: 527/527. Bo danh gia khong co run nao co INVALID_TRANSITION (replay F11), nen so khong doi.
Problem (phat hien khi sua F11): `judgeTransitions` xet lai moi cap lien ke o moi event den, va
`detectInvalidTransition` khong kiem tra finding da co. Run e1..e5 cua F9 co hai finding
`RESERVE_INVENTORY -> PROCESS_PAYMENT on SUCCESS` cung trich [e4]. Dem anomaly tren dashboard bi
thoi phong; evidence RCA lap lai.
Expected: moi cap (buoc truoc, buoc sau, dieu kien, event) chi mot finding dung; lap lai la khong ghi.

**F15 — R05 tru dong ho platform cho timestamp nguon; R06/durationMs khong kem nguon dong ho — DA SUA 2026-09-28 (tru durationMs API)** · Priority: trung binh
Da sua phan R05: khoang im lang do tren dong ho platform ca hai dau (`detectedAt` - `receivedAt` cuoi
cung), nhu SAGA_STUCK; khong con event nao nghe duoc truoc moc do thi khong do (khong ve `lastUpdated`,
dong ho nguon). Test: `SilenceClockTest` (nguon cham 10 phut khong con thoi phong; nguon nhanh 10 phut
khong con che mat — ca hai fail truoc khi sua). `AlertContentTest` sua fixture: receivedAt mac dinh la
`Instant.now()` luc chay test. Full suite: 532/532. Bo settled: khong run nao co MISSING_TERMINAL_EVENT nen
R05 khong du dieu kien; tren he demo hai cong thuc lech ~10ms.
Phan con lai DA SUA 2026-09-28: ba dong "Total duration" cua RCA (R06, R11, nhanh NORMAL) kem
`clocks=` tu nguon timestamp cua event o hai moc; cac event cung moc ma khac nguon -> unrecorded, khong
chon mot (khong phu thuoc thu tu). Test: `theRecoveredRunsTotalDurationSaysWhichClocksItSpans` (fail
truoc khi sua). Full suite: 539/539. Chua lam: `durationMs` trong API — them truong vao DTO la doi API,
chua co ai can.
Problem (phat hien khi sua F12): `ruleServiceCrash` tinh `stuckSeconds` = `detectedAt` (Instant.now()
cua platform luc detector chay) tru timestamp event lon nhat (dong ho nguon) — hai dong ho khac nhau
trong mot phep tru, lech dong ho nguon di thang vao so giay "stuck". R06 va `SagaInstance.durationMs`
in khoang thoi gian khong nhan nguon.
Expected: do khoang im lang tren cung dong ho (receivedAt, nhu SAGA_STUCK da lam); gan nhan cho R06.

**L25 — Dashboard dem ca finding da bi rut lai — DA SUA 2026-09-28** · Priority: trung binh
Da sua: `summarize` loc `retractedAt == null` truoc moi phep dem, giong cach RCA doc. Khong hien so da
rut lai (chua co ai can). Test: `aWithdrawnFindingIsNotCountedAsAnAnomaly` (fail truoc khi sua).
Full suite: 535/535.
Problem (phat hien khi sua L23): `DashboardMetricsService.summarize` dung `findByDetectedAtAfter`, khong
loc `retractedAt`, nen `totalAnomalies`, `topAnomalyTypes`, `topAffectedSteps` dem ca nhung claim chinh
platform da noi la sai. RCA thi chi doc `findStandingBySagaId`.
Expected: dashboard chi dem finding con dung; neu can, hien rieng so da rut lai.

**L26 — Cham timeline vo hinh voi moi he tru demo — DA SUA 2026-09-28** · Priority: trung binh
Problem (phat hien khi ra soat CSS L12): `.timeline-dot` khong co mau nen; chi `dot-order/payment/
inventory-service` (ten service demo) co mau, icon ben trong mau trang. Do tren dashboard that voi saga
hotel: ca 6 cham `background rgba(0,0,0,0)`, icon `rgb(255,255,255)` — khong thay icon nao.
Da sua: mau nen trung tinh mac dinh cho `.timeline-dot`; mau demo van de len (khai bao sau). Do lai:
6/6 cham `rgb(96,125,139)`; badge trang thai (global tu L12) dung mau o ca list va detail.

**L27 — Trang chi tiet ghi "Step: UNKNOWN" du platform biet buoc — DA SUA 2026-09-28** · Priority: trung binh
Problem (live hotel): moi event hien `Step: UNKNOWN` — placeholder ingestion luu khi nguon khong gui
stepName — trong khi detector da biet buoc qua workflow (L16).
Da sua: `SagaDetailResponse.workflowSteps` (id event -> buoc workflow; trong khi workflow xung dot thi
rong), them truong, khong doi truong cu; ban ghi luu khong bi viet lai. Dashboard uu tien buoc workflow,
khong co thi hien gia tri nguon gui. Test: `SagaDetailStepsTest` (2). Full suite: 545/545; `ng build` pass.
Live: 6 event hien BOOK_ROOM/TAKE_PAYMENT/RELEASE_ROOM.

**L24 — Validator khong bao khi `validEventTypes` bat dong voi `events` cua buoc — DA SUA 2026-09-28** · Priority: thap
Da sua: `WorkflowSpecValidator` canh bao khi map dat event vao buoc khac buoc dang khai bao no (ghi ro
map thang). Canh bao, khong loi: spec van dung duoc. Khong spec nao dang ship bi canh bao. Test:
`validEventTypesDisagreeingWithTheDeclaringStepIsAWarning` (fail truoc khi sua). Full suite: 537/537.
Problem (sau L21): hai nguon cung noi event thuoc buoc nao; bat dong thi map thang, im lang.
`WorkflowSpecValidator` chi canh bao buoc khong ton tai va event khong buoc nao khai bao. Hien
khong spec nao bat dong (da doi chieu 9 spec), nen chua co use case.
Expected: canh bao khi viet mot spec moi bat dong; hoac bo han map neu thay no khong con tac dung.

**L22 — Outcome FAILED duoc chap nhan toi moi dich co canh khai bao, ke ca canh chi cho SUCCESS — DA SUA 2026-09-28** · Priority: trung binh-cao
Nguyen nhan: nhanh FAILED co tu 9b64062 (khi con `isValidTransition` tra boolean, chua xu ly
canh khong dieu kien). Sau khi `unconditionedTargets` duoc them, truong hop comment nhac toi da
duoc lo o duoi; nhanh cu chi con tac dung sai. Da sua: bo nhanh. Test: `WorkflowSpecTest` (3:
FAILED khong di canh chi-SUCCESS -> INVALID; FAILED van di canh khong dieu kien va vao bu tru ->
VALID), `DetectorEvidenceTest` (ROOM_REJECTED roi TAKE_PAYMENT_REQUESTED tren ROOM_RESERVATION
that -> INVALID_TRANSITION "BOOK_ROOM -> TAKE_PAYMENT on FAILED"). Hai test loi fail truoc khi sua.
Full suite sach: 520/520.
Danh gia: replay 27 run cua `evaluation_report_settled_20260921.json` qua pipeline that (co che
Sc06ReplayTest: correlation + detector inline + RCA; INVALID_TRANSITION la detector inline) voi
code cu va moi: **khong run nao khac**, 0 INVALID_TRANSITION ca hai. Bo danh gia khong co run nao
co buoc FAILED roi di tiep, nen so lieu khong doi; khong gioi han cho SAGA_STUCK/STEP_TIMEOUT
(sweep) vi L22 khong cham chung.
Problem (phat hien khi viet test L16): `WorkflowSpec.explainTransition`, nhanh
`FAILED_OUTCOME.equals(condition)`, tra VALID neu co bat ky canh `from -> to`, khong xet
`t.getCondition() == null` du comment noi "declared without a condition". Nen BOOK_ROOM (FAILED) ->
TAKE_PAYMENT duoc coi la hop le du chi co canh SUCCESS; tuong tu PROCESS_PAYMENT (FAILED) ->
RESERVE_INVENTORY o ORDER_SAGA.
Expected: tai hien bang test, sua theo dung comment, chay lai danh gia (co the doi so INVALID_TRANSITION).

**L23 — `topAffectedSteps` cua dashboard doan buoc bang regex — DA SUA 2026-09-28** · Priority: thap
Rong hon ghi chu ban dau: voi INVALID_TRANSITION, MULTI_TERMINAL_CONFLICT va cac finding bu tru,
`thresholdSource` la `workflow:<TEN_WORKFLOW>.<...>`, nen dashboard dem **ten workflow** nhu mot buoc.
Da sua: buoc chi lay tu `workflow:<BUOC>.timeout` (khi BUOC co that) hoac tu buoc chung cua cac event
finding trich dan (qua workflow cua saga); trai nhieu buoc hoac khong xac dinh -> khong dem. Bo regex.
Test: `DashboardMetricsServiceTest`. Full suite: 534/534 (ca ApplicationContextTest). Live hotel:
`TAKE_PAYMENT` thay cho `TAKE_PAYMENT_REQUESTED`.
Problem (live F13): `DashboardMetricsService.inferStep` lay ten trong dau nhay cua description khi
thresholdSource khong phai `workflow:` — OUT_OF_ORDER cho ra "TAKE_PAYMENT_REQUESTED" (ten event).
Expected: lay buoc tu evidence event qua workflow, hoac khong dem khi khong xac dinh duoc.

**L19 — Khai bao cua cac he khac chua co test doi chieu mapping <-> workflow — DA SUA 2026-09-28** · Priority: trung binh
Da sua: `DeclaredVocabularyTest` doc moi file `declarations/ingestion`, xac dinh workflow qua
`workflow:` cua mapping hoac `sources:` cua workflow, va doi moi ten event mapping sinh ra phai duoc
workflow do nhan. Doc file truc tiep, khong them API cho `SourceMappingRegistry`. Hien kms va spm deu
khop (test giu nguyen trang thai dung); mutation doi ten mot event spm -> test chi dung ten va file.
Ngoai pham vi: probe khong co workflow, va hotel (workflow khong khai `sources`, da co L13).
Full suite: 536/536.
Problem (phat hien khi sua L13): `declarations/` con kms-order-saga, saga-pattern-order,
partner-order, second-estate. Khong test nao nap cac file nay va kiem tra ten event ma mapping
sinh ra co nam trong workflow cua he do; doi ten o mot ben chi lo ra khi chay live.
`SourceMappingRegistry` khong co API liet ke mapping, nen mot test tong quat can them API hoac
tu doc file.
Expected: lam theo mau L13 cho tung he co mot use case chay that; khong them API chi cho test
neu doc file truc tiep du.

**L20 — Workflow trong declarations/ khong ghi de duoc ban dong goi cung ten** · Priority: thap
Problem (phat hien khi sua L13): `WorkflowRegistry.register` giu ban nap truoc khi trung ten
(classpath truoc, declarations sau), chi log ERROR. Chinh co che nay lam test L13 fail khi
`target/test-classes` con ban cu. Trong jar hien chi co ORDER_SAGA, nen chi anh huong neu ai do
dat ORDER_SAGA vao declarations/ de sua.
Expected: quyet dinh thu tu uu tien co chu y va ghi ro; chua co use case -> chua lam.

**L18 — Moi lan tu choi nghiep vu deu sinh alert — DA DIEU TRA 2026-09-27; sua mot loi that, giu nguyen chinh sach** · Priority: trung binh
Tim thay khi dieu tra (tai hien live, saga hotel `cf99…`): alert noi nhieu hon ket qua no
thong bao. Ket qua luu `primaryRootCauses=[]` (R14 la OBSERVATION_PROCESS, khong promote),
nhung alert cung luc ghi `primaryRootCauses=['OUT_OF_ORDER_DELIVERY']` — vi `emitAlert` liet
ke moi candidate STRONG/SUPPORTING/WEAK, bo qua ClaimSubject. Da sua: alert lay dung
`primaryRootCauses` cua ket qua; chi phat khi co bang chung STRONG/SUPPORTING ve he duoc quan
sat (cung nguong EVIDENCE_AVAILABLE cua selector). Run chi co bang chung ve qua trinh quan sat
khong con alert; run ket SUPPORTING (R05) van alert nhung khong goi SERVICE_CRASH la nguyen
nhan chinh. Test: `AlertContentTest` (4 ca; go dieu kien moi thi ca live fail). Live sau khi
build lai: `cf99…` khong alert; R24/R11 alert voi dung nhan da luu.
Chinh sach "tu choi nghiep vu co alert khong": **co y khong doi.** R24/R11 la claim ve he duoc
quan sat, da promote; loc chung can mot khai niem "muc claim" trong code (hien chi co trong
oracle), ma chua co thanh phan nao doc alert trong repo de lam use case — khong dat dieu kien
tao abstraction cua AGENTS.md. Alert da mang `severity` (R11 LOW, R24 MEDIUM) de ben doc loc.
Mo lai khi co mot consumer alert that.
Van de goc ben duoi giu lai de doi chieu.
Problem (sau L17, live 2026-09-27): `emitAlert` phat voi moi `EVIDENCE_AVAILABLE`. R24 va R11 la
claim muc execution-state, khong khang dinh loi he thong, nhung nay chay tu dong nen moi phong
het cho / the bi tu choi deu ghi mot dong vao alerts-realtime.log (da thay: R11 LOW, R24 MEDIUM).
Truoc L17 dieu nay cung xay ra, nhung chi khi goi tay.
Expected: quyet dinh chinh sach alert theo muc claim (execution-state vs mechanism/cause), gan
voi cau hoi "business rejection co can chan doan khong". Khong tu loc trong code.

**L13 — Ban workflow ROOM_RESERVATION trong test resources da lech — DA SUA 2026-09-27** · Priority: trung binh
Da sua: xoa ba ban copy (`src/test/resources/workflows/room-reservation-workflow.yaml`,
`ingestion/hotel-saga-{room-booking,payment}.yaml`). `OutboxPatternIngestionTest` va hai test
"shipped" cua `MultiMappingSystemIdentityTest` nap thang `declarations/` — dung file observer
chay. Test tu vung nay lay ten event tu chinh cac mapping (4 topic) thay vi liet ke cung; hai ten
map nhung khong the xay ra (`REFUND_PAYMENT_REQUESTED`, `PAYMENT_REVERSED` — `goBack()` cua upstream
chi huy buoc truoc, payment la buoc cuoi) duoc ghim rieng kem ly do. Mutation: bo
`started: BOOK_ROOM_REQUESTED` khoi declarations -> test fail. Full suite sach (mvn clean): 511/511.
Luu y: `mvn test` khong `clean` sau khi keo thay doi nay van thay ban cu trong
`target/test-classes` va test tu vung se fail — can `clean` mot lan.
Problem (phat hien khi sua F13, 2026-09-27): `src/test/resources/workflows/room-reservation-workflow.yaml`
khong co `BOOK_ROOM_REQUESTED` (start events [ROOM_BOOKED, ROOM_REJECTED]); ban
`declarations/workflows/` ma observer live nap co them event `started` do. Test doc ROOM_RESERVATION
dang kiem mot workflow khac voi ban chay that.
Expected: mot nguon duy nhat cho test va runtime, hoac test nap tu `declarations/` nhu Sc06ReplayTest.

**L14 — Payload luu nguyen, khong che truong nhay cam — DA SUA 2026-09-28** · Priority: trung binh
Da sua: mapping khai bao `redact: [duong.dan, ...]` theo tung nguon (zero-touch); payload **luu tru**
thay gia tri bang `[redacted]`, sau khi moi truong nhan dang da doc tu ban goc. Khong co gi de che ->
giu nguyen tung byte. Khai bao cho hai topic lenh cua hotel (`creditCardNo`). Test: `RedactionTest`
(4; mutation bo khai bao -> test dau-cuoi fail). Full suite: 543/543. Live: BOOK_ROOM_REQUESTED luu
`"creditCardNo": "[redacted]"`, khong event nao con so the.
Gioi han: duong dan chi di qua JSON object, khong vao payload nhung dang chuoi; ban ghi cu trong DB khong
duoc viet lai (khong sua DB truc tiep) — can mot migration/job rieng neu muon xoa du lieu cu.
Bo sung 2026-09-28: log ERROR khi bo cuoc mot record (`KafkaErrorHandlingConfig`) in nguyen `record.value()`
— so the vao log. Nay che theo mapping cua topic truoc khi in; replay tay khong mat gi vi observer cung
khong luu gia tri do. Test: `KafkaErrorHandlingConfigTest`.
Da sua 2026-09-28: `EventIngestionService` ghi `e.getMessage()` khi payload khong doc duoc, va Jackson 2.15
mac dinh trich noi dung nguon vao thong bao (tai hien: so the nam trong ly do tu choi). Nay lan parse do
tat `INCLUDE_SOURCE_IN_LOCATION` (chi lan parse nay, khong doi mapper toan cuc). Test:
`anUnreadablePayloadIsNotQuotedInTheRejection` (fail truoc khi sua). Full suite: 547/547.
Problem (chay live F13): `TAKE_PAYMENT_REQUESTED` cua he khach san mang `creditCardNo`; observer luu
nguyen vao `saga_events.payload` va tra ra qua `/api/sagas/{id}`. O day la so the test cua upstream.
Expected: co phuong an che truong khai bao theo source truoc khi quan sat he that.

**L15 — Goi y cua R14 trai voi chinh Javadoc cua no — DA SUA 2026-09-28** · Priority: thap
Da sua: goi y cua R14 noi ve qua trinh doc (thu tu observer doc; Kafka khong sap thu tu giua cac
topic; doc run theo event time) thay vi partition key cua he nguon. Oracle chi cham nhan de cu, R14
khong bao gio duoc de cu (OBSERVATION_PROCESS), nen so khong doi. Test: `RcaPromotionTest`
`outOfOrderDeliveryAdvisesAboutTheReadingNotTheSource` (fail truoc khi sua). Full suite: 533/533.
Problem: R14 da danh dau `OBSERVATION_PROCESS` (thu tu la do observer doc nhieu topic), nhung suggestion
van la "Check Kafka partition key assignment" — huong nguoi doc ve phia he nguon. Tai hien live F13
khi replay 4 topic tu offset 0.
Expected: suggestion noi ve qua trinh quan sat, khong ve partition key cua he nguon.

**L16 — Event he khach san vao voi step=UNKNOWN; SAGA_STUCK dung nguong 60s — DA SUA 2026-09-27** · Priority: trung binh
Nguyen nhan goc (khong phai stepName): `WorkflowSpec.getExpectedStepForEvent` chi tra map
`validEventTypes` — mot khai bao thu hai lap lai `events:` cua cac buoc. ROOM_RESERVATION va
PARTNER_ORDER_SAGA khong viet map nay, nen voi chung: SAGA_STUCK dung nguong mac dinh 60s,
STEP_TIMEOUT kieu adjacency (authoritative o SHADOW) khong bao gio fire, `judgeTransitions` bo qua
moi transition, `thresholdSource` ghi nguong mac dinh.
Da sua: khong co map -> suy buoc tu `events`/`outcomes` cua buoc (`classifyEvent`); co map -> map
van quyet dinh (khong doi hanh vi ORDER_SAGA va cac workflow co map; da doi chieu: khong cho nao
map va buoc mau thuan). Test: `WorkflowSpecTest` (2), `DetectorEvidenceTest` (3, tren ROOM_RESERVATION
doc tu declarations/); mutation tra ve ham cu -> dung 3 test fail. Full suite sach: 516/516.
Live: SAGA_STUCK `timeout: 30s`, `workflow:TAKE_PAYMENT.timeout`, phat hien sau 45s (truoc ~75s);
sau khi bat lai payment, STEP_TIMEOUT fire (TAKE_PAYMENT 69613ms > 30000ms) va RCA promote R07
DOWNSTREAM_TIMEOUT thay cho R06 — cung dang voi SC27/28 cua ORDER_SAGA; ten R07 noi qua bang chung
o ca nay (payment bi dung, khong cham) la F10, khong phai L16.
Giu nguyen: `stepName="UNKNOWN"`/`serviceName="unknown"` ma ingestion dien khi nguon khong gui —
khong detector nao doc chung (buoc lay qua workflow), va SagaEventConsumerTest ghim hop dong nay.
Mot he qua nho: `ExecutionProjectionService.serviceOf` khong roi ve service khai bao vi "unknown"
khac null; `Operation.serviceName` hien khong co ai doc.
Problem (live F13): moi event ingest cua `hotel-saga` co `stepName=UNKNOWN`, `serviceName=unknown`;
RCA van dat anchor dung buoc nho workflow. SAGA_STUCK bao `timeout: 60s` (mac dinh) trong khi buoc
khai bao 30s. **Chua kiem chung** hai viec co cung nguyen nhan.
Expected: xac dinh nguong SAGA_STUCK lay tu dau; neu phai lay timeout cua buoc thi doc qua workflow,
khong doi he nguon them stepName.

**L12 — Dashboard saga-detail vuot CSS warning budget — DA RA SOAT 2026-09-28, con vuot 0.68 kB** · Priority: **thap**
Da lam: bo 14 rule khong dung (status ung vien khong con trong `CandidateStatus`: SELECTED,
POSSIBLE_BUT_UNSUPPORTED, CONTRIBUTING; `rca-status-selected` tu truoc V6; primary-cause/cause-name/
multi-cause-note/status-detail/candidate-suggestions/candidate-missing khong con trong HTML); dua badge
trang thai saga (dinh nghia trung o saga-list va saga-detail, cung mau) len `styles.css`; bo selector
`.cstatus-strong_evidence` lap. Kiem cac class ghep dong (`status-*`, `cstatus-*`, `candidate-*`, `dot-*`,
`rca-status-*`) truoc khi bo. 7.67 kB -> 6.68 kB; `ng build` pass. Phan con lai la style dang dung;
khong nang budget chi de tat canh bao (dung Expected). Cac mau `dot-order/payment/inventory-service`
gan cung ten service demo — he khac khong co mau rieng; chua doi vi la doi giao dien.
2026-09-29: da doi — mau cham timeline suy tu ten service (bam vao bang 8 mau, `serviceColor()`), bo 3 rule
gan ten service demo. Kiem tren trinh duyet (run SC02 that): order/payment/inventory ba mau khac nhau,
cung service cung mau. 6.68 kB -> 6.57 kB, con vuot 585 B; `ng build` pass. Dashboard khong co spec nao.
Con lai (thap, chua lam): `getEventIcon` gan icon theo 7 ten event demo; he khac deu la icon `event`. Chi la
hien thi, khong sai thong tin. Lam tong quat can API tra vai tro event (STARTED/COMPLETED/FAILED/bu tru) ma
`WorkflowSpec.classifyEvent` da biet — khong doan tu ten event.
Problem (build F7, 2026-09-23): stylesheet saga-detail 7.67 kB vuot warning budget
6 kB; production build van pass, chua vuot error budget 10 kB. CSS khong doi trong F7.
Expected: ra soat CSS lap/khong dung khi lam task dashboard; khong tang budget chi
de tat canh bao. Chi ghi nhan, khong implement trong task workflow conflict.

**F12 — Phep do duration chua dung nguon timestamp da luu — DA SUA 2026-09-28** · Priority: **cao**
Da sua: ca hai duong STEP_TIMEOUT (adjacency va projection) gan nhan nguon cua hai dau khoang do:
`clocks=DECLARED->DECLARED`, `clocks=A->B (mixed)` kem cau "different clocks ... includes whatever
separates them", hoac `clocks=unrecorded` voi du lieu truoc V17. Tach rieng voi `start=` (StartProvenance:
observation nao lam ranh gioi buoc). Chi la nhan: tier, nguong, oracle khong doi. Test: 3 test
`StepTimeoutDetectionTest` (moi test chay ca LEGACY lan PROJECTION; fail truoc khi sua). Full suite: 530/530.
Replay 27 run settled: chan doan (root, candidate, anchor) giong het; 10 run co STEP_TIMEOUT nay ghi
"Where the two timestamps came from was not recorded" — bo du lieu do co truoc F6 nen dung la khong biet.
Chua lam: R05/R06 va `durationMs` cua saga van do khoang thoi gian khong kem nhan; R05 con tru
`detectedAt` (dong ho platform) cho timestamp nguon — ghi F15.
Problem (ra soat code khi dong F6): `ExecutionProjection.Operation.duration()` va
duong STEP_TIMEOUT van tinh khoang giua timestamp ma chua xet `eventTimeSource`.
F6 lam nguon nay ton tai tren event, nhung hai dau co the la DECLARED/TRANSPORT hoac
NULL; duration nhu vay chua du de khang dinh thoi gian xu ly cua he nguon. Day la
kiem tra code, chua phai phep do so false positive tren bo live.
Expected: tai hien mixed clocks/legacy unknown bang test, xac dinh claim va nhan
provenance cho tung phep do; khong dong nhat StartProvenance (nguon ranh gioi buoc)
voi nguon timestamp, khong tu dong sua tier/oracle. Chi ghi roadmap trong F6.

**F11 — Citation cua cac occurrence khac va chon finding cho RCA con phu thuoc thu tu — DA SUA 2026-09-28** · Priority: **cao**
Tai hien truoc khi sua: STATE_REGRESSION co 3 dang citation qua 120 hoan vi; MULTI_TERMINAL_CONFLICT
nhieu dang; va cung mot tap finding tra theo thu tu khac nhau cho **4 chan doan khac nhau, doi ca
nguyen nhan de cu** (MULTI_TERMINAL_CONFLICT <-> INVALID_TRANSITION) vi R12 trich finding dau tien.
Da sua: (a) `raiseOrRevise` — co che F9 dung chung cho FORBIDDEN_POST_TERMINAL, STATE_REGRESSION,
MULTI_TERMINAL_CONFLICT; (b) `AnomalySignatureRule.matching` thay `firstMatch`: trich moi finding
khop, business time som nhat truoc, thu tu tong nhung khong khang dinh gi them; (c) anchor lay moc
som nhat, `anchorStepId` chi gan khi moi event o moc do cung mot buoc, hoa giua cac buoc -> UNKNOWN.
Test: `TransitionEvidenceTest` (3 moi, deu fail truoc khi sua), `RuleRegistryTest` (test "earliest"
truoc day chi ghim thu tu danh sach). Full suite sach: 526/526. Replay 27 run settled (co sweep
step-timeout) cu vs moi: khong run nao khac — moi run chi co toi da mot finding moi luat.
Van de goc giu lai ben duoi.
Problem (ra soat code khi sua F9, 2026-09-22): `detectStateRegression` va
`detectMultiTerminalConflict` van return khi da co finding, nen evidence cua chung giu
nguyen tien to dau tien. `AnomalySignatureRule.firstMatch` lay phan tu dau tien khop loai
tu danh sach repository; `findStandingBySagaId` khong co ORDER BY. R04 khop ca
FORBIDDEN_POST_TERMINAL va STATE_REGRESSION, nen chuan hoa citation F9 chua du de khang
dinh moi anchor/RCA deu doc lap thu tu. Chua do so ket qua de cu thay doi trong pipeline
day du; test F9 co tinh giu evidence canh tranh co dinh de khong danh dong hai van de.
Expected: tai hien bang hoan vi event va thu tu tra finding, roi chon evidence theo
business order da chung minh; giu UNKNOWN khi khong du can cu. Khong sua tier, oracle
hay doi dac ta I9 de hop thuc hoa processing order. Chi ghi roadmap trong task F9.

**F10 — Ngu nghia ten R07 va phep map strict cua oracle** · Priority: **trung binh-cao**
Problem: R07 de cu DOWNSTREAM_TIMEOUT tu khoang giua observation vuot nguong; khoang
do khong xac lap service nao xu ly cham (I3). Oracle strict map ten nay thanh
SOURCE_SERVICE_UNAVAILABLE, mot claim bao gom ca co lap mang; do do SC29 mat mang
khong tu no bac bo claim da map, va timeout khong dong nghia unavailable.
Expected: chot nghia claim/ten theo evidence va kiem tra suggestion; phep map oracle
phai duoc bien minh rieng. Hien tai 10 UNWARRANTED la ket qua cua mapping da chon,
chua phai phep do doc lap xac nhan nghia duy nhat cua ten. Khong doi score de lam dep.
F7 da danh cho workflow conflict; dung F10 de tranh trung ma.
Phan oracle da dinh chinh trong 963d4b6: B duoc ghi la dien giai da khai bao,
khong phai phep do. Phan doi ten/suggestion R07 van LATER, chua implement;
khong la dieu kien dong task oracle L10/L11.

**L1 — DLQ cho Kafka — DA SUA 2026-09-28 (khong DLQ; giu offset khi store sap)** · Priority: trung bình
Da sua goc: su kien mat vi loi cua CHINH observer (DB khong vao duoc) bi doi xu nhu loi cua record.
`KafkaErrorHandlingConfig.backOffFor`: loi store (TransientDataAccess, DataAccessResourceFailure,
CannotCreateTransaction, SQLTransient/SQLRecoverable trong chuoi cause) thu lai khong gioi han, backoff
tran 20s — offset khong commit, Kafka giu record, tu chay tiep khi DB ve. Loi khac giu ~60s roi log
va bo qua nhu cu, de mot record hong khong chan ca partition.
Khong lam DLQ topic: Kafka duy nhat observer doc la cua he nguon, tao topic/produce la ghi len ha tang
cua ho (ADR 0001). Bang dead-letter trong DB observer hong dung luc can (DB sap).
Test: `KafkaErrorHandlingConfigTest` (+3): phan loai loi; backoff store khong dung sau ~55h mo phong,
loi record dung trong 30-80s (fail tren code cu); handler seek lai offset khi store sap, bo qua record
loi. Full reactor 550/550.
Live 2026-09-28 (demo-postgres + demo-kafka): tat Postgres ~4 phut, 20 ORDER_CREATED vao order-created;
observer log "holding ... offset=29", 0 dong "Giving up"; bat lai -> 20/20 vao saga_events. Lan hai
(sau sua L2) them 20 nua, tong 40/40. Du lieu thu L1LIVE-1..40 con trong correlation_db demo.
Su co do chinh lan kiem chung nay gay ra (2026-09-28): 40 event L1LIVE duoc ban thang vao `order-created`,
topic payment-service cung tieu thu. Deserializer co kieu cua payment-service kep vinh vien o offset 29
(poison pill), moi saga demo sau do dung o IN_PROGRESS; preflight cua evaluate.py bat duoc. Da go bang
reset offset group `payment-service-group` qua dung cac record L1LIVE (p0 -> 49, p2 -> 51), khong dong
record that. Lan sau: event thu cho observer phai vao topic khop `topic-pattern` nhung khong he nao tieu
thu (vd. `order-observer-probe`).
Con lai: record loi khong phai store van chi con trong log (kem redaction L14).
Problem: hết retry (backoff ~60s) thì log ERROR và bỏ qua; mất sự kiện khi DB xuống lâu.
Expected: dead-letter topic, replay được.

**L2 — Consumer lag / backpressure — DA SUA 2026-09-28 (phan lag; backpressure HTTP chua)** · Priority: trung bình
Da sua: `KafkaClusterListeners.backlog()` = log-end offset - offset DA COMMIT, doc bang admin client
(chi describe, khong ghi len cluster nguon), cong tren moi partition cua moi cluster. Gauge
`correlation.kafka.backlog` (/actuator/metrics) va health component `ingestionBacklog`: UNKNOWN khi chua
doc duoc offset commit, LAGGING khi vuot `correlation.kafka.backlog-warning-threshold` (mac dinh 10000)
— khong keo service DOWN, vi dung doc moi la cach duy nhat giam lag.
Ban dau (80e5dd8) dung metric `records-lag` cua Kafka client — SAI, phat hien khi kiem chung live: metric
do theo vi tri fetch, nen partition bi giu o offset 29 voi 20 record phia sau (lag commit that = 20, xac
nhan bang kafka-consumer-groups) van bao 0 — dung ca ma no ton tai de bat.
Live sau sua (nguong 5): DB sap -> backlog 20, LAGGING o 20s/60s/100s; DB len -> 0, UP.
Test: `IngestionBacklogTest` (3, ca "giu o 29, end 49" lay tu lan do live). Full reactor 553/553.
Nguong 10000 la gia tri chon, chua do tren tai that. Partition chua tung commit khong duoc tinh.
HTTP ingest van chi co `MAX_BATCH=1000`.

**L3 — Luật RCA còn một nửa nằm trong Java — DA CHOT RANH GIOI 2026-09-28; sua R02** · Priority: trung bình
Chon nhanh "ranh gioi ro", khong chuyen het sang YAML: 9 luat YAML la luat ma tap anomaly la TOAN BO dieu
kien; luat Java can dieu ma format do khong noi duoc (trang thai run, so hoc thoi gian theo nguong
workflow, vang mat event spec doi hoi, bang chung tinh tu tap anomaly). Tieu chi kiem: khong luat Java nao
duoc chua tu vung cua mot he nguon — tu ngu cua nguon phai qua khai bao (I7).
Ra soat theo tieu chi do tim ra MOT vi pham: R02 nang TIMEOUT_RETRY_STORM len STRONG khi payload retry
(ca payload, ha chu thuong) chua "timeout"/"timed out" — cung loi F4 da go khoi R01. Da sua: them
`FailureSignal.CALL_TIMED_OUT`; R02 doc ly do qua `failure-signals` cua nguon, khong khai bao -> WEAK.
Khai bao demo them `lastException` vao reasonFields va `CALL_TIMED_OUT: [TimeoutException, timed out]`,
nen SC12 (lastException "TimeoutException: read timed out") van STRONG; SC06 (RuntimeException gateway)
van WEAK. Bang chung R02 gio trich ly do da doc, khong trich ca payload.
Test: `TimeoutRetryStormTest` (3; 2 fail tren code cu, ca SC12 pass ca hai). Full reactor 560/560.
Chua do lai bo danh gia; ket luan "SC12/SC06 khong doi" suy tu payload trong evaluate.py va replay SC06.
Van de goc giu lai:
Problem: 9 luật YAML, 9 luật hard-code trong `RcaService` (986 dòng). Thêm luật cho hệ
mới có khi phải sửa Java.
Expected: mọi luật khai báo được, hoặc ranh giới rõ giữa "luật domain-independent trong
core" và "luật khai báo".

**L4 — `AnomalyType` là enum Java** · Priority: thấp-trung bình
Problem: loại bất thường mới của một hệ cần sửa enum + migration.
Expected: đánh giá xem có use case thật không trước khi đổi. Chưa có → chưa làm.

**L5 — Xác thực cho endpoint ingest — DA SUA 2026-09-28** · Priority: trung bình
Da sua: `IngestAuthorization` (bearer token `correlation.ingest.token` / env `CORRELATION_INGEST_TOKEN`,
so sanh constant-time) chan ca hai duong GHI quan sat qua HTTP: `/api/ingest/**` (ke ca `/infer`, vi
no doc va tra lai payload) va `/api/admin/inject-event` — khoa mot ma de ngo cai kia thi vo nghia.
Khong dat token -> 503 (dong), khong phai mo. Header `Authorization` bi loai khoi header chuyen cho
mapping, de token khong thanh du lieu quan sat. Kafka khong doi (ACL la viec cua cluster). Khong them
Spring Security. `evaluate.py` gui token tu cung bien env; docker-compose truyen bien vao.
Test: `IngestControllerTest` (+4), `AdminEventTimeSourceTest` sua. Full reactor 557/557.
Live: khong cau hinh -> 503; co token: khong header 401, sai 401, dung 202, inject khong header 401,
GET /api/sagas 200.
Luu y tai lap: chay lai bo thuc nghiem (evaluate.py, che do INJECT) nay can dat CORRELATION_INGEST_TOKEN
cho ca observer va script; khong dat thi inject tra 503 va script bao loi ro.
Con mo (chua lam): API doc, `/rca/**` (ghi ket qua RCA, khong ghi evidence) va dashboard van khong xac thuc.
Problem: `/api/ingest/**` không xác thực; sự kiện gửi vào trở thành bằng chứng chẩn đoán.
Expected: có phương án trước khi chạy ở môi trường thật.

**L6 — `DeduplicationCache` per-instance — DA GHI RO 2026-09-28** · Priority: thấp
Javadoc cua `DeduplicationCache` da noi cache khong phai ranh gioi dung dan, nhung con tro vao unique
`event_id` — da loi thoi tu V13. Sua cho dung code: tra cuu (source_name, event_id) va unique tren cap do.
Chi doi tai lieu; hanh vi khong doi.
Problem: chạy nhiều replica thì cache không dùng chung.
Expected: chấp nhận được (UNIQUE constraint mới là ranh giới thật) — ghi lại để không ai
tưởng cache là cơ chế đúng đắn.

**L9 — schema-h2.sql lech khoi migration that — DA SUA 2026-09-28** · Priority: thap-trung binh
Da chon: cho ban H2 bam migration, va them test de lech lan sau lam test do thay vi thanh tieng on.
Them `source_name`, `correlation_basis`, `trace_id`, `transport_ref`, `retracted_at`, `retraction_reason`,
bang `execution_aliases`, rong `saga_id`/`event_id` 300, unique (source_name, event_id) nhu V13.
Test: `ApplicationContextTest.everyScheduledPassRunsAgainstTheTestSchema` goi truc tiep 8 luot quet
(khong qua lease, de khong pass rong) — fail truoc khi sua voi dung loi `SI1_0.SOURCE_NAME`. Full suite:
538/538, 0 dong "Column ... not found" trong log.
Problem: `ApplicationContextTest` khoi dong that, cac luot quet chay ngay, va chung query
cot `source_name` ma ban H2 khong co — moi lan chay in ra mot loat ERROR
`Column "SI1_0.SOURCE_NAME" not found`. Test van xanh, nen tieng on nay che dung cai ma
comment dau file schema so: mot loi wiring that se lan trong dong ERROR nay.
Expected: hoac cho ban H2 bam theo migration o nhung cot ma scheduled sweep doc, hoac
khong de sweep chay trong context test. Chua chon.

**L8 — Không có test cho nội dung log của merge** · Priority: thấp
Problem: N4 phát hiện một placeholder `{}` không được thay trong log của
`ExecutionAliasResolver.merge` — số absence claim thu hồi không bao giờ vào log. Đã sửa,
nhưng chỉ vì đọc log bằng mắt trong lúc kiểm chứng live.
Expected: cân nhắc xem log vận hành có đáng test không. Chưa rõ có đáng.

**L7 — Nhiễu log `rule.yaml has no 'rules' key`** · Priority: thấp
Kiem tra lai 2026-09-28: khong tai hien duoc. 113 jar tren classpath correlation-service khong jar nao
ship `rules/`, `workflows/`, `ingestion/` hay `failure-signals/*.yaml`; 0 dong canh bao trong full suite
va trong log observer live. Rui ro con lai (mot jar sau nay ship `workflows/*.yaml` se duoc nap nhu khai
bao) la gia dinh — theo AGENTS.md chua lam. Mo lai khi co mot jar that ship cac duong dan do.
Problem: một jar phụ thuộc ship `rules/rule.yaml`, bị quét trúng. Vô hại hôm nay, nhưng
một jar ship `rules/*.yaml` có key đúng sẽ nạp rác vào engine.
Expected: thu hẹp phạm vi quét classpath.

---

## OUT OF SCOPE

- Observer điều khiển saga (retry hộ, kích hoạt bồi hoàn, gửi command). Vi phạm I1.
- Suy luận tương quan không có bằng chứng (cửa sổ thời gian, trùng business key). Vi phạm I2.
- Điểm tin cậy dạng số cho RCA. Đã bị thay bằng tầng bằng chứng, cố ý.
- Distributed transaction giữa observer và hệ được quan sát. Vi phạm I6.
- Không ép hệ nguồn tuân theo convention riêng của observer nếu observer có thể
  adapt hợp lý. Các metadata chuẩn như sagaId, correlationId, traceId,
  message key/context propagation có thể được hệ nguồn cung cấp theo best practice
  và observer phải tận dụng chúng khi có.
