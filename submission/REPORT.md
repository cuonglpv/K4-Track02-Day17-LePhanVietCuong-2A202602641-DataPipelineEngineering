# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Lê Phan Việt Cường / 2A202602641
**Repo:** https://github.com/cuonglpv/K4-Track02-Day17-LePhanVietCuong-2A202602641-DataPipelineEngineering
**Commit bài nộp:** `0d15e0a106a097f30f139b430e86a0a68d56b25f` (commit mã nguồn đã kiểm tra; commit sau đó chỉ thêm REPORT và checksums)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Sonnet 5.5): cài môi trường, chạy baseline, đề xuất và áp dụng 3 sửa đổi (silver.py, staging.py, config.py), soạn REPORT. Tôi đã review diff và các lệnh verify/pytest/rerun/dbt/parity chạy trên code nộp.
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify: `silver_tickets` 24 dòng cho 12 ticket; T-91 có 3 phiên bản (low/open, high/open, high/closed/bug) thay vì 1 dòng mới nhất | verify: u05 ngày 08-12 đếm được (2, 0), mong đợi (5, 1); `LOOKBACK_DAYS=0 < 3`; `gold_feature_daily` c50b8851affe != recompute 8630e04a61d1 | verify: T-97 `is_deleted=False`, còn text (đã mask email/sđt, còn tên); còn 1 dòng ở training set mới nhất và 2 chunk trong RAG |
| **Nguyên nhân gốc** | `upsert_silver_tickets` chỉ dedup trong 1 batch rồi `INSERT` thẳng, nên mỗi ngày thêm 1 hàng cho cùng `ticket_id`; replay không có chốt thứ tự | `LOOKBACK_DAYS=0` giả định event đến ngay; thực đo P99 = 3 ngày (u05 sinh 08-12, giao 08-15), nên partition 08-12 không bao giờ được tính lại khi event muộn đến | `ticket_changes_sql` lấy `ticket_id` từ `after`; CDC delete có `after=null` nên bị `WHERE ticket_id IS NOT NULL` loại → xoá không bao giờ vào Silver/Gold |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: `MERGE ... ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `NOT MATCHED INSERT` | `pipeline/config.py`: `LOOKBACK_DAYS = 3 = ceil(P99)`; `gold.py` giữ nhóm theo `event_time` (không chuyển sang ngày ingest) | `pipeline/staging.py`: `coalesce(key.ticket_id, after.ticket_id, before.ticket_id)`; tombstone Kafka (`_op IS NULL`) vẫn bị bỏ vì không mang thay đổi; Silver lưu `is_deleted=true`, PII null, giữ `_lsn` chống hồi sinh |
| **Khái niệm trên slide** | MERGE/upsert theo khoá, idempotent, thứ tự LSN | Event time vs ingest time, late data, lookback từ P99 | CDC delete vs tombstone, propagate delete xuống Gold/RAG |

## 2. Các con số

- Baseline lateness (43 bản ghi Bronze): P50 = 0.00, P95 = 2.90, P99 = 3.00 ngày (max 3)
- P99 lateness đo từ Bronze: `3` ngày → `LOOKBACK_DAYS = 3` (ceil(3.00) = 3, làm tròn lên để phủ cả P99)
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity` (scripts.parity): PARITY (silver_tickets `3c15dfd43701`, gold_feature_daily `8630e04a61d1`); dbt build PASS=19

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: Silver là trạng thái hiện tại theo khoá nên MERGE + điều kiện LSN giúp chạy lại/replay mà không nhân đôi hay ghi đè bản mới; Gold là aggregate theo ngày nên xoá-rồi-ghi cả partition [day-3, day] cho kết quả đúng bất kể thứ tự đến.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ khoá và `_lsn` để batch cũ replay không hồi sinh ticket đã xoá, còn PII (user_id, subject, body) được null hoá.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: huấn luyện tái lập được và không bị rò rỉ dữ liệu tương lai; thay đổi muộn tạo version mới thay vì sửa version cũ.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu chỉ vài chục dòng, một máy là đủ; Spark chỉ thêm chi phí vận hành. dbt cho merge/microbatch khai báo và test sẵn, parity chứng minh hai cách ra cùng checksum.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot bất biến vs quyền xoá.** Lab chỉ xoá T-97 khỏi snapshot mới nhất và RAG; `v2026-08-12..14` vẫn giữ văn bản (đã che email/sđt nhưng còn tên) nên chưa phải xoá PII đầy đủ. Ở production: (a) tách PII khỏi snapshot, snapshot chỉ giữ `ticket_id` + đặc trưng, văn bản nằm ở bảng có thể xoá và join khi huấn luyện; (b) có quy trình xoá theo yêu cầu: danh sách `deleted_ticket_ids` bắt buộc, khi cần thì dựng lại các snapshot bị ảnh hưởng thành version mới (đánh dấu cũ là thu hồi, không sửa tại chỗ) và huỷ model/artefact huấn luyện từ chúng; (c) giới hạn thời gian lưu snapshot và ghi nhật ký xoá để audit. Bất biến áp dụng cho quá trình ghi, không đặt trên quyền pháp lý.
2. **Chốt PII.** Regex chỉ bắt dạng có cấu trúc; tên riêng cần lớp thứ hai. Tôi đặt chốt ở Silver (ngay sau Bronze, trước mọi bảng tiêu thụ), vì Bronze phải giữ nguyên bản gốc, và thêm kiểm tra ở cửa Gold/RAG: NER tiếng Việt (hoặc từ điển tên từ bảng users) che tên trước khi tạo text/chunk. Đo bằng bộ mẫu gán nhãn tay: recall PII (mục tiêu ≥ 0.99 cho email/sđt, báo riêng cho tên), precision để không che nhầm, cộng một check trong verify quét Silver/Gold tìm pattern còn sót và theo dõi số lần quét thấy PII mỗi lần chạy (phải bằng 0).

## 5. Output (dán nguyên văn)

Lệnh PowerShell tương đương (`make verify` / `test` / `rerun3` / `lateness` / `dbt` / `parity`); chạy trên commit `0d15e0a`. pytest dùng `--basetemp` vì thư mục temp mặc định của máy bị chặn quyền.

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest --basetemp=.pytest_tmp
..................................                                       [100%]
34 passed in 2.45s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ main.py --land-only; dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
05:28:56  Running with dbt=1.12.5
05:28:56  Registered adapter: duckdb=1.11.0
05:28:57  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
05:28:57  
05:28:57  Concurrency: 1 threads (target='dev')
05:28:57  
05:28:57  1 of 19 START sql view model main.stg_events ................................... [RUN]
05:28:57  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.07s]
05:28:57  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
05:28:57  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
05:28:57  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
05:28:57  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.11s]
05:28:57  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
05:28:57  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.11s]
05:28:57  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
05:28:57  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.13s]
05:28:57  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
05:28:57  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
05:28:57  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
05:28:57  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
05:28:57  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
05:28:57  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
05:28:57  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
05:28:57  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
05:28:57  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
05:28:57  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
05:28:57  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
05:28:57  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
05:28:57  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
05:28:57  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
05:28:57  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
05:28:57  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
05:28:57  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
05:28:57  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
05:28:57  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
05:28:57  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
05:28:57  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
05:28:57  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
05:28:57  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.04s]
05:28:57  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
05:28:57  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.02s]
05:28:57  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
05:28:57  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.02s]
05:28:57  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
05:28:58  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.02s]
05:28:58  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
05:28:58  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.02s]
05:28:58  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
05:28:58  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.03s]
05:28:58  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
05:28:58  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.02s]
05:28:58  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.20s]
05:28:58  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
05:28:58  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
05:28:58  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
05:28:58  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
05:28:58  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
05:28:58  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
05:28:58  
05:28:58  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.06 seconds (1.06s).
05:28:58  
05:28:58  Completed successfully
05:28:58  
05:28:58  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
