# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Lò Văn Long / 2A202602541
**Repo:** https://github.com/getlmt/K4-Track02-Day17-LoVanLong-2A202602541-DataPipelineEngineering
**Commit bài nộp:** `bb863fc` (3 lỗi đã sửa: `d67f37c`, `7cc5171`, `17e13ce`; bonus B1: `bb863fc`; REPORT được commit ngay sau)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus 5.5): Hỗ trợ chi tiết các quy trình thực hiện bài lab, bao gồm hướng dẫn cách tiếp cận bài toán, các bước thực hiện từng yêu cầu, cách sử dụng công cụ và tài nguyên cần thiết, cách kiểm tra kết quả sau mỗi bước và cách hoàn thiện bài lab. Học viên tự thực hiện theo hướng dẫn và chịu trách nhiệm về kết quả cuối cùng.
**Nguồn tham khảo khác (nếu có):** README và `docs/` của repo đề bài, slide Ngày 17.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify: `24 rows for 12 tickets`; T-91 có 3 hàng (low/open, high/open, high/closed); `gold_doc_chunks` 22 rows / 9 chunks; checksum doc_chunks đổi sau mỗi lần rerun | verify: feature_daily ≠ full recompute (`c50b8851affe != 8630e04a61d1`); u05 ngày 08-12 = (2, 0) thay vì (5, 1); `LOOKBACK_DAYS=0 < 3`; rerun: feature_daily C0 ≠ C1 = C2 = C3 | verify: T-97 `is_deleted=False`, còn `u06` + subject/body ("Nguyễn Văn An"); T-97 còn trong snapshot `v2026-08-16` (1 row) và 2 chunk trong `gold_doc_chunks` |
| **Nguyên nhân gốc** | `upsert_silver_tickets` chỉ dedup trong một batch rồi `INSERT` (append): không có khoá giữa các batch, không so thứ tự thay đổi → mỗi ngày thêm hàng mới cho cùng ticket, rerun nhân bản hàng, `gold_doc_chunks` join vào nên nhân chunk | `LOOKBACK_DAYS = 0` dựa trên giả định "event tới trong vài giây"; mỗi run chỉ tính lại partition của chính ngày đó, nên event của u05 (event_time 08-12, tới Bronze 08-15) không bao giờ được cộng vào ngày 08-12 | `ticket_changes_sql` lấy `ticket_id` từ `after`; với `op='d'` thì `after = null` → `ticket_id` NULL → bị `WHERE ticket_id IS NOT NULL` lọc mất: delete bị nuốt im lặng như Kafka tombstone, không tới Silver/Gold |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: `INSERT` → `MERGE INTO silver_tickets ... ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT` | `pipeline/config.py`: `LOOKBACK_DAYS = 3` (= ceil(P99) đo từ Bronze); mỗi run xoá và tính lại `[day−3, day]` từ Silver | `pipeline/staging.py`: `ticket_id = coalesce(after.ticket_id, before.ticket_id)`; các cột khác vẫn lấy từ `after` (NULL) → tombstone không PII, `is_deleted = true`, giữ LSN 24020000 |
| **Khái niệm trên slide** | Silver có khoá (1 hàng = 1 thực thể); MERGE theo khoá (bốn cách viết idempotent); LSN guard: chỉ thay đổi mới hơn mới được ghi | Data về muộn; event time vs ingest time; lookback = ceil(P99) "đo từ Bronze, đừng đoán"; overwrite-partition | CDC log-based (phong bì Debezium `before/after/op/lsn`); CDC delete ≠ Kafka tombstone; "Xoá phải lan" xuống Gold; tombstone + LSN chống hồi sinh |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (p50 = 0.00, p95 = 2.90, max = 3) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY (`silver_tickets` 3c15dfd43701, `gold_feature_daily` 8630e04a61d1 ở cả lite và dbt)

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` là bảng thực thể, mỗi thay đổi chạm đúng một khoá và một ticket sống qua nhiều ngày nên không có partition nào để ghi đè; MERGE + LSN guard cho kết quả idempotent và "mới hơn thắng" kể cả khi replay batch cũ. `gold_feature_daily` là tổng hợp theo `event_date`: xoá rồi tính lại nguyên partition từ Silver vừa đơn giản, tất định, vừa tự hấp thụ event muộn, còn MERGE trên số tổng hợp dễ cộng trùng khi chạy lại.
- Tombstone thay vì xoá hẳn hàng trong Silver: hàng giữ `ticket_id` + `_lsn` của lần xoá nên replay batch cũ (LSN nhỏ hơn) không làm ticket hồi sinh (khi thử bỏ LSN guard, rerun 08-12 đưa T-97 trở lại và checksum `gold_doc_chunks` lệch); downstream đọc `is_deleted` để gỡ khỏi RAG/training; PII vẫn bị xoá vì các cột là NULL. Đánh đổi: hàng tombstone tồn tại mãi, có thể dọn sau khi quá cửa sổ replay tối đa.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: thí nghiệm train/eval tái lập được, đúng point-in-time (T-88: `v2026-08-14` = False, `v2026-08-15` = True — feedback muộn tạo version mới chứ không sửa lịch sử), và `SnapshotImmutableError` chặn việc vô tình ghi đè.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: ~80 bản ghi Bronze trong 7 ngày, chạy trên một máy, không cần hạ tầng; DuckDB đọc Parquet trực tiếp và có MERGE; dbt thêm contract, data test, unit test và chiến lược incremental (merge, microbatch). Spark (cluster, JVM, chi phí vận hành) chỉ đáng khi dữ liệu vượt quá sức một máy.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?

   Bất biến là mặc định để tái lập, nhưng yêu cầu xoá hợp lệ phải thắng (Nghị định 13/2023/NĐ-CP, GDPR Điều 17). (a) Phòng từ gốc: snapshot chỉ chứa text đã khử định danh, khi đó không còn dữ liệu cá nhân để xoá. (b) Khi có yêu cầu xoá: phát hành lại các version bị ảnh hưởng (ví dụ `v2026-08-12-r1` không có T-97), đánh dấu bản cũ deprecated rồi xoá vật lý, ghi lineage và lý do; model đã train trên bản cũ được retrain theo lịch. (c) Với bản không thể ghi lại (backup): crypto-shredding — mã hoá theo user, xoá khoá. Bronze cũng chứa bản gốc nên phải đi qua cùng quy trình.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

   Chốt ở ranh giới Bronze → Silver, cùng chỗ `mask_pii`: thêm NER tiếng Việt (underthesea hoặc model NER fine-tune, hoặc Presidio với recognizer tuỳ biến) để che tên thành `<NAME>`, kết hợp từ điển họ Việt; thêm một gate trước Gold/RAG giống check "no email / phone survives" của verify — phát hiện PII thì quarantine hoặc fail run. Đo: bộ mẫu gán nhãn tay → precision/recall theo loại PII (ưu tiên recall, chấp nhận che nhầm); khi vận hành, lấy mẫu định kỳ để đếm tỉ lệ hàng còn PII sau khi che và cảnh báo khi vượt ngưỡng; chạy test hồi quy mỗi lần đổi model.

## 5. Output (dán nguyên văn)

Chạy trên Windows bằng lệnh Python tương đương các target `make` (theo [SUBMISSION.md](../docs/SUBMISSION.md)), trên code của commit `17e13ce` (đủ 3 bản sửa). Sau khi thêm B1 (`bb863fc`, chỉ đổi `pipeline/llm_label.py`), verify chạy lại vẫn 18/18 ALL PASS, pytest 34 passed, `checksums.txt` không đổi.

```text
PS> .\.venv\Scripts\python.exe -m scripts.verify          # make verify
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

PS> .\.venv\Scripts\python.exe -m pytest                  # make test
..................................                                       [100%]
34 passed in 3.44s

PS> .\.venv\Scripts\python.exe -m scripts.rerun_check     # make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

PS> .\.venv\Scripts\python.exe main.py --lateness         # make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

PS> .\.venv\Scripts\python.exe main.py --land-only        # make dbt (bước 1: land Bronze)
  2026-08-10  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-11  tickets:already-landed(3)  events:already-landed(5)  transcripts:already-landed(2)
  2026-08-12  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-13  tickets:already-landed(3)  events:already-landed(7)  transcripts:already-landed(1)
  2026-08-14  tickets:already-landed(4)  events:already-landed(4)  transcripts:already-landed(1)
  2026-08-15  tickets:already-landed(4)  events:already-landed(8)  transcripts:already-landed(1)
  2026-08-16  tickets:already-landed(4)  events:already-landed(7)  transcripts:already-landed(2)

PS> cd dbt_project; ..\.venv\Scripts\dbt.exe --no-use-colors build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17   # make dbt (bước 2)
10:30:13  Running with dbt=1.12.5
10:30:13  Registered adapter: duckdb=1.11.0
10:30:14  Unable to do partial parsing because saved manifest not found. Starting full parse.
10:30:16  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
10:30:16  
10:30:16  Concurrency: 1 threads (target='dev')
10:30:16  
10:30:18  1 of 19 START sql view model main.stg_events ................................... [RUN]
10:30:19  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.07s]
10:30:19  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
10:30:19  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
10:30:19  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
10:30:19  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.10s]
10:30:19  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
10:30:19  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.16s]
10:30:19  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
10:30:19  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.10s]
10:30:19  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
10:30:19  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.05s]
10:30:19  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
10:30:19  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
10:30:19  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
10:30:19  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
10:30:19  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
10:30:19  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
10:30:19  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
10:30:19  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
10:30:19  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
10:30:19  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
10:30:19  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
10:30:19  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
10:30:19  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
10:30:19  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
10:30:19  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
10:30:19  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
10:30:19  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
10:30:19  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
10:30:19  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
10:30:19  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
10:30:19  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.03s]
10:30:19  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
10:30:19  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.05s]
10:30:19  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
10:30:19  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
10:30:19  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
10:30:19  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.03s]
10:30:19  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
10:30:19  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.03s]
10:30:19  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
10:30:19  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.04s]
10:30:19  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
10:30:20  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.03s]
10:30:20  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.28s]
10:30:20  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
10:30:20  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
10:30:20  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
10:30:20  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
10:30:20  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
10:30:20  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
10:30:20  
10:30:20  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 3.42 seconds (3.42s).
10:30:20  
10:30:20  Completed successfully
10:30:20  
10:30:20  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

PS> .\.venv\Scripts\python.exe -m scripts.parity          # make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

### Bonus B1 — bước LLM có cache (`pipeline/llm_label.py`)

Khoá cache = `sha256(MODEL | PROMPT_VERSION | prompt)`; cache lưu cả câu trả lời sai schema nên chạy lại cùng model + prompt gọi LLM 0 lần; ước tính token/chi phí cho các ticket chưa có trong cache trước khi gọi; nhãn ngoài `bug/billing/other` vào `llm_label_quarantine`, không vào Gold; mỗi hàng Gold mang `model` + `prompt_version`.

```text
PS> .\.venv\Scripts\python.exe -m scripts.bonus_llm       # make bonus-llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```
