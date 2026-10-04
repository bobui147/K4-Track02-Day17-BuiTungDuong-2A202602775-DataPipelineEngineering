# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Bùi Tùng Dương / 2A202602775
**Repo:** https://github.com/bobui147/K4-Track02-Day17-BuiTungDuong-2A202602775-DataPipelineEngineering
**Commit bài nộp:** `e85f2599071a27e0fa462a989314d1344c62e032`
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Codex — đọc luồng dữ liệu, chạy baseline, phân tích và triển khai ba cách sửa, hỗ trợ viết REPORT và chạy toàn bộ kiểm chứng; người nộp cần review, hiểu và giải thích được các thay đổi trước khi nộp.
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Verify báo 24 hàng cho 12 ticket; T-91 trả về cả 3 trạng thái thay vì chỉ `high / closed / bug`. | Checksum `gold_feature_daily` là `c50b8851affe…`, khác full recompute `8630e04a61d1…`; u05 ngày 08-12 chỉ có `(2 events, 1 click, 0 feedback down)` thay vì `(5, 3, 1)`. | T-97 còn 2 hàng sống chứa dữ liệu cá nhân đã mask; snapshot mới nhất còn 1 hàng và RAG còn 2 chunks của T-97. |
| **Nguyên nhân gốc** | Batch đã dedup nội bộ nhưng vẫn `INSERT`, nên nhiều batch tạo nhiều hàng; replay batch cũ cũng có thể ghi đè logic hiện tại. | `LOOKBACK_DAYS=0` chỉ tính partition của ngày ingest, không tính lại ngày event của dữ liệu đến muộn. | Parser chỉ lấy khóa từ `after`; CDC delete có `after=null`, nên bị lọc mất cùng Kafka tombstone. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: `MERGE` theo `ticket_id`; chỉ update khi `source._lsn > target._lsn`, insert khi chưa có khóa. | `pipeline/config.py`: đặt `LOOKBACK_DAYS=3`, bằng `ceil(P99)` đo từ Bronze; logic Gold tiếp tục nhóm theo `event_time`. | `pipeline/staging.py`: lấy `ticket_id = coalesce(after.ticket_id, before.ticket_id)`; giữ delete thành hàng Silver có `is_deleted=true` và các trường PII null. |
| **Khái niệm trên slide** | Silver có khóa; keyed upsert/MERGE; LSN guard cho replay idempotent. | Event time khác ingest time; recompute cửa sổ lookback bằng overwrite-partition. | CDC delete khác Kafka tombstone; “xoá phải lan” từ Silver xuống snapshot mới nhất và RAG. |

## 2. Các con số

- Lateness đo từ 43 bản ghi Bronze: P50 `0.00`, P95 `2.90`, P99 `3.00`, max `3` ngày → cấu hình baseline `LOOKBACK_DAYS = 0` chưa bao phủ; giá trị yêu cầu tối thiểu là `3`.
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: ticket là trạng thái mutable cần cập nhật có điều kiện theo LSN, còn feature ngày có thể tính lại nguyên partition một cách xác định.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ khóa, LSN và dấu xoá để replay thay đổi cũ không làm ticket hồi sinh, đồng thời loại dữ liệu cá nhân.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: mỗi version tái lập được đúng dữ liệu đã biết tại thời điểm đó và tránh label leakage.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu nhỏ, chạy local zero-key; DuckDB đủ nhanh còn dbt bổ sung materialization, contract và test khai báo mà không cần chi phí cụm phân tán.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   Tôi sẽ tách tính bất biến logic khỏi lưu giữ vật lý: ghi audit tombstone/manifest xoá, mã hoá dữ liệu nhạy cảm bằng khóa riêng rồi crypto-shred khóa hoặc chạy quy trình purge có kiểm toán trên mọi snapshot/backup chịu phạm vi pháp lý. Sau purge, tạo version mới và lưu bằng chứng xoá thay vì giả vờ snapshot cũ chưa từng tồn tại.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   Tôi sẽ đặt detector kết hợp rule, NER và allowlist ngay tại cổng Bronze→Silver, quarantine bản ghi rủi ro cao trước khi văn bản đi vào Gold; đo precision/recall trên tập PII gán nhãn, tỷ lệ leak qua scan định kỳ và tỷ lệ false-positive theo từng loại thực thể.

## 5. Output (dán nguyên văn)

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest -p no:cacheprovider
..................................                                       [100%]
34 passed in 3.10s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
fresh build             8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
[OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
[OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
