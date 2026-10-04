# Bonus B2 — Thiết kế flywheel dữ liệu cho chatbot CSKH tiếng Việt

## Bài toán và ràng buộc thực

Một chatbot CSKH cho ví điện tử/ngân hàng số tại Việt Nam xử lý khoảng 200k hội thoại mỗi ngày. Mỗi hội thoại sinh ra trace (câu hỏi, tài liệu RAG được truy xuất, câu trả lời, phản hồi 👍/👎, và đôi khi agent người tiếp quản). Đội ML muốn biến trace thành **eval set** và **dữ liệu fine-tune/DPO** mỗi tuần. Khó ở chỗ: (1) văn bản chứa PII (tên, CCCD, số tài khoản, số điện thoại) bằng tiếng Việt có dấu mà regex không bắt hết; (2) phản hồi đến muộn và có thể bị sửa/xoá khi khách yêu cầu xoá dữ liệu; (3) nếu trace dùng để train lại bị rò sang eval set thì điểm eval vô nghĩa; (4) ngân sách LLM-as-judge có hạn.

```
App/Bot ──trace──▶ Kafka ─▶ Bronze (Parquet, bất biến, PII thô, mã hoá, TTL ngắn)
Feedback/CDC ────────────────▶ Bronze
                                 │ Silver: dedup theo trace_id, MERGE theo khoá + LSN,
                                 │         PII scrub (regex + NER) → quarantine nếu nghi ngờ
                                 ▼
                  Silver traces ──▶ Gold: eval_set (đóng băng, version, hash-split theo user)
                                └─▶ Gold: sft/dpo_pairs (snapshot as-of, loại user trong eval)
                                └─▶ Gold: judge_labels (cache hash+model+prompt_version)
            deleted_user_ids ──▶ lan xuống Silver tombstone, Gold mới, RAG index
```

## Các quyết định chính

**1. Batch hay streaming? → batch theo ngày, micro-batch theo giờ cho phản hồi.** Flywheel dùng dữ liệu cho lần train tuần sau nên độ tươi vài giờ là đủ. Đánh đổi: streaming (Kappa) cho độ trễ thấp nhưng tốn vận hành và khó backfill; batch giữ được cùng một code path cho chạy hằng ngày và backfill như lab này. Chọn batch vì không có tính năng nào cần dưới một giờ.

**2. Dữ liệu đến muộn → đo lateness từ Bronze, lookback = ceil(P99).** Phản hồi 👎 và việc agent tiếp quản có thể đến sau hội thoại nhiều ngày (như u05 trong lab). Đánh đổi: lookback lớn tính lại nhiều partition hơn (tốn tiền) so với lookback nhỏ (sai số). Tôi đo P99 hằng tuần và tự động cảnh báo khi P99 vượt lookback hiện tại, thay vì đặt số cố định.

**3. Rò rỉ train/eval → chia theo `user_id` bằng hash cố định, không chia theo hội thoại.** Cùng một khách hỏi nhiều lần các câu gần giống nhau; chia theo hội thoại làm eval và train chứa gần-trùng. Đánh đổi: chia theo user làm lệch phân phối chủ đề một chút so với chia ngẫu nhiên, nhưng đổi lại điểm eval đáng tin. Snapshot train dựng lại "as of" ngày cắt, và loại mọi user có trong eval; thêm bước decontaminate bằng so khớp n-gram trước khi publish.

**4. PII tiếng Việt → nhiều lớp, chốt ở Silver và lần nữa trước Gold.** Regex bắt email/số điện thoại/CCCD; NER tiếng Việt (hoặc từ điển tên từ bảng khách hàng) che tên; mẫu nghi ngờ vào quarantine thay vì đi tiếp. Đo bằng bộ mẫu gán nhãn tay (recall PII mục tiêu ≥ 0.99, báo riêng cho tên) và đếm PII còn sót khi quét Gold, phải bằng 0. Đánh đổi: che quá tay làm mất ngữ nghĩa train; ưu tiên recall hơn precision vì rủi ro pháp lý (Nghị định 13/2023 về bảo vệ dữ liệu cá nhân) nặng hơn rủi ro mất chút chất lượng.

**5. Quyền xoá vs snapshot bất biến → tách PII khỏi snapshot, xoá lan bằng tombstone.** Snapshot chỉ chứa `trace_id` và văn bản đã scrub; khi có yêu cầu xoá thì ghi tombstone ở Silver (giữ khoá và LSN để replay không hồi sinh), tạo version Gold mới không chứa user đó, thu hồi version cũ và model đã train từ chúng. Đánh đổi: tốn thêm một lần train lại so với "chấp nhận snapshot cũ còn dữ liệu", nhưng cái sau không bảo vệ được quyền xoá.

**6. LLM-as-judge → cache theo hash(input)+model+prompt_version, validate schema, ước tính chi phí trước.** Cùng cơ chế với B1 trong lab: chạy lại 0 lần gọi, đổi prompt thì chấm lại có chủ đích, câu trả lời sai schema vào quarantine. Đánh đổi: bộ nhớ cache tăng theo số phiên bản prompt, nhưng chi phí judge (khoảng 80% hoá đơn LLM của pipeline) giảm mạnh khi backfill.

## Phương án bị loại

**Fine-tune thẳng từ stream thời gian thực (online learning).** Hấp dẫn vì mô hình "học ngay", nhưng bị loại vì (a) không kiểm soát được rò rỉ train/eval, (b) một lô phản hồi xấu hoặc tấn công prompt có thể đầu độc mô hình trước khi ai kịp nhìn, (c) không tái lập được lần train, trái với nguyên tắc "chạy lại cho cùng checksum". Batch có snapshot version cho phép dừng, so sánh và quay lui.

## Chi phí và quy mô

Ở 10× dữ liệu, điểm nghẽn đầu tiên là chi phí LLM judge và số file nhỏ ở Bronze (nên compact theo giờ), không phải tính toán Silver/Gold. DuckDB/dbt đủ cho một node đến cỡ vài trăm triệu dòng; khi vượt thì chuyển Gold sang Spark/lakehouse mà giữ nguyên contract và bộ kiểm tra checksum.
