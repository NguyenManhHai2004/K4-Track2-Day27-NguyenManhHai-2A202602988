# Reflection (≤1 page)

**Kết quả:** Practice 50.00 (TPR 1.0, FPR 0) · Public 48.18 (TPR 1.0, FPR 0, overage 0.091) · Private 32.41 (TPR 0.648, FPR 0, không vượt ngân sách).

**Which fault types were hardest to catch, and why?**

- **Lỗi tinh vi trên các chỉ số liên tục.** Practice/public gần như toàn lỗi "obvious", cách dải sạch hàng chục σ, nên đều đạt TPR 1.0; private chỉ bắt được khoảng 65%. Vì FPR private = 0, phần bị sót gần như chắc chắn nằm ngay dưới ngưỡng của tôi, ở các chỉ số liên tục: row_count / mean_amount / null / staleness, freshness SLA, lineage runtime, feature skew, embedding centroid / doc age. Các lỗi nhị phân (`schema_hash_mismatch`, `type_violation`, orphan output, missing upstream) không có dạng tinh vi vì tool trả về đúng/sai rõ ràng.
- **Embedding centroid không có khoảng trống:** dữ liệu sạch lên tới 0.0388, drift bắt đầu từ 0.040. Ngưỡng nào cũng là đánh cược giữa FP và FN.
- **Missing upstream** bị sót ở practice không phải vì khó về thống kê mà vì đọc sai trường payload (tên model nằm ở `job`, không phải `model`). Bài học: in payload thật trước khi viết logic.
- **Lỗi đến sớm trong stream.** Hai lỗi tinh vi ở public (doc age 48.3, centroid 0.040) bị sót vì rơi vào giai đoạn warm-up, khi defense chưa học được tâm phân phối nên vẫn dùng ngưỡng 3σ thô. Ngưỡng một phía là `cap − (cap − mean)/6`, sai số của mean chỉ ảnh hưởng 1/6, nên tôi giảm warm-up từ 5 xuống 3 mẫu.

**What would you change about your cost/coverage tradeoff, if you had another pass?**

- Payload chỉ chứa metadata cố định, gọi lại cùng tool trả về cùng kết quả, nên chiến lược tối ưu là đúng 1 tool / event và không bỏ event nào. Ở public, 240/220 credits mất 1.82 điểm, nhưng bỏ ~10 event để tiết kiệm 20 credits sẽ mất ~1.3 điểm cho mỗi lỗi lọt qua. Ở private (budget 320, ~300 credits cho 200 event) không bị phạt. Phần chi phí tôi sẽ giữ nguyên.
- Thứ tôi sẽ đổi là cân bằng TPR/FPR. Tôi chọn ngưỡng có ~0.6% FP mỗi chỉ số (2.5σ một phía, 2.75σ hai phía). Ở private, một FP mất ~0.2 điểm (30/146) còn một lỗi sót mất ~0.9 điểm (50/54), và FPR = 0 cho thấy tôi vẫn còn quá thận trọng. Lần sau tôi sẽ hạ ngưỡng các chỉ số liên tục về khoảng 2σ, chấp nhận vài FP, và kiểm chứng bằng FPR trên 208 sự kiện sạch của practice + public trước khi chạy private.

**Nếu có thêm thời gian, tôi sẽ cải tiến thống kê nào trong `ctx.state`?**

1. **Học cả σ trực tuyến** (median + MAD) thay vì tin σ suy ra từ baseline. Baseline `feature_mean_shift_sigma_max` (0.41) đã thấp hơn dao động sạch thực tế (0.47); các baseline khác có thể lệch tương tự trên private.
2. **EWMA / CUSUM theo từng chỉ số** để bắt lỗi trôi dần qua nhiều batch, khi từng batch riêng lẻ chưa vượt ngưỡng.
3. **Đưa `std_amount` vào composite** cho `distribution_shift` (hiện đang bỏ qua vì baseline không có), học phân phối của nó trực tuyến.
4. **Ngưỡng theo Bayes:** ước lượng tỷ lệ lỗi của từng loại event trong state và chọn ngưỡng tối ưu theo chi phí FP/FN, thay vì cố định 2.5σ cho mọi chỉ số.
