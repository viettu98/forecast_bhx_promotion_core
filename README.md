### 📊 Promotions Forecast Data Pipeline (Bách Hóa Xanh)
#### 🎯 Mục tiêu dự án
Xây dựng pipeline xử lý dữ liệu tự động để trích xuất, làm sạch và tính toán tỷ lệ giảm giá thực tế (% giảm giá) của hàng loạt các chương trình khuyến mãi (CTKM) phức tạp. Dữ liệu đầu ra được tổng hợp theo cấp độ Siêu thị (Store level) và Trung tâm phân phối (DC level) nhằm phục vụ trực tiếp cho mô hình Dự báo Khuyến mãi (Promotions Forecasting).

#### 💡 Bối cảnh & Vấn đề (Problem Statement)
Trong giai đoạn hệ thống dữ liệu tiền Khuyến mãi chưa được tích hợp đầy đủ, việc tính toán mức độ ảnh hưởng của các CTKM gặp nhiều khó khăn do:

Dữ liệu thô từ hệ thống POS (POS168, POS510) bị phân mảnh, chứa nhiều dữ liệu rác, trùng lặp và thiếu đồng bộ.

Các chương trình khuyến mãi cực kỳ đa dạng và phức tạp (Giảm giá trực tiếp, Tặng quà, Bán kèm, Khuyến mãi trên tổng số lượng vs. Đích danh sản phẩm).

Thiếu các metrics chuẩn hóa (như % giảm giá thực tế) để đưa vào mô hình Machine Learning dự báo.

#### 🚀 Quy trình thực hiện (Workflow)
Dự án được chia thành 4 giai đoạn chính:

1. Thu thập dữ liệu (Data Extraction)
Trích xuất dữ liệu các CTKM từ báo cáo POS168 (theo scope sản phẩm, ngày, danh sách 144 stores thuộc DC Vĩnh Lộc).

Trích xuất dữ liệu giá bán, giá quà tặng từ POS510 (Khu vực giá 643 - chuẩn BHX).

Truy vấn Database để lấy thông số quy đổi cơ sở (exchangequantity) cho từng itemid và productid.

2. Tiền xử lý & Làm sạch dữ liệu (Data Cleaning)
Loại bỏ các dòng dữ liệu trùng lặp (duplicates) và các CTKM không hợp lệ (ngày kết thúc < ngày bắt đầu).

Lọc bỏ các siêu thị không đạt điều kiện (đóng cửa, chỉ bán online) để giữ lại tập siêu thị Offline chuẩn.

Loại bỏ các trường hợp gây nhiễu (noise/outliers) như: % giảm > 95% (do giá sản phẩm tặng kèm cao hơn giá sản phẩm chính), hoặc các dữ liệu thiếu hụt.

3. Xử lý Logic Nghiệp vụ & Transform Dữ liệu (Data Transformation - Core)
Thiết lập bộ logic phức tạp để tính toán chính xác % giảm giá dựa trên tổ hợp các điều kiện:

Phân loại CTKM: "Khuyến mãi trên tổng số lượng" vs "Khuyến mãi đích danh theo sản phẩm".

Hình thức & Chiết khấu: Giảm giá (giá trị/phần trăm), Tặng quà (cùng loại/khác loại), Bán kèm.

Xử lý các Case đặc thù (Edge Cases):

Cộng dồn khuyến mãi: Tính toán Actual_benefit (sử dụng rank, cumsum) cho các sản phẩm áp dụng nhiều CTKM giảm giá cùng lúc (cùng ngày, cùng store).

Kiểm soát tính hợp lý: Nhận dạng và xử lý các case bất thường như "mua số lượng nhiều nhưng % giảm lại ít hơn mua số lượng ít" (so sánh Min_benefit và Actual_benefit).

4. Tổng hợp & Xuất dữ liệu (Data Aggregation & Output)
Store Level: Tính toán % giảm giá trung bình (dựa trên Max và Min discount) cho từng itemid tại từng storeid theo chuỗi thời gian (datekey).

DC Level: Tiếp tục tổng hợp dữ liệu từ Store lên cấp độ Trung tâm phân phối (DC), tính toán % giảm DC và tỷ lệ siêu thị có áp dụng KM (% áp dụng DC).

Output: Tự động xuất ra các file cấu trúc chuẩn (file_km_store_level, file_km_dc_level) sẵn sàng để đưa vào train model.

🛠 Kỹ năng & Công cụ áp dụng (Skills & Technologies)
Kỹ năng: Data Cleaning, Feature Engineering, Data Aggregation, Xử lý Logic Nghiệp vụ Bán lẻ (Retail Business Logic).

Công cụ/Thư viện (Dự kiến): Python (Pandas, Numpy cho Data Manipulation), ETL process, Data Analytics.
