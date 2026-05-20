# BigData Lab 3 — Documentation

## Cấu trúc thư mục

```text
23120243
|--- data
|    |--- data.csv          # File dữ liệu gốc (Amazon Sale Report.csv)
|--- docs
|    |--- README.md         # File hướng dẫn chạy code và giải thích
|    |--- drive_link.txt    # File text chứa Link Google Drive nộp bài
|--- src
|    |--- Task_1-1
|    |    |--- build.sbt    # Cấu hình biên dịch SBT, khai báo thư viện Hadoop và plugin
|    |    |--- project
|    |    |    |--- build.properties # Khai báo phiên bản sbt sử dụng
|    |    |    |--- plugins.sbt      # Chứa sbt-assembly để đóng gói ra file JAR
|    |    |--- source
|    |    |    |--- task_1-1.scala   # Mã nguồn Scala thực thi thuật toán MapReduce
|    |--- Task_1-2
|    |    |--- build.sbt    # Cấu hình biên dịch SBT, khai báo thư viện Hadoop và plugin
|    |    |--- project
|    |    |    |--- build.properties # Khai báo phiên bản sbt sử dụng
|    |    |    |--- plugins.sbt      # Chứa sbt-assembly để đóng gói ra file JAR
|    |    |--- source
|    |    |    |--- task_1-2.scala   # Mã nguồn Scala thực thi thuật toán MapReduce
|    |--- Task_2-1
|    |    |--- build.sbt    # Cấu hình biên dịch SBT, khai báo Spark và tự động dẫn config Hadoop local
|    |    |--- hadoop
|    |    |    |--- bin     # Các file hỗ trợ Spark chạy local trên Windows (winutils.exe, hadoop.dll)
|    |    |--- project
|    |    |    |--- build.properties # Khai báo phiên bản sbt sử dụng
|    |    |    |--- plugins.sbt      # Chứa plugin SBT
|    |    |--- source
|    |    |    |--- task_2-1.scala   # Mã nguồn Scala thực thi Spark Structured APIs
|    |--- Task_2-2
|    |    |--- build.sbt    # Cấu hình biên dịch SBT, khai báo Spark và tự động dẫn config Hadoop
|    |    |--- hadoop
|    |    |    |--- bin     # Các file binaries hỗ trợ chạy Spark/Hadoop Local trên Windows (winutils.exe, hadoop.dll...)
|    |    |--- project
|    |    |    |--- build.properties # Khai báo phiên bản sbt sử dụng
|    |    |    |--- plugins.sbt      # Chứa plugin SBT
|    |    |--- source
|    |    |    |--- task_2-2.scala   # Mã nguồn Scala thực thi Spark DataFrame/SQL tính toán độ lệch chuẩn
```

## Yêu cầu môi trường

- Java JDK 17
- sbt 1.x
- Apache Hadoop 3.3.6

---

# Task 1-1 — Most Bought Size per State per Rolling Window

**Mô tả:** Đối với mỗi khung thời gian 7 ngày (rolling window) và mỗi tiểu bang, tìm kích cỡ quần áo được mua thường xuyên nhất.

## Build

```bash
cd src/Task_1-1
sbt assembly
```

File JAR đầu ra: `target/scala-2.12/task1-1-hadoop-assembly-1.0.jar`

## How to Run

```bash
# 1. Copy data lên HDFS
hdfs dfs -mkdir -p /data
hdfs dfs -put /path/to/asr.csv /data/asr.csv

# 2. Chạy job
hadoop jar target/scala-2.12/task1-1-hadoop-assembly-1.0.jar \
    Task1_1Driver \
    /data/asr.csv \
    /output/task1-1
```

## Output Schema

File CSV được lưu tại thư mục local: `/output/task1-1/Task_1-1.csv`

| Cột                | Kiểu dữ liệu | Mô tả                           |
| ------------------ | ------------ | ------------------------------- |
| `window_date`      | Date         | Ngày bắt đầu của rolling window |
| `state`            | String       | Tên tiểu bang                   |
| `most_bought_size` | String       | Size được mua nhiều nhất        |

**Ví dụ:**

```
window_date,state,most_bought_size
2022-04-08,ANDHRA PRADESH,M
...
```

---

# Task 1-2 — Monthly Median Variety per State

**Mô tả:** Lọc các giao dịch mua sản phẩm có kích cỡ lớn (từ XXL trở lên). Đối với mỗi tháng và mỗi tiểu bang, tính giá trị trung vị (median) của số lượng SKU phân biệt (variety) được mua cho mỗi kiểu dáng (style).

## Build

```bash
cd src/Task_1-2
sbt assembly
```

File JAR đầu ra: `target/scala-2.12/task1-2-assembly-1.0.jar`

## How to Run

Giả sử đã có file data trên HDFS tại `/usr/khang/input/data.csv`:

```bash
hadoop jar target/scala-2.12/task1-2-assembly-1.0.jar \
  /usr/khang/input/data.csv \
  /tmp/task1-2-out
```

- **Tham số 1**: Đường dẫn file CSV trên HDFS
- **Tham số 2**: Đường dẫn trên HDFS để lưu các thư mục trung gian (`job1_tmp`, `job2_tmp`, `job3_tmp`) — sẽ tự động xóa sau khi chạy xong

Kết quả cuối cùng lưu tại thư mục local: `/tmp/task1-2-out/Task_1-2.csv`

## Output Schema

| Cột              | Kiểu dữ liệu | Mô tả                                |
| ---------------- | ------------ | ------------------------------------ |
| `month`          | String       | Năm-tháng (YYYY-MM)                  |
| `state`          | String       | Tên tiểu bang                        |
| `median_variety` | Double       | Trung vị số lượng category phân biệt |

**Ví dụ:**

```
month       state               median_variety
2022-03     Andhra Pradesh      1
2022-03     Chandigarh          1
...
```

---

# Task 2-1 — City-level Cancelled Percentage with Promotion and Amount Filters

**Mô tả:** Sử dụng Spark Structured APIs để tính tỷ lệ đơn hàng bị hủy theo từng thành phố và tiểu bang. Chỉ xét các đơn hàng `Standard` có ít nhất 3 mã khuyến mãi hợp lệ và có `Amount` thấp hơn giá trị trung bình của bang đó. Giá trị trung bình của bang được tính trên các đơn hàng do `merchant` thực hiện và có trạng thái giao hàng `shipped`.

**Logic chính:**

- Promotion hợp lệ là promotion có khoảng cách giữa lần xuất hiện đầu tiên và cuối cùng từ 2 ngày trở lên.
- Mỗi đơn hàng được đếm số promotion hợp lệ phân biệt.
- Kết quả được ghi ra 1 file Parquet duy nhất.
- Khi chạy, chương trình có in `explain(true)`, join strategy, số `Exchange` và số stage để đưa vào báo cáo.

## Build & Run

Thư mục `hadoop/bin` trong `Task_2-1` chỉ chứa `winutils.exe` và `hadoop.dll` để Spark chạy được trên Windows local, chỉ cần cài Java và sbt, sau đó chạy lệnh:

```bash
cd src/Task_2-1
sbt run
```

Mặc định chạy với input `../../data/data.csv` và ghi output ra:

```text
src/Task_2-1/Task_2-1.parquet
```

## Output Schema

| Cột                                 | Kiểu dữ liệu | Mô tả                                                 |
| ----------------------------------- | ------------ | ----------------------------------------------------- |
| `ship_city`                         | String       | Thành phố nhận hàng                                   |
| `ship_state`                        | String       | Tiểu bang nhận hàng                                   |
| `qualified_standard_order_count`    | Long         | Số đơn hàng thỏa điều kiện lọc                        |
| `cancelled_order_count`             | Long         | Số đơn hàng bị hủy trong nhóm thỏa điều kiện          |
| `cancelled_percentage`              | Double       | Tỷ lệ đơn bị hủy theo phần trăm                       |
| `state_avg_merchant_shipped_amount` | Double       | Amount trung bình của bang với đơn merchant + shipped |

---

# Task 2-2 — SKU Monthly Amount StdDev with Dynamic Percentile Filtering

**Mô tả:** Đối với mỗi SKU trong từng tháng, tính độ lệch chuẩn (standard deviation) của doanh thu các đơn hàng có số lượng khuyến mãi đạt ngưỡng phân vị động (P90 và P80). So sánh cách dùng hàm xấp xỉ `approx_percentile` của Spark với cách tự cài đặt phân vị chính xác dùng hàm `cume_dist()`.

**Logic:**

- **P90**: Các đơn hàng có số lượng khuyến mãi ≥ bách phân vị thứ 90 trong nhóm SKU-tháng đó
- **P80**: Các đơn hàng có số lượng khuyến mãi ≥ bách phân vị thứ 80 trong nhóm SKU-tháng đó
- **Ngoại lệ (Edge Case)**: Nếu có ít hơn 2 đơn hàng thỏa mãn điều kiện lọc, độ lệch chuẩn được gán bằng `0.0`

## Build & Run

Không cần cài đặt Apache Spark thủ công và cũng **không cần build ra file JAR**. `sbt` sẽ tự động tải thư viện, biên dịch và chạy code dựa vào file `build.sbt`. Lỗi thư viện Hadoop DLL trên môi trường Windows cũng đã được xử lý trong cấu hình project.

```bash
cd src/Task_2-2
sbt run
```

Mặc định đọc dataset từ `../../data/data.csv`.

## Output Schema

Kết quả được lưu dưới dạng file Parquet: `src/Task_2-2/Task_2-2.parquet`

| Cột              | Kiểu dữ liệu | Mô tả                                            |
| ---------------- | ------------ | ------------------------------------------------ |
| `SKU`            | String       | Mã sản phẩm (Stock Keeping Unit)                 |
| `year`           | Int          | Năm của đơn hàng                                 |
| `month`          | Int          | Tháng của đơn hàng                               |
| `std_p90_approx` | Double       | Độ lệch chuẩn (StdDev) dùng ngưỡng P90 xấp xỉ    |
| `std_p80_approx` | Double       | Độ lệch chuẩn (StdDev) dùng ngưỡng P80 xấp xỉ    |
| `std_p90_exact`  | Double       | Độ lệch chuẩn (StdDev) dùng ngưỡng P90 chính xác |
| `std_p80_exact`  | Double       | Độ lệch chuẩn (StdDev) dùng ngưỡng P80 chính xác |

**Ví dụ:**

```
SKU         year    month   std_p90_approx      std_p80_approx      std_p90_exact       std_p80_exact
SKU_123     2022    3       145.67              120.45              145.67              120.45
SKU_456     2022    3       0.0                 45.2                0.0                 45.2
...
```
