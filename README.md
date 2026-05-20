# Intro to Big Data - Lab 3

## Giới thiệu nhóm

Nhóm HDBL gồm có 4 thành viên

## Giới thiệu đồ án

Đây là đồ án Lab 3 của học phần **Intro to Big Data**. Đồ án sử dụng bộ dữ liệu thương mại điện tử Amazon Sale Report để thực hành xử lý dữ liệu lớn bằng Hadoop MapReduce và Apache Spark.

Mục tiêu chính của đồ án là xây dựng các chương trình phân tích dữ liệu bán hàng, bao gồm xử lý cửa sổ trượt, thống kê theo nhóm, lọc dữ liệu theo điều kiện nghiệp vụ và so sánh các chiến lược tính toán trên dữ liệu lớn.

## Nội dung thực hiện

| Task | Công nghệ | Mô tả ngắn |
| --- | --- | --- |
| Task 1-1 | Hadoop MapReduce, Scala | Tìm size được mua nhiều nhất theo từng bang trong cửa sổ trượt tối đa 7 ngày trước ngày hiện tại. |
| Task 1-2 | Hadoop MapReduce, Scala | Tính median variety theo tháng và bang cho các sản phẩm size lớn. |
| Task 2-1 | Apache Spark, Scala | Tính tỷ lệ đơn hàng bị hủy theo thành phố/bang với các điều kiện về promotion và amount. |
| Task 2-2 | Apache Spark, Scala | Tính độ lệch chuẩn amount theo SKU-tháng với ngưỡng phân vị P90/P80 và so sánh approximate với exact. |

## Cấu trúc thư mục

```text
.
|--- data/              # Dữ liệu đầu vào
|--- docs/              # Tài liệu hướng dẫn chạy và link nộp bài
|--- src/               # Mã nguồn các task
|    |--- Task_1-1/
|    |--- Task_1-2/
|    |--- Task_2-1/
|    |--- Task_2-2/
|--- README.md          # Giới thiệu tổng quan đồ án
```

## Tài liệu chi tiết

Hướng dẫn build, chạy chương trình và mô tả output schema nằm trong:

- [docs/README.md](docs/README.md)
- [docs/drive_link.txt](docs/drive_link.txt)

## Môi trường sử dụng

- Scala 2.12
- sbt 1.x
- Hadoop 3.3.6
- Apache Spark
- Java JDK 17
