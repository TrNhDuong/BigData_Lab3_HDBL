# Task 1-1 — How to Run

## Yêu cầu
- Java JDK 17
- sbt 1.x
- Apache Hadoop 3.3.6

## Build

```bash
cd src/Task1-1
sbt assembly
```

JAR tạo ra tại: `target/scala-2.12/task1-1-hadoop-assembly-1.0.jar`

## Chạy

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

## Kết quả

File CSV nằm tại local filesystem: `/output/task1-1/Task_1-1.csv`

```
window_date,state,most_bought_size,purchase_count
2022-04-08,ANDHRA PRADESH,M,12
...
```

# Task 1-2

## Build
```bash
cd src/Task1-2
sbt assembly
```

JAR tạo ra tại: `target/scala-2.12/task1-2-assembly-1.0.jar`

## Run
Giả sử đã có file data trên HDFS tại `/usr/khang/input/data.csv`

```bash
hadoop jar target/scala-2.12/task1-2-assembly-1.0.jar \
  /usr/khang/input/data.csv \
  /tmp/task1-2-out
```

- Tham số thứ nhất: đường dẫn file CSV **trên HDFS**
- Tham số thứ hai: đường dẫn **trên HDFS** dùng để lưu các thư mục trung gian của từng job (`job1_tmp`, `job2_tmp`, `job3_tmp`) — các thư mục này sẽ bị xóa tự động sau khi chạy xong.

Kết quả cuối được lưu trên **local filesystem** tại: `/tmp/task1-2-out/Task_1-2.csv`

## Result
```
month	    state	            median_variety
2022-03	    Andhra Pradesh	    1
2022-03	    Chandigarh	        1
...
```