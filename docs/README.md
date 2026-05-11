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