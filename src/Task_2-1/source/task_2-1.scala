import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

// Spark trên Windows có thể gọi chmod qua Hadoop local filesystem.
// Override setPermission để không cần Hadoop cluster thật.
class WinLocalFileSystemTask21 extends RawLocalFileSystem {
  override def setPermission(p: Path, permission: FsPermission): Unit = {}
}

object Task21 {

  def main(args: Array[String]): Unit = {
    // Dùng đường dẫn tương đối để không cần tự set HADOOP_HOME.
    val hadoopHome = new File("hadoop").getCanonicalPath
    System.setProperty("hadoop.home.dir", hadoopHome)

    val spark = SparkSession.builder()
      .appName("Lab3_Task21_City_Cancelled_Percentage")
      .master("local[*]")
      .config("spark.hadoop.fs.file.impl", classOf[WinLocalFileSystemTask21].getName)
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val csvPath = if (args.nonEmpty) args(0) else "../../data/data.csv"
    val outputFile = if (args.length >= 2) args(1) else "Task_2-1.parquet"
    val outputDir = outputFile + "_tmp"

    println(s"File du lieu: $csvPath")
    println(s"File ket qua: $outputFile")

    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    // Chuẩn hóa dữ liệu một lần rồi cache vì các nhánh tính toán dùng lại nhiều lần.
    val cleaned = prepareOrders(rawDF).cache()
    val rawCount = rawDF.count()
    val cleanCount = cleaned.count()
    println(s"Da doc $rawCount dong, giu lai $cleanCount dong sau khi lam sach.")

    // Promotion hợp lệ nếu khoảng cách giữa lần xuất hiện đầu và cuối ít nhất 2 ngày.
    val validPromotions = computeValidPromotions(cleaned).cache()
    val validPromotionCount = validPromotions.count()
    println(s"So promotion hop le theo thoi gian: $validPromotionCount")

    val validPromoCountsByOrder = computeValidPromotionCounts(cleaned, validPromotions)
    val stateAverageAmounts = computeStateAverageAmounts(cleaned)
    val finalResult = computeCityCancellationPercentages(
      cleaned,
      validPromoCountsByOrder,
      stateAverageAmounts
    )

    println("\n" + "=" * 80)
    println("Execution plan mo rong de dua vao bao cao")
    println("=" * 80)
    finalResult.explain(true)

    val physicalPlan = finalResult.queryExecution.executedPlan.toString()
    printPlanDiagnostics(physicalPlan)

    val materialized = finalResult.cache()
    spark.sparkContext.setJobGroup("task21-final-count", "Task 2-1 final result materialization")
    val resultRows = materialized.count()
    val countStageIds = stageIdsForGroup(spark, "task21-final-count")
    spark.sparkContext.clearJobGroup()

    println(s"So dong ket qua: $resultRows")
    println(s"So stage Spark khi materialize ket qua: ${countStageIds.length}")
    println(s"Stage IDs: ${countStageIds.sorted.mkString(", ")}")

    materialized
      .orderBy("ship_state", "ship_city")
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(outputDir)

    copySingleParquetFile(outputDir, outputFile)

    materialized.unpersist()
    validPromotions.unpersist()
    cleaned.unpersist()
    spark.stop()
  }

  private def prepareOrders(rawDF: DataFrame): DataFrame = {
    // Tách promotion-ids thành mảng, bỏ phần tử rỗng để đếm chính xác số mã khuyến mãi.
    val promoArray = filter(
      transform(
        split(coalesce(col("promotion-ids"), lit("")), ","),
        promo => trim(promo)
      ),
      promo => length(promo) > 0
    )

    rawDF
      .withColumn("row_id", monotonically_increasing_id())
      .withColumn("order_date", to_date(col("Date"), "MM-dd-yy"))
      .withColumn("amount", col("Amount").cast("double"))
      .withColumn("status_norm", lower(trim(col("Status"))))
      .withColumn("fulfilment_norm", lower(trim(col("Fulfilment"))))
      .withColumn("service_level_norm", lower(trim(col("ship-service-level"))))
      .withColumn("courier_status_norm", lower(trim(col("Courier Status"))))
      .withColumn("ship_city", upper(trim(col("ship-city"))))
      .withColumn("ship_state", upper(trim(col("ship-state"))))
      .withColumn("promotion_array", promoArray)
      .filter(
        col("row_id").isNotNull &&
        col("order_date").isNotNull &&
        col("amount").isNotNull &&
        col("ship_city").isNotNull &&
        col("ship_state").isNotNull &&
        length(col("ship_city")) > 0 &&
        length(col("ship_state")) > 0
      )
  }

  private def computeValidPromotions(cleaned: DataFrame): DataFrame = {
    cleaned
      .select(col("order_date"), explode(col("promotion_array")).alias("promotion_id"))
      .groupBy("promotion_id")
      .agg(
        min(col("order_date")).alias("first_seen_date"),
        max(col("order_date")).alias("last_seen_date")
      )
      .withColumn("active_days", datediff(col("last_seen_date"), col("first_seen_date")))
      .filter(col("active_days") >= 2)
      .select("promotion_id", "first_seen_date", "last_seen_date", "active_days")
  }

  private def computeValidPromotionCounts(cleaned: DataFrame, validPromotions: DataFrame): DataFrame = {
    // Join với danh sách promotion hợp lệ rồi đếm số promotion hợp lệ của từng order.
    cleaned
      .select(col("row_id"), explode(col("promotion_array")).alias("promotion_id"))
      .join(validPromotions.select("promotion_id"), Seq("promotion_id"), "inner")
      .groupBy("row_id")
      .agg(countDistinct("promotion_id").alias("valid_promotion_count"))
  }

  private def computeStateAverageAmounts(cleaned: DataFrame): DataFrame = {
    // Ngưỡng amount được tính theo từng state từ đơn Merchant có courier status Shipped.
    cleaned
      .filter(
        col("fulfilment_norm") === "merchant" &&
        col("courier_status_norm") === "shipped"
      )
      .groupBy("ship_state")
      .agg(avg(col("amount")).alias("state_avg_merchant_shipped_amount"))
  }

  private def computeCityCancellationPercentages(
      cleaned: DataFrame,
      validPromoCountsByOrder: DataFrame,
      stateAverageAmounts: DataFrame
  ): DataFrame = {
    val enriched = cleaned
      .join(validPromoCountsByOrder, Seq("row_id"), "left")
      .withColumn("valid_promotion_count", coalesce(col("valid_promotion_count"), lit(0L)))
      .join(stateAverageAmounts, Seq("ship_state"), "left")

    // Mẫu số: đơn Standard thỏa điều kiện promotion và amount nhỏ hơn trung bình state.
    val qualifiedStandardOrders = enriched.filter(
      col("service_level_norm") === "standard" &&
      col("valid_promotion_count") >= 3 &&
      col("state_avg_merchant_shipped_amount").isNotNull &&
      col("amount") < col("state_avg_merchant_shipped_amount")
    )

    qualifiedStandardOrders
      .groupBy("ship_city", "ship_state")
      .agg(
        count(lit(1)).alias("qualified_standard_order_count"),
        sum(
          when(col("status_norm").contains("cancelled"), lit(1L))
            .otherwise(lit(0L))
        ).alias("cancelled_order_count"),
        first(col("state_avg_merchant_shipped_amount")).alias("state_avg_merchant_shipped_amount")
      )
      .withColumn(
        "cancelled_percentage",
        round(
          col("cancelled_order_count").cast("double") * lit(100.0) /
            col("qualified_standard_order_count").cast("double"),
          4
        )
      )
      .select(
        col("ship_city"),
        col("ship_state"),
        col("qualified_standard_order_count"),
        col("cancelled_order_count"),
        col("cancelled_percentage"),
        col("state_avg_merchant_shipped_amount")
      )
  }

  private def printPlanDiagnostics(physicalPlan: String): Unit = {
    // Các thông tin này dùng để đưa vào report.
    val exchangeCount = "\\bExchange\\b".r.findAllMatchIn(physicalPlan).length
    val joinStrategies = Seq(
      "BroadcastHashJoin",
      "SortMergeJoin",
      "ShuffledHashJoin",
      "BroadcastNestedLoopJoin",
      "CartesianProduct"
    ).filter(physicalPlan.contains)

    println("\n" + "=" * 80)
    println("Thong tin plan de dua vao bao cao")
    println("=" * 80)
    println(s"Chien luoc join Spark chon: ${joinStrategies.mkString(", ")}")
    println(s"So Exchange node trong physical plan: $exchangeCount")
  }

  private def stageIdsForGroup(spark: SparkSession, groupId: String): Array[Int] = {
    val tracker = spark.sparkContext.statusTracker
    tracker.getJobIdsForGroup(groupId).flatMap { jobId =>
      tracker.getJobInfo(jobId).map(_.stageIds()).getOrElse(Array.empty[Int])
    }.distinct
  }

  private def copySingleParquetFile(outputDir: String, outputFile: String): Unit = {
    // Spark luôn ghi ra thư mục part-*.parquet; đoạn này copy part file ra đúng tên cần nộp.
    val tmpDir = new File(outputDir)
    val partFile = Option(tmpDir.listFiles())
      .getOrElse(Array.empty)
      .find(file => file.getName.startsWith("part-") && file.getName.endsWith(".parquet"))
      .getOrElse(throw new RuntimeException(s"No part-*.parquet file found in $outputDir"))

    val destFile = new File(outputFile)
    val parent = destFile.getParentFile
    if (parent != null) parent.mkdirs()
    if (destFile.exists()) deleteRecursively(destFile)

    Files.copy(partFile.toPath, destFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    deleteRecursively(tmpDir)
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).getOrElse(Array.empty).foreach(deleteRecursively)
    }
    if (!file.delete() && file.exists()) {
      throw new RuntimeException(s"Unable to delete ${file.getAbsolutePath}")
    }
  }
}
