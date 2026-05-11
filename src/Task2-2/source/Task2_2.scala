/**
 * Task 2-2: SKU Monthly Amount Standard Deviation with Dynamic Percentile Filtering.
 *
 * For each SKU within each month, compute the population standard deviation of
 * order amounts whose promotion count meets a dynamic percentile threshold (P90 / P80).
 *
 * Two approaches are implemented and benchmarked:
 *   A) Spark built-in percentile_approx (approximate, fast)
 *   B) Exact percentile via cume_dist() window function (exact, heavier)
 *
 * Requirements satisfied:
 *   - Scala + Spark Structured APIs only (no Spark SQL string queries)
 *   - Benchmarked over 5 runs with mean and std dev reported
 *   - explain(true) included for execution plan analysis
 *   - Output: single Task_2-2.parquet under normal filesystem
 */

import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.io.File
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission
import java.nio.file.{Files, StandardCopyOption}

// On Windows, Spark's LocalFileSystem tries to call winutils.exe for chmod,
// which fails if Hadoop binaries are not installed. We override setPermission
// to be a no-op so the job can run without a full Hadoop installation.
class WinLocalFileSystem extends RawLocalFileSystem {
  override def setPermission(p: Path, permission: FsPermission): Unit = {}
}

object Task22 {

  def main(args: Array[String]): Unit = {

    // Point Hadoop home to a dummy local folder to suppress HADOOP_HOME warnings on Windows.
    val projectRoot = new java.io.File(".").getAbsoluteFile.getParent
    System.setProperty("hadoop.home.dir", projectRoot + java.io.File.separator + "hadoop")

    val spark = SparkSession.builder()
      .appName("Lab3_Task22_SKU_Monthly_Analysis")
      .master("local[*]")
      .config("spark.hadoop.fs.file.impl", classOf[WinLocalFileSystem].getName)
      // Reduce shuffle partitions from the default 200 to something reasonable
      // for a dataset this size; 8 avoids excessive small-task overhead.
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val csvPath  = if (args.nonEmpty) args(0) else "Amazon Sale Report.csv"
    val outputDir  = "Task_2-2_tmp"
    val outputFile = "Task_2-2.parquet"

    println("\n[INFO] Starting Task 2.2 Processing...")

    // -------------------------------------------------------------------------
    // 1. Load raw CSV
    // -------------------------------------------------------------------------
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    // -------------------------------------------------------------------------
    // 2. Preprocessing
    //
    //  - Parse "Date" column (format MM-dd-yy) into a proper DateType so we can
    //    extract year and month for grouping.
    //
    //  - Compute promo_count: the number of promotion identifiers on each order.
    //    The "promotion-ids" field stores a comma-separated list of IDs, including
    //    Amazon-issued ones (e.g. "Amazon PLCC Free-Financing...").
    //    Splitting on "," and taking the size gives the exact count per order.
    //    Null or blank → 0 (no promotions).
    //
    //  - Cast Amount to Double (inferSchema may read it as String in some locales).
    //
    //  - Drop rows where date, SKU, or Amount is missing — they cannot be grouped
    //    or contribute to a meaningful standard deviation.
    //
    //  - cache() because this cleaned DataFrame is reused many times across
    //    both approaches and all benchmark runs.
    // -------------------------------------------------------------------------
    val df = rawDF
      .withColumn("parsed_date", to_date(col("Date"), "MM-dd-yy"))
      .withColumn("year",  year(col("parsed_date")))
      .withColumn("month", month(col("parsed_date")))
      .withColumn("promo_count",
        when(col("promotion-ids").isNull || trim(col("promotion-ids")) === "", lit(0L))
          .otherwise(size(split(trim(col("promotion-ids")), ",")).cast("long"))
      )
      .withColumn("Amount", col("Amount").cast("double"))
      .filter(col("parsed_date").isNotNull && col("SKU").isNotNull && col("Amount").isNotNull)
      .cache()

    // -------------------------------------------------------------------------
    // 3. Helper: computeStd
    //
    // Given a DataFrame that already has both the order rows and their threshold
    // column (either p90 or p80), this function:
    //   a) Keeps only orders whose promo_count is >= the threshold for that group.
    //   b) Groups by SKU / year / month and computes population stddev (df=0).
    //   c) If fewer than 2 orders survive the filter, stddev is forced to 0.0
    //      (a single-element population has stddev = 0 mathematically, and the
    //      lab explicitly requires this edge-case handling).
    //   d) stddev_pop returns null when the group has exactly 1 row; coalesce
    //      converts that null to 0.0 as a safety net.
    // -------------------------------------------------------------------------
    def computeStd(
        joined:     org.apache.spark.sql.DataFrame,
        threshCol:  String,
        stdColName: String,
        cntColName: String
    ): org.apache.spark.sql.DataFrame = {
      joined
        .filter(col("promo_count") >= col(threshCol))
        .groupBy("SKU", "year", "month")
        .agg(
          count("*").alias(cntColName),
          stddev_pop(col("Amount")).alias(stdColName + "_raw")
        )
        .withColumn(
          stdColName,
          when(col(cntColName) < 2, lit(0.0))
            .otherwise(coalesce(col(stdColName + "_raw"), lit(0.0)))
        )
        .drop(stdColName + "_raw")
    }

    // Pre-build the full set of (SKU, year, month) combinations that appear in
    // the data. This acts as the "spine" for left joins later, ensuring every
    // SKU-month shows up in the output even if no orders pass the percentile
    // filter (those get stddev = 0.0 via coalesce).
    val allSkuMonths = df.select("SKU", "year", "month").distinct().cache()

    // -------------------------------------------------------------------------
    // 4. Benchmarking — 9 runs (lab requires at least 5; we use 9 for a more
    // stable mean and smaller variance in the reported standard deviation).
    //
    // Each run times Approach A and Approach B independently.
    // count() is the action that forces Spark to actually execute the DAG;
    // without it, Spark's lazy evaluation means nothing runs yet.
    // File I/O is intentionally excluded from timing — we want to measure
    // the computational cost of the percentile + stddev logic only.
    // -------------------------------------------------------------------------
    val numRuns   = 9
    val timesApprox = new scala.collection.mutable.ArrayBuffer[Long]()
    val timesExact  = new scala.collection.mutable.ArrayBuffer[Long]()
    var finalResultDF: org.apache.spark.sql.DataFrame = null

    for (run <- 1 to numRuns) {
      println(s"[Trial $run/$numRuns] Computing thresholds and statistics...")

      // -----------------------------------------------------------------------
      // Approach A: Built-in percentile_approx
      //
      // percentile_approx uses the Greenwald-Khanna algorithm internally.
      // The third argument (accuracy = 1_000_000) pushes it toward near-exact
      // results at the cost of slightly more memory — acceptable trade-off here.
      //
      // Steps:
      //   1. Compute P90 and P80 thresholds per SKU-month group.
      //   2. Join thresholds back to the order rows.
      //   3. Call computeStd twice (once for P90, once for P80).
      //   4. Left-join both results onto allSkuMonths so every group is present.
      //   5. Fill nulls with 0.0 for groups where no orders passed the filter.
      // -----------------------------------------------------------------------
      val t1Start = System.currentTimeMillis()

      val threshApprox = df
        .groupBy("SKU", "year", "month")
        .agg(
          percentile_approx(col("promo_count"), lit(0.90), lit(1000000)).alias("p90_thresh"),
          percentile_approx(col("promo_count"), lit(0.80), lit(1000000)).alias("p80_thresh")
        )

      val dfA = df.join(threshApprox, Seq("SKU", "year", "month"), "inner")
      val p90A = computeStd(dfA, "p90_thresh",  "std_p90_approx", "cnt_p90_approx")
      val p80A = computeStd(dfA, "p80_thresh",  "std_p80_approx", "cnt_p80_approx")

      val approxResult = allSkuMonths
        .join(p90A, Seq("SKU", "year", "month"), "left")
        .join(p80A, Seq("SKU", "year", "month"), "left")
        .withColumn("std_p90_approx", coalesce(col("std_p90_approx"), lit(0.0)))
        .withColumn("std_p80_approx", coalesce(col("std_p80_approx"), lit(0.0)))

      approxResult.count() // trigger execution — this is what we are timing
      timesApprox += (System.currentTimeMillis() - t1Start)

      // -----------------------------------------------------------------------
      // Approach B: Exact percentile via cume_dist()
      //
      // cume_dist() computes the cumulative distribution value for each row
      // within its partition (SKU / year / month), ordered by promo_count.
      // It gives the fraction of rows with promo_count <= current row's value.
      //
      // To find the P90 threshold we take the minimum promo_count value whose
      // cume_dist is >= 0.90 — this is the exact lower bound of the top 10%.
      //
      // This is exact (no approximation) but requires a full sort per partition,
      // making it more expensive than percentile_approx on large groups.
      // -----------------------------------------------------------------------
      val t2Start = System.currentTimeMillis()

      val winSpec = Window.partitionBy("SKU", "year", "month").orderBy("promo_count")
      val dfWithCumeDist = df.withColumn("cume_dist_val", cume_dist().over(winSpec))

      val threshExact = dfWithCumeDist
        .groupBy("SKU", "year", "month")
        .agg(
          // min(when(...)) picks the smallest promo_count value that sits at or
          // above the target quantile — equivalent to the standard percentile def.
          min(when(col("cume_dist_val") >= 0.90, col("promo_count"))).alias("p90_thresh_exact"),
          min(when(col("cume_dist_val") >= 0.80, col("promo_count"))).alias("p80_thresh_exact")
        )

      val dfE = df.join(threshExact, Seq("SKU", "year", "month"), "inner")
      val p90E = computeStd(dfE, "p90_thresh_exact", "std_p90_exact", "cnt_p90_exact")
      val p80E = computeStd(dfE, "p80_thresh_exact", "std_p80_exact", "cnt_p80_exact")

      val exactResult = allSkuMonths
        .join(p90E, Seq("SKU", "year", "month"), "left")
        .join(p80E, Seq("SKU", "year", "month"), "left")
        .withColumn("std_p90_exact", coalesce(col("std_p90_exact"), lit(0.0)))
        .withColumn("std_p80_exact", coalesce(col("std_p80_exact"), lit(0.0)))

      exactResult.count() // trigger execution
      timesExact += (System.currentTimeMillis() - t2Start)

      // On the last run, print execution plans and build the final merged result.
      // explain(true) shows the full physical plan including join strategies,
      // Exchange (shuffle) nodes, and stage boundaries — required for the report.
      if (run == numRuns) {
        println("\n[INFO] Generating execution plans for report...")
        println("--- APPROX APPROACH PLAN ---")
        approxResult.explain(true)
        println("\n--- EXACT APPROACH PLAN ---")
        exactResult.explain(true)

        // Merge both approach results into one DataFrame.
        // Inner join is safe here because both sides derive from the same
        // allSkuMonths spine, so they have identical (SKU, year, month) keys.
        finalResultDF = approxResult
          .join(exactResult, Seq("SKU", "year", "month"), "inner")
          .select(
            col("SKU"),
            col("year"),
            col("month"),
            col("std_p90_approx"),
            col("std_p80_approx"),
            col("std_p90_exact"),
            col("std_p80_exact")
          )
      }
    }

    // -------------------------------------------------------------------------
    // 5. Benchmark summary
    //
    // Population std dev is used here (same formula as the lab requires for
    // the order amounts) — dividing by N, not N-1.
    // -------------------------------------------------------------------------
    def stats(data: Seq[Long]): (Double, Double) = {
      val mean = data.sum.toDouble / data.size
      val std  = Math.sqrt(data.map(x => Math.pow(x - mean, 2)).sum / data.size)
      (mean, std)
    }

    val (m1, s1) = stats(timesApprox)
    val (m2, s2) = stats(timesExact)

    println("\n" + "=" * 70)
    println(f"BENCHMARK SUMMARY (N=$numRuns)")
    println("-" * 70)
    println(f"Approx Percentile: Mean = $m1%10.2f ms | StdDev = $s1%10.2f ms")
    println(f"Exact Percentile : Mean = $m2%10.2f ms | StdDev = $s2%10.2f ms")
    println("=" * 70)

    // -------------------------------------------------------------------------
    // 6. Export result
    //
    // coalesce(1) merges all partitions into a single output file.
    // Spark writes to a temp directory first (Hadoop convention), then we copy
    // the part-*.parquet file to the final destination with the required name.
    // The temp directory is cleaned up afterwards.
    // -------------------------------------------------------------------------
    println("\n[INFO] Saving final results to " + outputFile)
    finalResultDF
      .orderBy("year", "month", "SKU")
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(outputDir)

    try {
      val tmpDir   = new File(outputDir)
      val partFile = tmpDir.listFiles()
        .find(f => f.getName.startsWith("part-") && f.getName.endsWith(".parquet"))
        .get
      val destFile = new File(outputFile)
      if (destFile.exists()) destFile.delete()
      Files.copy(partFile.toPath, destFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      tmpDir.listFiles().foreach(_.delete())
      tmpDir.delete()
      println("[SUCCESS] Exported Task_2-2.parquet successfully.")
    } catch {
      case e: Exception =>
        println("[WARNING] Could not rename part file automatically: " + e.getMessage)
    }

    spark.stop()
  }
}
