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
 *   - Benchmarked over 5+ runs with mean and std dev reported
 *   - explain(true) included for execution plan analysis
 *   - Diagnostic outputs to support report sections:
 *       * threshold accuracy comparison (approx vs exact)
 *       * qualifying-set differences between the two approaches
 *       * analysis of SKU-month groups containing > 1000 orders
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

    val csvPath  = if (args.nonEmpty) args(0) else "../../data/data.csv"
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
    val rawCount = rawDF.count()

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

    val cleanCount = df.count()
    println(s"[INFO] Loaded $rawCount rows, kept $cleanCount after cleaning " +
            s"(dropped ${rawCount - cleanCount} rows with null date/SKU/Amount).")

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
    //
    // We also expose the last run's threshold DataFrames (threshApproxFinal,
    // threshExactFinal) so the diagnostic section below can analyze them
    // without recomputing.
    // -------------------------------------------------------------------------
    val numRuns   = 9
    val timesApprox = new scala.collection.mutable.ArrayBuffer[Long]()
    val timesExact  = new scala.collection.mutable.ArrayBuffer[Long]()
    var finalResultDF: org.apache.spark.sql.DataFrame = null
    var threshApproxFinal: org.apache.spark.sql.DataFrame = null
    var threshExactFinal:  org.apache.spark.sql.DataFrame = null

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
      // This is the standard "nearest-rank / inverse-CDF" definition of
      // percentile and matches NumPy's "lower" interpolation method.
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

      // On the last run, print execution plans, build the final merged result,
      // and expose the threshold DataFrames for the diagnostic analysis below.
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

        // Save threshold DataFrames so the diagnostic section can reuse them
        // without recomputing the percentile_approx / cume_dist work.
        threshApproxFinal = threshApprox
        threshExactFinal  = threshExact
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

    // =========================================================================
    // 6. DIAGNOSTIC ANALYSIS FOR REPORT
    //
    // The lab report explicitly requires three pieces of analysis that are not
    // visible from the final parquet output alone:
    //
    //   (a) Accuracy: how do approx and exact thresholds differ?
    //   (b) Qualifying-set differences: which SKU-month groups produce
    //       different sets of qualifying orders under the two approaches?
    //   (c) Large groups: are there any SKU-month groups with > 1000 orders,
    //       and what would the partitioning implications be?
    //
    // We compute all three here, AFTER the benchmark, so they do not pollute
    // the timing measurements. Numbers are printed to console — the user can
    // copy them into the report directly.
    // =========================================================================

    println("\n" + "=" * 70)
    println("DIAGNOSTIC ANALYSIS (for report)")
    println("=" * 70)

    // -------------------------------------------------------------------------
    // 6.1 Threshold accuracy comparison (Approach A vs Approach B)
    //
    // We join the two threshold DataFrames on (SKU, year, month) and compute
    // the per-group difference (approx - exact). Useful summary statistics:
    //   - count of groups with diff != 0          → how often they disagree
    //   - mean / max absolute difference          → magnitude of disagreement
    //   - distribution of disagreement (>0 vs <0) → bias direction
    //
    // Note: percentile_approx returns a Long here because promo_count is Long.
    // cume_dist's threshold is also Long. Cast to Double before subtracting
    // just to be safe against any future schema changes.
    // -------------------------------------------------------------------------
    println("\n[6.1] Threshold accuracy comparison")
    println("-" * 70)

    val threshCompare = threshApproxFinal
      .join(threshExactFinal, Seq("SKU", "year", "month"), "inner")
      .withColumn("diff_p90", col("p90_thresh").cast("double") - col("p90_thresh_exact").cast("double"))
      .withColumn("diff_p80", col("p80_thresh").cast("double") - col("p80_thresh_exact").cast("double"))
      .cache()

    val totalGroups = threshCompare.count()
    val diffP90Cnt  = threshCompare.filter(col("diff_p90") =!= 0).count()
    val diffP80Cnt  = threshCompare.filter(col("diff_p80") =!= 0).count()

    println(f"Total SKU-month groups            : $totalGroups")
    println(f"Groups where P90 thresholds differ: $diffP90Cnt%d  (${100.0 * diffP90Cnt / totalGroups}%.2f%%)")
    println(f"Groups where P80 thresholds differ: $diffP80Cnt%d  (${100.0 * diffP80Cnt / totalGroups}%.2f%%)")

    // Aggregate statistics on the magnitude of disagreement.
    // We use abs(diff) so positive and negative gaps don't cancel each other.
    val diffStats = threshCompare.agg(
      avg(abs(col("diff_p90"))).alias("mean_abs_diff_p90"),
      max(abs(col("diff_p90"))).alias("max_abs_diff_p90"),
      avg(abs(col("diff_p80"))).alias("mean_abs_diff_p80"),
      max(abs(col("diff_p80"))).alias("max_abs_diff_p80")
    )
    println("\nMagnitude of disagreement (|approx - exact|):")
    diffStats.show(false)

    // Show up to 10 concrete examples where the thresholds disagree — useful
    // for the report to illustrate the kind of values involved.
    println("Examples of groups with disagreement (up to 10):")
    threshCompare
      .filter(col("diff_p90") =!= 0 || col("diff_p80") =!= 0)
      .select("SKU", "year", "month",
              "p90_thresh", "p90_thresh_exact", "diff_p90",
              "p80_thresh", "p80_thresh_exact", "diff_p80")
      .show(10, false)

    // -------------------------------------------------------------------------
    // 6.2 Qualifying-set differences
    //
    // For each group we count how many orders pass the P90 filter under each
    // approach. If the counts differ, the two approaches selected different
    // orders → potentially different stddev values.
    //
    // Even when the threshold values are identical, the resulting qualifying
    // sets must also be identical (same filter on same data), so non-zero
    // diff in counts implies non-zero diff in thresholds. Reporting both gives
    // the reader a concrete sense of downstream impact.
    // -------------------------------------------------------------------------
    println("\n[6.2] Qualifying-set differences (orders passing P90 filter)")
    println("-" * 70)

    val joinedA = df.join(threshApproxFinal, Seq("SKU", "year", "month"), "inner")
    val joinedE = df.join(threshExactFinal,  Seq("SKU", "year", "month"), "inner")

    val cntApprox = joinedA
      .filter(col("promo_count") >= col("p90_thresh"))
      .groupBy("SKU", "year", "month")
      .agg(count("*").alias("n_qualified_approx"))

    val cntExact = joinedE
      .filter(col("promo_count") >= col("p90_thresh_exact"))
      .groupBy("SKU", "year", "month")
      .agg(count("*").alias("n_qualified_exact"))

    // outer join to capture groups that have qualifying orders in one approach
    // but zero in the other (treated as null on the missing side → fill 0).
    val cntCompare = cntApprox
      .join(cntExact, Seq("SKU", "year", "month"), "outer")
      .withColumn("n_qualified_approx", coalesce(col("n_qualified_approx"), lit(0L)))
      .withColumn("n_qualified_exact",  coalesce(col("n_qualified_exact"),  lit(0L)))
      .withColumn("diff_count", col("n_qualified_approx") - col("n_qualified_exact"))
      .cache()

    val diffSetCnt = cntCompare.filter(col("diff_count") =!= 0).count()
    println(f"Groups with different qualifying-set sizes (P90): $diffSetCnt%d  " +
            f"(${100.0 * diffSetCnt / totalGroups}%.2f%% of all groups)")

    println("\nExamples (up to 10):")
    cntCompare
      .filter(col("diff_count") =!= 0)
      .orderBy(abs(col("diff_count")).desc)
      .show(10, false)

    // -------------------------------------------------------------------------
    // 6.3 Large groups (> 1000 orders) — partitioning analysis
    //
    // The lab requires discussion of: (a) whether manual repartitioning helps,
    // (b) chosen partition strategy, (c) how the group's data volume compares
    // to Spark's default partition size (~128 MB).
    //
    // We:
    //   1. Find all SKU-month groups with > 1000 orders.
    //   2. Estimate per-row size in bytes (rough): integers ~ 4-8 B, strings
    //      vary widely. For our columns (SKU, year, month, Amount,
    //      promotion-ids, etc.) a conservative average is ~ 200 bytes per row.
    //   3. Multiply count * 200 to estimate group volume; compare with 128 MB.
    //
    // The 200 B/row figure is a heuristic for the Amazon Sales dataset's
    // typical row width; the report should note this is an estimate, not an
    // exact measurement from Spark internals.
    // -------------------------------------------------------------------------
    println("\n[6.3] Large SKU-month groups (> 1000 orders)")
    println("-" * 70)

    val avgRowBytesEstimate = 200L           // rough estimate for this dataset
    val defaultPartitionMB  = 128.0
    val mbToBytes           = 1024.0 * 1024.0

    val groupSizes = df
      .groupBy("SKU", "year", "month")
      .agg(count("*").alias("n_orders"))
      .withColumn("est_size_mb",
        col("n_orders") * lit(avgRowBytesEstimate) / lit(mbToBytes))
      .cache()

    val largeGroups = groupSizes.filter(col("n_orders") > 1000)
    val nLarge = largeGroups.count()
    println(f"Number of SKU-month groups with > 1000 orders: $nLarge%d")

    if (nLarge > 0) {
      println("\nLargest groups (top 20 by order count):")
      largeGroups.orderBy(desc("n_orders")).show(20, false)

      val maxSizeMB = largeGroups.agg(max(col("est_size_mb"))).first().getDouble(0)
      println(f"Largest estimated group size: $maxSizeMB%.4f MB")
      println(f"Spark default partition size: $defaultPartitionMB%.1f MB")
      println(f"Ratio (largest group / default partition): ${maxSizeMB / defaultPartitionMB}%.6f")
      println("\nInterpretation hints for the report:")
      println("  - If ratio << 1, each group fits comfortably inside one partition.")
      println("    Default partitioning is already efficient; manual repartition by")
      println("    SKU would create many tiny tasks (overhead > benefit).")
      println("  - If ratio is close to or above 1, that group would span multiple")
      println("    partitions and benefit from repartition(col(\"SKU\")) so all rows")
      println("    of one SKU collocate on the same task (better data locality).")
    } else {
      println("\nNo group exceeds the 1000-order threshold — the dataset's largest")
      println("SKU-month groups are well below Spark's default 128 MB partition size,")
      println("so manual repartitioning would not provide measurable benefit and")
      println("would only add coordination overhead.")
    }

    // -------------------------------------------------------------------------
    // 6.4 Optional: cross-check stddev values where qualifying sets differ
    //
    // If approx and exact picked different orders, their stddev outputs may
    // also differ. Quick scan over the final merged DataFrame.
    // -------------------------------------------------------------------------
    println("\n[6.4] Final stddev differences (approx vs exact)")
    println("-" * 70)
    val stddevDiff = finalResultDF
      .withColumn("diff_std_p90", abs(col("std_p90_approx") - col("std_p90_exact")))
      .withColumn("diff_std_p80", abs(col("std_p80_approx") - col("std_p80_exact")))

    // tolerance 1e-9 avoids flagging floating-point noise as a real difference
    val tol = 1e-9
    val diffStdP90Cnt = stddevDiff.filter(col("diff_std_p90") > tol).count()
    val diffStdP80Cnt = stddevDiff.filter(col("diff_std_p80") > tol).count()
    println(f"Groups with non-trivial stddev_p90 disagreement (> $tol): $diffStdP90Cnt%d")
    println(f"Groups with non-trivial stddev_p80 disagreement (> $tol): $diffStdP80Cnt%d")

    if (diffStdP90Cnt > 0 || diffStdP80Cnt > 0) {
      println("\nTop 10 largest stddev disagreements:")
      stddevDiff
        .orderBy(desc("diff_std_p90"))
        .select("SKU", "year", "month",
                "std_p90_approx", "std_p90_exact", "diff_std_p90",
                "std_p80_approx", "std_p80_exact", "diff_std_p80")
        .show(10, false)
    }

    println("\n" + "=" * 70)
    println("End of diagnostic analysis")
    println("=" * 70)

    // -------------------------------------------------------------------------
    // 7. Export result
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

    // Free caches before stopping to keep things tidy.
    threshCompare.unpersist()
    cntCompare.unpersist()
    groupSizes.unpersist()
    allSkuMonths.unpersist()
    df.unpersist()

    spark.stop()
  }
}
