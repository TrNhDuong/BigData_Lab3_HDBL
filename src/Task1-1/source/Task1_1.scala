import org.apache.spark.{SparkConf, SparkContext}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * Task 1-1: Sliding Window Computation
 *
 * For each date d and each state, identify the size that is mostly bought
 * within a 7-day window [d-7, d-1] prior to the current date d.
 *
 * An item is "bought" if:
 *   - The order status contains "Shipped" (case-insensitive)
 *   - The quantity (Qty) is non-zero
 *
 * The window slides by 1 day at a time.
 * Output: (date, state, most_bought_size, count)
 *
 * Uses MapReduce (RDD API) as required by the problem statement.
 */
object Task1_1 {

  def main(args: Array[String]): Unit = {

    // ── 1. Spark context ──────────────────────────────────────────────────────
    val conf = new SparkConf()
      .setAppName("Task1-1: Sliding Window Most Bought Size per State")
      .setIfMissing("spark.master", "local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // ── 2. Input / Output paths ───────────────────────────────────────────────
    val inputPath  = if (args.length > 0) args(0) else "Amazon Sale Report.csv"
    val outputPath = if (args.length > 1) args(1) else "output/Task1_1"

    // ── 3. Load raw CSV ───────────────────────────────────────────────────────
    val rawRDD = sc.textFile(inputPath)

    // Identify header
    val header = rawRDD.first()

    // Parse header to get column indices
    val headerCols = header.split(",", -1).map(_.trim.toLowerCase.replaceAll("\\s+", "_").replaceAll("-", "_"))

    // Find required column indices from header
    val dateIdx   = headerCols.indexOf("date")
    val statusIdx = headerCols.indexOf("status")
    val qtyIdx    = headerCols.indexOf("qty")
    val sizeIdx   = headerCols.indexOf("size")
    val stateIdx  = headerCols.indexOf("ship_state")

    // Broadcast column indices so workers can access them
    val dateIdxB   = sc.broadcast(dateIdx)
    val statusIdxB = sc.broadcast(statusIdx)
    val qtyIdxB    = sc.broadcast(qtyIdx)
    val sizeIdxB   = sc.broadcast(sizeIdx)
    val stateIdxB  = sc.broadcast(stateIdx)

    // ── 4. Filter & parse data rows ───────────────────────────────────────────
    // Keep only rows where status contains "shipped" (case-insensitive) and qty > 0
    // Emit (date: LocalDate, state: String, size: String)

    val formatter = DateTimeFormatter.ofPattern("M/d/yy")  // e.g. "4/2/22"

    /**
     * Try multiple date formats used in Amazon Sale Report dataset.
     * Common formats: "04-02-2022", "2022-04-02", "4/2/22", "04/02/2022"
     */
    def parseDate(s: String): Option[LocalDate] = {
      val formats = List(
        DateTimeFormatter.ofPattern("M/d/yy"),
        DateTimeFormatter.ofPattern("MM-dd-yyyy"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd"),
        DateTimeFormatter.ofPattern("M/d/yyyy"),
        DateTimeFormatter.ofPattern("MM/dd/yyyy")
      )
      formats.foldLeft(Option.empty[LocalDate]) { (acc, fmt) =>
        acc.orElse {
          try Some(LocalDate.parse(s.trim, fmt))
          catch { case _: Exception => None }
        }
      }
    }

    /**
     * Parse a CSV line respecting quoted fields.
     * Simple implementation: split by comma but handle double-quoted fields.
     */
    def parseCsvLine(line: String): Array[String] = {
      val result = scala.collection.mutable.ArrayBuffer[String]()
      var inQuotes = false
      val current  = new StringBuilder
      for (c <- line) {
        c match {
          case '"' => inQuotes = !inQuotes
          case ',' if !inQuotes =>
            result += current.toString().trim
            current.clear()
          case other => current += other
        }
      }
      result += current.toString().trim
      result.toArray
    }

    val filteredRDD = rawRDD
      .filter(_ != header) // remove header
      .flatMap { line =>
        val cols = parseCsvLine(line)

        // Guard: enough columns
        if (cols.length <= math.max(
              math.max(dateIdxB.value, statusIdxB.value),
              math.max(qtyIdxB.value, math.max(sizeIdxB.value, stateIdxB.value))
            )) {
          None
        } else {
          val rawStatus = cols(statusIdxB.value).toLowerCase
          val rawQtyStr = cols(qtyIdxB.value)
          val rawSize   = cols(sizeIdxB.value).trim
          val rawState  = cols(stateIdxB.value).trim
          val rawDate   = cols(dateIdxB.value).trim

          val qty = try rawQtyStr.trim.toInt catch { case _: Exception => 0 }

          // Condition: "shipped" in status and qty > 0 and non-empty fields
          if (rawStatus.contains("shipped") && qty > 0 && rawSize.nonEmpty && rawState.nonEmpty && rawDate.nonEmpty) {
            parseDate(rawDate).map { date =>
              (date, rawState.toUpperCase, rawSize.toUpperCase)
            }
          } else {
            None
          }
        }
      }

    // Cache for reuse
    val boughtRDD = filteredRDD.cache()

    // ── 5. Collect all unique dates to build the sliding window ───────────────
    val allDates: Array[LocalDate] = boughtRDD
      .map(_._1)
      .distinct()
      .collect()
      .sorted

    if (allDates.isEmpty) {
      println("No valid data found. Exiting.")
      sc.stop()
      return
    }

    val minDate = allDates.head
    val maxDate = allDates.last

    // Generate every calendar date from minDate to maxDate (sliding by 1 day)
    val allSlidingDates: Seq[LocalDate] = {
      val buf = scala.collection.mutable.ArrayBuffer[LocalDate]()
      var cur = minDate
      while (!cur.isAfter(maxDate)) {
        buf += cur
        cur = cur.plusDays(1)
      }
      buf.toSeq
    }

    // Broadcast dates
    val allSlidingDatesB = sc.broadcast(allSlidingDates)

    // ── 6. Map phase ──────────────────────────────────────────────────────────
    // For each bought record (orderDate, state, size), emit it to ALL sliding
    // window dates d where orderDate ∈ [d-7, d-1].
    // Key: (windowDate, state, size)   Value: 1L

    val mapPhase = boughtRDD.flatMap { case (orderDate, state, size) =>
      allSlidingDatesB.value.flatMap { d =>
        val windowStart = d.minusDays(7)   // d - 7
        val windowEnd   = d.minusDays(1)   // d - 1
        // Include the order if it falls within [d-7, d-1]
        if (!orderDate.isBefore(windowStart) && !orderDate.isAfter(windowEnd)) {
          Some(((d, state, size), 1L))
        } else {
          None
        }
      }
    }

    // ── 7. Reduce phase ───────────────────────────────────────────────────────
    // Count total purchases per (windowDate, state, size)
    val reducedCounts = mapPhase.reduceByKey(_ + _)

    // ── 8. Find the most bought size per (windowDate, state) ─────────────────
    // Re-key as ((windowDate, state), (size, count)) then pick max by count
    val resultRDD = reducedCounts
      .map { case ((date, state, size), count) =>
        ((date, state), (size, count))
      }
      .reduceByKey { case ((size1, count1), (size2, count2)) =>
        // In case of tie, pick lexicographically smaller size for determinism
        if (count1 > count2) (size1, count1)
        else if (count2 > count1) (size2, count2)
        else if (size1 <= size2) (size1, count1)
        else (size2, count2)
      }
      .map { case ((date, state), (size, count)) =>
        (date, state, size, count)
      }
      .sortBy { case (date, state, _, _) => (date.toString, state) }

    // ── 9. Export to a single CSV file ────────────────────────────────────────
    // Coalesce to 1 partition and write as CSV-compatible text
    val csvHeader = "window_date,state,most_bought_size,purchase_count"

    val csvRDD = resultRDD.map { case (date, state, size, count) =>
      s"$date,$state,$size,$count"
    }

    // Prepend header using union of a single-element RDD
    val headerRDD = sc.parallelize(Seq(csvHeader), 1)
    val finalCSV  = headerRDD.union(csvRDD.coalesce(1))

    // Save as text (single partition)
    finalCSV.coalesce(1).saveAsTextFile(outputPath)

    println(s"✓ Task 1-1 done. Output written to: $outputPath")
    println(s"  Total window-date × state results: ${resultRDD.count()}")

    boughtRDD.unpersist()
    sc.stop()
  }
}
