import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.io.{BufferedReader, InputStreamReader}
import java.lang.Iterable
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

// ─────────────────────────────────────────────────────────────────────────────
// Job 1 – Count purchases per (windowDate, state, size)
//
// Mapper:
//   Input : one CSV row
//   Output: key = "windowDate\tstate\tsize"  value = "1"
//   → emits one pair for every window date d where orderDate ∈ [d-7, d-1]
//
// Reducer:
//   Input : key = "windowDate\tstate\tsize"  values = ["1","1",...]
//   Output: key = "windowDate\tstate"        value = "size\tcount"
// ─────────────────────────────────────────────────────────────────────────────
object Job1_CountPurchases {

  class PurchaseMapper extends Mapper[LongWritable, Text, Text, Text] {

    private var dateIdx   = -1
    private var statusIdx = -1
    private var qtyIdx    = -1
    private var sizeIdx   = -1
    private var stateIdx  = -1

    private val outKey   = new Text()
    private val outValue = new Text("1")

    override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val conf  = context.getConfiguration
      dateIdx   = conf.getInt("col.date",   -1)
      statusIdx = conf.getInt("col.status", -1)
      qtyIdx    = conf.getInt("col.qty",    -1)
      sizeIdx   = conf.getInt("col.size",   -1)
      stateIdx  = conf.getInt("col.state",  -1)
    }

    override def map(
        key: LongWritable,
        value: Text,
        context: Mapper[LongWritable, Text, Text, Text]#Context
    ): Unit = {

      val line = value.toString.trim
      if (line.isEmpty) return

      // Skip header row (byte offset 0 = first line of file)
      if (key.get() == 0 && line.toLowerCase.contains("date")) return

      val cols   = parseCsvLine(line)
      val maxIdx = List(dateIdx, statusIdx, qtyIdx, sizeIdx, stateIdx).max
      if (cols.length <= maxIdx) return

      val rawStatus = cols(statusIdx).toLowerCase
      val rawSize   = cols(sizeIdx).trim.toUpperCase
      val rawState  = cols(stateIdx).trim.toUpperCase
      val rawDate   = cols(dateIdx).trim
      val qty       = try cols(qtyIdx).trim.toInt catch { case _: Exception => 0 }

      if (!rawStatus.contains("shipped") || qty <= 0 ||
          rawSize.isEmpty || rawState.isEmpty || rawDate.isEmpty) return

      val orderDateOpt = parseDate(rawDate)
      if (orderDateOpt.isEmpty) return
      val orderDate = orderDateOpt.get

      // Emit for every window date d where orderDate ∈ [d-7, d-1]
      // → d ranges from orderDate+1 to orderDate+7
      for (offset <- 1 to 7) {
        val windowDate = orderDate.plusDays(offset)
        outKey.set(s"${windowDate}\t${rawState}\t${rawSize}")
        context.write(outKey, outValue)
      }
    }

    private def parseCsvLine(line: String): Array[String] = {
      val result   = scala.collection.mutable.ArrayBuffer[String]()
      var inQuotes = false
      val current  = new StringBuilder
      for (c <- line) c match {
        case '"'              => inQuotes = !inQuotes
        case ',' if !inQuotes => result += current.toString().trim; current.clear()
        case other            => current += other
      }
      result += current.toString().trim
      result.toArray
    }

    private val dateFormats = List(
      DateTimeFormatter.ofPattern("M/d/yy"),
      DateTimeFormatter.ofPattern("MM-dd-yyyy"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd"),
      DateTimeFormatter.ofPattern("M/d/yyyy"),
      DateTimeFormatter.ofPattern("MM/dd/yyyy")
    )

    private def parseDate(s: String): Option[LocalDate] =
      dateFormats.foldLeft(Option.empty[LocalDate]) { (acc, fmt) =>
        acc.orElse(try Some(LocalDate.parse(s.trim, fmt)) catch { case _: Exception => None })
      }
  }

  class CountReducer extends Reducer[Text, Text, Text, Text] {

    private val outKey   = new Text()
    private val outValue = new Text()

    override def reduce(
        key: Text,
        values: Iterable[Text],
        context: Reducer[Text, Text, Text, Text]#Context
    ): Unit = {
      val count = values.asScala.foldLeft(0L)((acc, _) => acc + 1L)
      val parts = key.toString.split("\t", -1)
      if (parts.length < 3) return

      // Re-key as "windowDate\tstate" → "size\tcount"
      outKey.set(s"${parts(0)}\t${parts(1)}")
      outValue.set(s"${parts(2)}\t${count}")
      context.write(outKey, outValue)
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Job 2 – Find the most bought size per (windowDate, state)
//
// Mapper  : identity – re-key lines from Job 1 output
// Reducer : pick size with highest count; tie-break = lexicographic order
// ─────────────────────────────────────────────────────────────────────────────
object Job2_FindMaxSize {

  class PassThroughMapper extends Mapper[LongWritable, Text, Text, Text] {

    private val outKey   = new Text()
    private val outValue = new Text()

    override def map(
        key: LongWritable,
        value: Text,
        context: Mapper[LongWritable, Text, Text, Text]#Context
    ): Unit = {
      val line  = value.toString.trim
      if (line.isEmpty) return

      // Job 1 output line: "windowDate\tstate\tsize\tcount"
      val parts = line.split("\t", -1)
      if (parts.length < 4) return

      outKey.set(s"${parts(0)}\t${parts(1)}")
      outValue.set(s"${parts(2)}\t${parts(3)}")
      context.write(outKey, outValue)
    }
  }

  class MaxSizeReducer extends Reducer[Text, Text, Text, Text] {

    private val outKey   = new Text()
    private val outValue = new Text("")

    override def reduce(
        key: Text,
        values: Iterable[Text],
        context: Reducer[Text, Text, Text, Text]#Context
    ): Unit = {
      var bestSize  = ""
      var bestCount = 0L

      for (v <- values.asScala) {
        val parts = v.toString.split("\t", -1)
        if (parts.length >= 2) {
          val size  = parts(0)
          val count = try parts(1).toLong catch { case _: Exception => 0L }
          val better =
            count > bestCount ||
            (count == bestCount && (bestSize.isEmpty || size < bestSize))
          if (better) { bestSize = size; bestCount = count }
        }
      }

      if (bestSize.nonEmpty) {
        // Output key carries all data; value is empty
        outKey.set(s"${key.toString}\t${bestSize}\t${bestCount}")
        context.write(outKey, outValue)
      }
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Driver – chains Job 1 → Job 2, merges output to a single local CSV
//
// Usage:
//   hadoop jar task1-1.jar Task1_1Driver <input_csv> <output_dir>
//
// Example:
//   hadoop jar task1-1.jar Task1_1Driver hdfs:///data/asr.csv hdfs:///out/task1-1
//
// Final result: <output_dir>/Task_1-1.csv  (local filesystem)
// ─────────────────────────────────────────────────────────────────────────────
object Task1_1Driver {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: Task1_1Driver <input_csv> <output_dir>")
      System.exit(1)
    }

    val inputPath  = args(0)
    val outputBase = args(1)
    val job1Out    = outputBase + "/job1_tmp"
    val job2Out    = outputBase + "/job2_tmp"
    val finalCsv   = outputBase + "/Task_1-1.csv"

    val conf = new Configuration()

    // ── Resolve column indices from CSV header ──────────────────────────────
    val colMap = resolveColumns(conf, inputPath)
    println(s"Column indices resolved: $colMap")

    conf.setInt("col.date",   colMap("date"))
    conf.setInt("col.status", colMap("status"))
    conf.setInt("col.qty",    colMap("qty"))
    conf.setInt("col.size",   colMap("size"))
    conf.setInt("col.state",  colMap("ship_state"))

    val fs = FileSystem.get(conf)

    // ── Job 1 ───────────────────────────────────────────────────────────────
    val job1 = Job.getInstance(conf, "Task1-1 Job1: Count purchases per window-state-size")
    job1.setJarByClass(Task1_1Driver.getClass)
    job1.setMapperClass(classOf[Job1_CountPurchases.PurchaseMapper])
    job1.setReducerClass(classOf[Job1_CountPurchases.CountReducer])
    job1.setMapOutputKeyClass(classOf[Text])
    job1.setMapOutputValueClass(classOf[Text])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[Text])
    job1.setNumReduceTasks(4)

    FileInputFormat.addInputPath(job1, new Path(inputPath))
    val job1OutPath = new Path(job1Out)
    if (fs.exists(job1OutPath)) fs.delete(job1OutPath, true)
    FileOutputFormat.setOutputPath(job1, job1OutPath)

    println("Starting Job 1 ...")
    if (!job1.waitForCompletion(true)) { System.err.println("Job 1 failed."); System.exit(1) }
    println("Job 1 done.")

    // ── Job 2 ───────────────────────────────────────────────────────────────
    val job2 = Job.getInstance(conf, "Task1-1 Job2: Find most bought size per window-state")
    job2.setJarByClass(Task1_1Driver.getClass)
    job2.setMapperClass(classOf[Job2_FindMaxSize.PassThroughMapper])
    job2.setReducerClass(classOf[Job2_FindMaxSize.MaxSizeReducer])
    job2.setMapOutputKeyClass(classOf[Text])
    job2.setMapOutputValueClass(classOf[Text])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[Text])
    job2.setNumReduceTasks(1)   // single reducer → globally sorted output

    FileInputFormat.addInputPath(job2, job1OutPath)
    val job2OutPath = new Path(job2Out)
    if (fs.exists(job2OutPath)) fs.delete(job2OutPath, true)
    FileOutputFormat.setOutputPath(job2, job2OutPath)

    println("Starting Job 2 ...")
    if (!job2.waitForCompletion(true)) { System.err.println("Job 2 failed."); System.exit(1) }
    println("Job 2 done.")

    // ── Merge part files → single local CSV ────────────────────────────────
    mergeToLocalCsv(conf, job2OutPath, finalCsv)
    println(s"✓ Done. Result written to: $finalCsv")

    // Clean up temporary HDFS directories
    fs.delete(job1OutPath, true)
    fs.delete(job2OutPath, true)
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  /** Read the CSV header and map canonical column names → column indices. */
  private def resolveColumns(conf: Configuration, inputPath: String): Map[String, Int] = {
    val fs     = FileSystem.get(new Path(inputPath).toUri, conf)
    val reader = new BufferedReader(new InputStreamReader(fs.open(new Path(inputPath))))
    val header = reader.readLine()
    reader.close()

    val cols = header.split(",", -1).map(
      _.trim.toLowerCase.replaceAll("\\s+", "_").replaceAll("-", "_")
    )

    val required = Map(
      "date"       -> List("date"),
      "status"     -> List("status"),
      "qty"        -> List("qty"),
      "size"       -> List("size"),
      "ship_state" -> List("ship_state", "ship-state", "shipstate", "state")
    )

    required.map { case (canonical, aliases) =>
      val idx = aliases
        .flatMap(a => cols.zipWithIndex.find(_._1 == a).map(_._2))
        .headOption
        .getOrElse(throw new RuntimeException(
          s"Column '$canonical' not found in header: $header"
        ))
      canonical -> idx
    }
  }

  /** Merge HDFS part-* files into a single local CSV with a header row. */
  private def mergeToLocalCsv(conf: Configuration, hdfsDir: Path, localPath: String): Unit = {
    val fs     = FileSystem.get(conf)
    val writer = new java.io.PrintWriter(
      new java.io.BufferedWriter(new java.io.FileWriter(localPath))
    )
    try {
      writer.println("window_date,state,most_bought_size,purchase_count")
      fs.listStatus(hdfsDir)
        .filter(_.getPath.getName.startsWith("part-"))
        .sortBy(_.getPath.getName)
        .foreach { status =>
          val reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath)))
          var line   = reader.readLine()
          while (line != null) {
            val trimmed = line.trim
            if (trimmed.nonEmpty) {
              // key = "windowDate\tstate\tmostBoughtSize\tpurchaseCount"
              // value field is empty → last tab-separated token is empty, drop it
              val parts  = trimmed.split("\t", -1)
              val useful = if (parts.last.isEmpty) parts.dropRight(1) else parts
              writer.println(useful.mkString(","))
            }
            line = reader.readLine()
          }
          reader.close()
        }
    } finally {
      writer.close()
    }
  }
}