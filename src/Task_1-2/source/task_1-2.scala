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
import scala.math

// ─────────────────────────────────────────────────────────────────────────────
// Task 1-2: State-Level Median Variety by Month (XXL+ sizes only)
//
// APPROACH:
// Job 1 – Filter & Extract: Extract (month, state, style, SKU) for XXL+ sizes
// Job 2 – Count Variety: For each (month, state, style), count distinct SKUs
// Job 3 – Compute Median: For each (month, state), compute median variety
//
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Job 1 – Filter & Extract: Extract records with XXL+ sizes
//
// Mapper:
//   Input : one CSV row
//   Output: key = "month\tstate\tstyle\tsku"  value = "1"
//   → emits only if size is XXL or larger
//
// Reducer:
//   Input : key = "month\tstate\tstyle\tsku"  values = ["1","1",...]
//   Output: key = "month\tstate\tstyle"       value = "1" (marks this style exists)
// ─────────────────────────────────────────────────────────────────────────────
object Job1_FilterAndExtract {

  class FilterMapper extends Mapper[LongWritable, Text, Text, Text] {

    private var dateIdx   = -1
    private var sizeIdx   = -1
    private var skuIdx    = -1
    private var styleIdx  = -1
    private var stateIdx  = -1

    private val outKey   = new Text()
    private val outValue = new Text("1")

    // XXL and larger sizes
    private val xxlAndLarger = Set("XXL", "3XL", "4XL", "5XL", "6XL")

    override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val conf  = context.getConfiguration
      dateIdx   = conf.getInt("col.date",   -1)
      sizeIdx   = conf.getInt("col.size",   -1)
      skuIdx    = conf.getInt("col.sku",    -1)
      styleIdx  = conf.getInt("col.style",  -1)
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
      val maxIdx = List(dateIdx, sizeIdx, skuIdx, styleIdx, stateIdx).max
      if (cols.length <= maxIdx) return

      val rawSize   = cols(sizeIdx).trim.toUpperCase
      val rawSku    = cols(skuIdx).trim
      val rawStyle  = cols(styleIdx).trim
      val rawState  = cols(stateIdx).trim
      val rawDate   = cols(dateIdx).trim

        // Filter: XXL+ size and non-empty fields
        if (!xxlAndLarger.contains(rawSize) ||
          rawSku.isEmpty || rawStyle.isEmpty || rawState.isEmpty || rawDate.isEmpty) return

      val monthOpt = parseMonth(rawDate)
      if (monthOpt.isEmpty) return
      val month = monthOpt.get

      // Standardize state name to title case
      val standardizedState = standardizeStateName(rawState)

      // Emit: key = "month\tstate\tstyle\tsku"  value = "1"
      outKey.set(s"${month}\t${standardizedState}\t${rawStyle}\t${rawSku}")
      context.write(outKey, outValue)
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
      DateTimeFormatter.ofPattern("MM-dd-yy")  // dataset format: e.g. 04-30-22
    )

    private def parseMonth(s: String): Option[String] = {
      dateFormats.foldLeft(Option.empty[String]) { (acc, fmt) =>
        acc.orElse(try {
          val date = LocalDate.parse(s.trim, fmt)
          Some(f"${date.getYear}-${date.getMonthValue}%02d")
        } catch { case _: Exception => None })
      }
    }

    private def standardizeStateName(state: String): String = {
      val lower = state.toLowerCase.trim
      // Map old name and variations to standard names
      val stateMap = Map(
        "orissa" -> "Odisha",
        "rajsthan" -> "Rajasthan"
      )
      
      stateMap.getOrElse(lower, {
        // Title case all other states
        lower.split("\\s+").map(word => word.charAt(0).toUpper + word.substring(1)).mkString(" ")
      })
    }
  }

  class DeduplicateReducer extends Reducer[Text, Text, Text, Text] {

    private val outKey   = new Text()
    private val outValue = new Text("1")

    override def reduce(
        key: Text,
        values: Iterable[Text],
        context: Reducer[Text, Text, Text, Text]#Context
    ): Unit = {
      // Deduplicate: for each (month, state, style, sku), emit once
      val parts = key.toString.split("\t", -1)
      if (parts.length < 4) return

      // Re-key as "month\tstate\tstyle" (drop SKU from key)
      outKey.set(s"${parts(0)}\t${parts(1)}\t${parts(2)}")
      context.write(outKey, outValue)
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Job 2 – Count Variety: Count distinct SKUs per (month, state, style)
//
// Mapper  : identity – re-key lines from Job 1 output
// Reducer : for each (month, state, style), count distinct SKUs
//           output: key = "month\tstate\tstyle"  value = "variety_count"
// ─────────────────────────────────────────────────────────────────────────────
object Job2_CountVariety {

  class PassThroughMapper extends Mapper[LongWritable, Text, Text, Text] {

    private val outKey   = new Text()
    private val outValue = new Text()

    override def map(
        key: LongWritable,
        value: Text,
        context: Mapper[LongWritable, Text, Text, Text]#Context
    ): Unit = {
      val line = value.toString.trim
      if (line.isEmpty) return

      // Job 1 output line: "month\tstate\tstyle"
      outKey.set(line)
      outValue.set("1")
      context.write(outKey, outValue)
    }
  }

  class CountVarietyReducer extends Reducer[Text, Text, Text, Text] {

    private val outKey   = new Text()
    private val outValue = new Text()

    override def reduce(
        key: Text,
        values: Iterable[Text],
        context: Reducer[Text, Text, Text, Text]#Context
    ): Unit = {
      val variety = values.asScala.size

      val parts = key.toString.split("\t", -1)
      if (parts.length < 3) return

      // Output: key = "month\tstate"  value = "style\tvariety"
      outKey.set(s"${parts(0)}\t${parts(1)}")
      outValue.set(s"${parts(2)}\t${variety}")
      context.write(outKey, outValue)
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Job 3 – Compute Median: Calculate median variety per (month, state)
//
// Mapper  : identity mapper
// Reducer : collect all varieties for a (month, state) and compute median
//           output: key = ""  value = "month\tstate\tmedian_variety"
// ─────────────────────────────────────────────────────────────────────────────
object Job3_ComputeMedian {

  class PassThroughMapper extends Mapper[LongWritable, Text, Text, Text] {

    private val outKey   = new Text()
    private val outValue = new Text()

    override def map(
        key: LongWritable,
        value: Text,
        context: Mapper[LongWritable, Text, Text, Text]#Context
    ): Unit = {
      val line = value.toString.trim
      if (line.isEmpty) return

      val parts = line.split("\t", -1)
      if (parts.length < 4) return

      // Group by (month, state) → key = "month\tstate"
      // Job 2 output line: "month\tstate\tstyle\tvariety"
      outKey.set(s"${parts(0)}\t${parts(1)}")
      outValue.set(parts(3))  // variety count for this style
      context.write(outKey, outValue)
    }
  }

  class MedianReducer extends Reducer[Text, Text, Text, Text] {

    private val outKey   = new Text()
    private val outValue = new Text()

    override def reduce(
        key: Text,
        values: Iterable[Text],
        context: Reducer[Text, Text, Text, Text]#Context
    ): Unit = {
      // Collect all varieties for this (month, state)
      val varieties = values.asScala.map(v => {
        try v.toString.toLong catch { case _: Exception => 0L }
      }).filter(_ > 0).toList.sorted

      if (varieties.isEmpty) return

      // Compute median
      val median = if (varieties.length % 2 == 1) {
        varieties(varieties.length / 2)
      } else {
        (varieties(varieties.length / 2 - 1) + varieties(varieties.length / 2)) / 2.0
      }

      val parts = key.toString.split("\t", -1)
      if (parts.length < 2) return

      // Output format: key = "month\tstate\tmedian"  value = ""
      outKey.set(s"${parts(0)}\t${parts(1)}\t${median}")
      outValue.set("")
      context.write(outKey, outValue)
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Driver – chains Job 1 → Job 2 → Job 3
//
// Usage:
//   hadoop jar task1-2.jar Task1_2Driver <input_csv> <output_dir>
//
// Example:
//   hadoop jar task1-2.jar Task1_2Driver hdfs:///data/asr.csv hdfs:///out/task1-2
//
// Final result: <output_dir>/Task_1-2.csv  (local filesystem)
// ─────────────────────────────────────────────────────────────────────────────
object Task1_2Driver {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: Task1_2Driver <input_csv> <output_dir>")
      System.exit(1)
    }

    val inputPath  = args(0)
    val outputBase = args(1)
    val job1Out    = outputBase + "/job1_tmp"
    val job2Out    = outputBase + "/job2_tmp"
    val job3Out    = outputBase + "/job3_tmp"
    val finalCsv   = outputBase + "/Task_1-2.csv"

    val conf = new Configuration()

    // ── Resolve column indices from CSV header ──────────────────────────────
    val colMap = resolveColumns(conf, inputPath)
    println(s"Column indices resolved: $colMap")

    conf.setInt("col.date",   colMap("date"))
    conf.setInt("col.sku",    colMap("sku"))
    conf.setInt("col.style",  colMap("style"))
    conf.setInt("col.size",   colMap("size"))
    conf.setInt("col.state",  colMap("ship_state"))

    val fs = FileSystem.get(conf)

    // ── Job 1 ───────────────────────────────────────────────────────────────
    val job1 = Job.getInstance(conf, "Task1-2 Job1: Filter & Extract XXL+ sizes")
    job1.setJarByClass(Task1_2Driver.getClass)
    job1.setMapperClass(classOf[Job1_FilterAndExtract.FilterMapper])
    job1.setReducerClass(classOf[Job1_FilterAndExtract.DeduplicateReducer])
    job1.setMapOutputKeyClass(classOf[Text])
    job1.setMapOutputValueClass(classOf[Text])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[Text])
    job1.setNumReduceTasks(4)

    FileInputFormat.addInputPath(job1, new Path(inputPath))
    val job1OutPath = new Path(job1Out)
    if (fs.exists(job1OutPath)) fs.delete(job1OutPath, true)
    FileOutputFormat.setOutputPath(job1, job1OutPath)

    println("Starting Job 1: Filter & Extract ...")
    if (!job1.waitForCompletion(true)) { System.err.println("Job 1 failed."); System.exit(1) }
    println("Job 1 done.")

    // ── Job 2 ───────────────────────────────────────────────────────────────
    val job2 = Job.getInstance(conf, "Task1-2 Job2: Count Variety")
    job2.setJarByClass(Task1_2Driver.getClass)
    job2.setMapperClass(classOf[Job2_CountVariety.PassThroughMapper])
    job2.setReducerClass(classOf[Job2_CountVariety.CountVarietyReducer])
    job2.setMapOutputKeyClass(classOf[Text])
    job2.setMapOutputValueClass(classOf[Text])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[Text])
    job2.setNumReduceTasks(4)

    FileInputFormat.addInputPath(job2, job1OutPath)
    val job2OutPath = new Path(job2Out)
    if (fs.exists(job2OutPath)) fs.delete(job2OutPath, true)
    FileOutputFormat.setOutputPath(job2, job2OutPath)

    println("Starting Job 2: Count Variety ...")
    if (!job2.waitForCompletion(true)) { System.err.println("Job 2 failed."); System.exit(1) }
    println("Job 2 done.")

    // ── Job 3 ───────────────────────────────────────────────────────────────
    val job3 = Job.getInstance(conf, "Task1-2 Job3: Compute Median")
    job3.setJarByClass(Task1_2Driver.getClass)
    job3.setMapperClass(classOf[Job3_ComputeMedian.PassThroughMapper])
    job3.setReducerClass(classOf[Job3_ComputeMedian.MedianReducer])
    job3.setMapOutputKeyClass(classOf[Text])
    job3.setMapOutputValueClass(classOf[Text])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[Text])
    job3.setNumReduceTasks(1)  // Single reducer to consolidate results

    FileInputFormat.addInputPath(job3, job2OutPath)
    val job3OutPath = new Path(job3Out)
    if (fs.exists(job3OutPath)) fs.delete(job3OutPath, true)
    FileOutputFormat.setOutputPath(job3, job3OutPath)

    println("Starting Job 3: Compute Median ...")
    if (!job3.waitForCompletion(true)) { System.err.println("Job 3 failed."); System.exit(1) }
    println("Job 3 done.")

    // ── Merge part files → single local CSV ────────────────────────────────
    mergeToLocalCsv(conf, job3OutPath, finalCsv)
    println(s"✓ Done. Result written to: $finalCsv")

    // Clean up temporary HDFS directories
    fs.delete(job1OutPath, true)
    fs.delete(job2OutPath, true)
    fs.delete(job3OutPath, true)
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
      "sku"        -> List("sku"),
      "style"      -> List("style"),
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
    
    // Create parent directory if it doesn't exist
    val localFile = new java.io.File(localPath)
    localFile.getParentFile.mkdirs()
    
    val writer = new java.io.PrintWriter(
      new java.io.BufferedWriter(new java.io.FileWriter(localPath))
    )
    try {
      writer.println("month,state,median_variety")
      fs.listStatus(hdfsDir)
        .filter(_.getPath.getName.startsWith("part-"))
        .sortBy(_.getPath.getName)
        .foreach { status =>
          val reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath)))
          var line   = reader.readLine()
          while (line != null) {
            val trimmed = line.trim
            if (trimmed.nonEmpty) {
              // key = "month\tstate\tmedian"  value = empty
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
