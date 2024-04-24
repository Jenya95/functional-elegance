package com.sanevich.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.mutable
import scala.util.chaining.scalaUtilChainingOps

object NaiveApproach {
  implicit val spark = SparkSession
    .builder()
    .appName("functional-elegance-naive")
    .master("local[*]")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  import spark.implicits._

  def generateReport(reportConfig: MedReportConfig): (ReportMetadata, DataFrame) = {
    // 1. read dataset and collect some metrics
    val mcdDf = MedCleanDataset
      .read()
      .where($"ins_id" === reportConfig.insCompId)
      .where($"appointment_time" >= reportConfig.from && $"appointment_time" <= reportConfig.to)
      .cache()

    val mcdDfSize = mcdDf.count()
    val uniquePatients = mcdDf.select("patient_id").distinct().count()
    val (minTs, maxTs) = mcdDf.select(min("appointment_time"), max("appointment_time")).as[(Long, Long)].head()

    // 2. add custom columns and collect errors if exists
    val customColumnsErrors: mutable.Map[String, String] = mutable.Map()
    val reportWithCustomColumns = reportConfig.customColumns.foldLeft(mcdDf) { case (df, (colName, expression)) =>
      try {
        df.withColumn(colName, expr(expression))
      } catch {
        case t: Throwable =>
          customColumnsErrors += colName -> t.getMessage
          df.withColumn(colName, lit("null"))
      }
    }

    // 3. group report and throw rows lower than threshold
    val userMetrics: List[Column] = reportConfig.metrics.map { case (name, fn) => expr(s"$fn($name)") }.toList
    val grouped = reportWithCustomColumns
      .groupBy(reportConfig.dimensions.map(col): _*)
      .agg(count_distinct($"patient_id").as("unique_patients_count"), userMetrics: _*)
      .cache()

    val abovePrivacyThreshold = grouped.where($"unique_patients_count" > 20)
    val removedGroups = grouped.where($"unique_patients_count" <= 20).count()
    val reportSize = abovePrivacyThreshold.count()

    val metadata = ReportMetadata(
      Map(
        "raw_dataset_size" -> mcdDfSize,
        "raw_unique_patients" -> uniquePatients,
        "min_ts" -> minTs,
        "maxTs" -> maxTs,
        "gropes_removed" -> removedGroups,
        "report_size" -> reportSize
      ),
      customColumnsErrors.toMap
    )

    (metadata, abovePrivacyThreshold)
  }

  def main(args: Array[String]): Unit = {
    val (metadata, df) = generateReport(
      MedReportConfig(
        insCompId = "183",
        from = Instant.now().minus(7, ChronoUnit.DAYS),
        to = Instant.now(),
        dimensions = List("hour", "day", "hospital"),
        customColumns = Map("day" -> "substring(appointment_time, 0, 10)", "hour" -> "get_hour(appointment_time)"),
        metrics = Map("*" -> "count")
      )
    )

    df.show(false)
    metadata.tap(pprint.pprintln(_))
  }
}
