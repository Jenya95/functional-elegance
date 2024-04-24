package com.sanevich.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.mutable
import scala.util.chaining.scalaUtilChainingOps

object TupledApproach {
  implicit val spark = SparkSession
    .builder()
    .appName("functional-elegance")
    .master("local[*]")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  import spark.implicits._

  def readMedCleanDataset(reportConfig: MedReportConfig): (Map[String, Long], DataFrame) = {
    val mcdDf = MedCleanDataset
      .read()
      .where($"ins_id" === reportConfig.insCompId)
      .where($"appointment_time" >= reportConfig.from && $"appointment_time" <= reportConfig.to)
      .cache()

    val mcdDfSize = mcdDf.count()
    val uniquePatients = mcdDf.select("patient_id").distinct().count()
    val (minTs, maxTs) = mcdDf.select(min("appointment_time"), max("appointment_time")).as[(Long, Long)].head()
    (
      Map(
        "raw_dataset_size" -> mcdDfSize,
        "raw_unique_patients" -> uniquePatients,
        "min_ts" -> minTs,
        "maxTs" -> maxTs
      ),
      mcdDf
    )
  }

  def addCustomColumns(reportConfig: MedReportConfig)(in: DataFrame): (Map[String, String], DataFrame) = {
    val customColumnsErrors: mutable.Map[String, String] = mutable.Map()
    val reportWithCustomColumns = reportConfig.customColumns.foldLeft(in) { case (df, (colName, expression)) =>
      try {
        df.withColumn(colName, expr(expression))
      } catch {
        case t: Throwable =>
          customColumnsErrors += colName -> t.getMessage
          df.withColumn(colName, lit("null"))
      }
    }
    (customColumnsErrors.toMap, reportWithCustomColumns)
  }

  def group(reportConfig: MedReportConfig)(in: DataFrame): (Map[String, Long], DataFrame) = {
    val userMetrics: List[Column] = reportConfig.metrics.map { case (name, fn) => expr(s"$fn($name)") }.toList
    val grouped = in
      .groupBy(reportConfig.dimensions.map(col): _*)
      .agg(count_distinct($"patient_id").as("unique_patients_count"), userMetrics: _*)
      .cache()

    val abovePrivacyThreshold = grouped.where($"unique_patients_count" > 20)
    val removedGroups = grouped.where($"unique_patients_count" <= 20).count()
    val reportSize = abovePrivacyThreshold.count()
    (
      Map(
        "gropes_removed" -> removedGroups,
        "report_size" -> reportSize
      ),
      abovePrivacyThreshold
    )
  }

  def generateReport(reportConfig: MedReportConfig): (ReportMetadata, DataFrame) = {
    // 1. read dataset and collect some metrics
    val (metrics1, fullDf) = readMedCleanDataset(reportConfig)
    // 2. add custom columns and collect errors if exists
    val (ccErrors, withCc) = addCustomColumns(reportConfig)(fullDf)
    // 3. group report and throw rows lower than threshold
    val (metrics2, grouped) = group(reportConfig)(withCc)

    (ReportMetadata(metrics1 ++ metrics2, ccErrors), grouped)
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
