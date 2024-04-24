package com.sanevich.example

import cats.data.Writer
import cats.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.Instant
import java.time.temporal.ChronoUnit

object WriterApproach {
  implicit val spark = SparkSession
    .builder()
    .appName("functional-elegance")
    .master("local[*]")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  def readMedCleanDataset(reportConfig: MedReportConfig): Writer[ReportMetadata, DataFrame] = {
    val mcdDf = MedCleanDataset
      .read()
      .where($"ins_id" === reportConfig.insCompId)
      .where($"appointment_time" >= reportConfig.from && $"appointment_time" <= reportConfig.to)
      .cache()

    val mcdDfSize = mcdDf.count()
    val uniquePatients = mcdDf.select("patient_id").distinct().count()
    val (minTs, maxTs) = mcdDf.select(min("appointment_time"), max("appointment_time")).as[(Long, Long)].head()
    Writer(
      ReportMetadata(
        Map(
          "raw_dataset_size" -> mcdDfSize,
          "raw_unique_patients" -> uniquePatients,
          "min_ts" -> minTs,
          "maxTs" -> maxTs
        )
      ),
      mcdDf
    )
  }

  def addCustomColumns(reportConfig: MedReportConfig)(in: DataFrame): Writer[ReportMetadata, DataFrame] = {
    reportConfig.customColumns.foldLeft(Writer(ReportMetadata(), in)) { case (writer, (colName, expression)) =>
      try {
        writer.map(_.withColumn(colName, expr(expression)))
      } catch {
        case t: Throwable =>
          writer
            .map(_.withColumn(colName, lit("null")))
            .tell(ReportMetadata(customColumnsErrors = Map(colName -> t.getMessage)))
      }
    }
  }

  def group(reportConfig: MedReportConfig)(in: DataFrame): Writer[ReportMetadata, DataFrame] = {
    val userMetrics: List[Column] = reportConfig.metrics.map { case (name, fn) => expr(s"$fn($name)") }.toList
    val grouped = in
      .groupBy(reportConfig.dimensions.map(col): _*)
      .agg(count_distinct($"patient_id").as("unique_patients_count"), userMetrics: _*)
      .cache()

    val abovePrivacyThreshold = grouped.where($"unique_patients_count" > 20)
    val removedGroups = grouped.where($"unique_patients_count" <= 20).count()
    val reportSize = abovePrivacyThreshold.count()

    Writer(
      ReportMetadata(
        Map(
          "gropes_removed" -> removedGroups,
          "report_size" -> reportSize
        )
      ),
      abovePrivacyThreshold
    )
  }

  def generateReport(reportConfig: MedReportConfig): Writer[ReportMetadata, DataFrame] = {
    readMedCleanDataset(reportConfig) >>= addCustomColumns(reportConfig) >>= group(reportConfig)
  }

  def main(args: Array[String]): Unit = {
    val res = generateReport(
      MedReportConfig(
        insCompId = "183",
        from = Instant.now().minus(7, ChronoUnit.DAYS),
        to = Instant.now(),
        dimensions = List("hour", "day", "hospital"),
        customColumns = Map("day" -> "substring(appointment_time, 0, 10)", "hour" -> "get_hour(appointment_time)"),
        metrics = Map("*" -> "count")
      )
    )

    pprint.pprintln(res)
  }
}
