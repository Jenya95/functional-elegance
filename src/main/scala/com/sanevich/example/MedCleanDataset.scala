package com.sanevich.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, udf}

import java.time.Instant
import scala.util.Random

object MedCleanDataset {

  val random = new Random()

  def appointmentTime(): Instant = {
    Instant.now().minusSeconds(Random.between(10, 24 * 60 * 60 * 30))
  }

  def hospitalUdf(): String = {
    Random.shuffle(List("hsp_123", "hsp_456", "hsp_789")).head
  }

  def read()(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._
    (1 to 100)
      .map(i => s"patient_$i")
      .flatMap(s => {
        List.fill(random.between(10, 40))(s)
      })
      .toDF("patient_id")
      .withColumn("ins_id", lit("183"))
      .withColumn("appointment_time", udf(() => appointmentTime()).apply())
      .withColumn("hospital", udf(() => hospitalUdf()).apply())
  }
}
