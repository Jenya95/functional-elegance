package com.sanevich.example

import cats.Semigroup
import cats.syntax.semigroup._

case class ReportMetadata(
    metrics: Map[String, Long] = Map(),
    // new column name -> error message
    customColumnsErrors: Map[String, String] = Map()
)

object ReportMetadata {
  implicit val semigroup: Semigroup[ReportMetadata] = new Semigroup[ReportMetadata] {
    override def combine(x: ReportMetadata, y: ReportMetadata): ReportMetadata = ReportMetadata(
      x.metrics |+| y.metrics,
      x.customColumnsErrors |+| y.customColumnsErrors
    )
  }
}
