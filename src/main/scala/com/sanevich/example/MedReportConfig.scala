package com.sanevich.example

import java.time.Instant

case class MedReportConfig(
    insCompId: String,
    from: Instant,
    to: Instant,
    dimensions: List[String],
    // new column name -> spark expression
    customColumns: Map[String, String],
    // column name -> agg function
    metrics: Map[String, String]
)
