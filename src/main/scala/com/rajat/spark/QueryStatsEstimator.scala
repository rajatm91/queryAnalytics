package com.rajat.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service



trait QueryStatsEstimator {

  val session = SparkSession
    .builder()
    .appName("Query-Analyzer")
    .master("local[*]")

    .enableHiveSupport()
    .getOrCreate()

  session.sparkContext.setLogLevel("ERROR")

  session.conf.set("spark.sql.cbo.enabled","true")
  session.conf.set("spark.sql.statistics.histogram.enabled","true")
  session.conf.set("spark.sessionState.conf.histogramEnabled","true")

 /* val df = session.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:\\dev\\spring\\QueryAnalytics\\src\\main\\resources\\Baby_Names__Beginning_2007.csv")
   // .csv(getClass.getResource("Baby_Names__Beginning_2007.csv").getPath)

  val tableName = "maha"
  val tid = TableIdentifier(tableName)
  val sessionCatalog = session.sessionState.catalog
  sessionCatalog.dropTable(tid, ignoreIfNotExists = true, purge = true)*/

 /* val t = System.nanoTime()

  df.write.saveAsTable(tableName)
  session.sql(s"REFRESH TABLE $tableName")
  val allCols = df.columns.mkString(",")
  val analyzeTableSQL = s"ANALYZE TABLE maha COMPUTE STATISTICS FOR COLUMNS $allCols"
  session.sql(analyzeTableSQL)*/

  def estimateRows(query : String) : BigInt = {

    val exec = session.sql(query).queryExecution
    val queryStatistics: Statistics = exec.optimizedPlan.stats(session.sessionState.conf)
    val estimatedRows: BigInt = queryStatistics.rowCount.get
    val estimatedRowStrings = s"The estimated no of rows to be returned for the query is $estimatedRows "

    estimatedRows
  }
}

@Service
object QueryStatsEstimator extends QueryStatsEstimator
