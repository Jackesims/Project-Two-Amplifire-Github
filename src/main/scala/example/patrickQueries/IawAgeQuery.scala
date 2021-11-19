package example.patrickQueries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class IawAgeQuery {  
  
  def ageQuery: Unit = {

    val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("PatrickQuery2")
    .getOrCreate()

    val sc = spark.sparkContext

    val iawDFAge = spark.sql("SELECT injury_at_work, detail_age FROM global_temp.iawDFGlobalTemp")
    val iawAgesDF = iawDFAge.where(col("detail_age") === "1")
    .groupBy("detail_age")
    .count()
    .orderBy(col("detail_age").desc).toDF
    .show(false)
  }

}
