package example.patrickQueries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class IawAgeQuery {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("PatrickQuery")
    .getOrCreate()

  val sc = spark.sparkContext

  // only show iaw and age from iaw gt
  // _ double check the code for age
  val iawDFAge = spark.sql("select injury_at_work, age_code from iawDFGlobalTemp")

  val iawGBAge = iawDFAge.groupBy("age_code")

  val iawGBAgeCount = iawGBAge.count()

}
