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
  val iawDFAge = spark.sql("select injury_at_work, detail_age from globaltemp.iawDFGlobalTemp where detail_age_type === 1")

  val iawGBAge = iawDFAge.groupBy("detail_age")

  val iawGBAgeCount = iawGBAge.count()

}
