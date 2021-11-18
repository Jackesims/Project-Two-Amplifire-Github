package example.patrickQueries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class IawEducationQuery {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("PatrickQuery")
    .getOrCreate()

  val sc = spark.sparkContext

  // only show iaw and educ from iaw gt
  // _ double check the code for educ
  val iawDFEd = spark.sql("select injury_at_work, education_code from globaltemp.iawDFGlobalTemp")

  val iawGBEd = iawDFEd.groupBy("education_code")

  val iawGBEdCount = iawGBEd.count()

}
