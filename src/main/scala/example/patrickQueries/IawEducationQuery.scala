package example.patrickQueries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class IawEducationQuery {
  
  def eduQuery: Unit = {

    val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("PatrickQuery3")
    .getOrCreate()

    val sc = spark.sparkContext

    // only show iaw and educ from iaw gt
    val iawDFEd = spark.sql("select injury_at_work, education_reporting_flag, education_1989_revision, education_2003_revision from global_temp.iawDFGlobalTemp")
    
    // "1989 revision of education item on certificate"
    val iawEdu1989DF = iawDFEd.where(col("education_reporting_flag") === "0")
    .groupBy("education_1989_revision")
    .count()
    .orderBy(col("education_1989_revision").desc).toDF

    iawEdu1989DF.show(false)

    // "2003 revision of education item on certificate"
    val iawEdu2003DF = iawDFEd.where(col("education_reporting_flag") === "1")
    .groupBy("education_2003_revision")
    .count()
    .orderBy(col("education_2003_revision").desc).toDF

    iawEdu2003DF.show(false)

    spark.close()
    
  }
  
}
