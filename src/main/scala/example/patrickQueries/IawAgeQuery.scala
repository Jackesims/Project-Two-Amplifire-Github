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
    .getOrCreate();

    val sc = spark.sparkContext;

    val iawUnfilterdAgeDF = spark.sql("SELECT injury_at_work, detail_age_type, detail_age FROM global_temp.iawDFGlobalTemp");
    
    val iawAgesDF = iawUnfilterdAgeDF.where(col("detail_age_type") === "1" && col("detail_age") < "999")
    .groupBy("detail_age")
    .count()
    .orderBy(col("detail_age").desc).toDF;

    iawAgesDF.show(125, false);

    spark.close()

  }

}
