// readparquetdf.scala
package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._



object Proj2 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("DeathMannerTopInfant")
      .getOrCreate()

    val parqDF = spark.read.parquet("/user/maria_dev/ParquetHolder/")

    // Query 1: Manner of Death % Each Year
    parqDF.createOrReplaceTempView("ParquetTable")
    val parkSQL2005 = spark.sql("SELECT manner_of_death, current_data_year, COUNT(manner_of_death) AS MOD from ParquetTable group BY current_data_year, manner_of_death Order By current_data_year desc")
    parkSQL2005.repartition(1).write.csv("/user/maria_dev/MannerOutput2005.csv")
    parkSQL2005.show(70,100,false)
  


    //Query Two: Top Five Infant Death Causes
    val parkSQL2 = spark.sql("SELECT 130_infant_cause_recode, COUNT(130_infant_cause_recode) AS ICR from ParquetTable GROUP BY 130_infant_cause_recode ORDER BY ICR DESC ")
    parkSQL2.limit(5)
    parkSQL2.show(5)
    parkSQL2.repartition(1).write.csv("/user/maria_dev/babyoutouttrue.csv")

   

  
  }
}
