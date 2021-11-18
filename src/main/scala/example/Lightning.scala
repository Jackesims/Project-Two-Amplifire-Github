package example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Lightning {

    val spark = SparkSession
        .builder()
        .master("local[1]")
        .appName("Spark SQL - Different ways to create DataFrames")
        .getOrCreate()
    
    val sc = spark.sparkContext
        spark.sparkContext.setLogLevel("ERROR")


    val allDataDF = spark.read
        .parquet("/user/maria_dev/Mortality Data/Real Data/merged_data.parquet")


    def getMostByAgeLightning() : Unit = {

        val allDataDF = spark.read.parquet("/user/maria_dev/merged_data.parquet")   
                
        val avgerageAge = allDataDF.filter("358_cause_recode = 416")
        .agg( avg("detail_age").as("Avg Age") )
        .show()
        
        val lightningDF = allDataDF.where(col("358_cause_recode") === "416")
        .groupBy(col("age_recode_52"))
        .count()
        .orderBy(col("count").desc).toDF()
        .show(52,100, false)
    }
}