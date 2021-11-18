package example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object VitalAct {

    val spark = SparkSession
        .builder()
        .master("local[1]")
        .appName("Spark SQL - Different ways to create DataFrames")
        .getOrCreate()
    
    val sc = spark.sparkContext
        spark.sparkContext.setLogLevel("ERROR")


    val allDataDF = spark.read
        .parquet("/user/maria_dev/Mortality Data/Real Data/merged_data.parquet")

        
    def getVitalDeathsWith358() : Unit = {
        //First one gives the years, second gives the top deaths by cause
        val vitalDF = allDataDF.where(col("activity_code") === "4")
        .groupBy(col("current_data_year"))
        .count()
        .orderBy(col("count").desc).toDF()
        .show(13,100, false)

        val vitalDF2 = allDataDF.where(col("activity_code") === "4")
        .groupBy(col("358_cause_recode"))
        .count()
        .orderBy(col("count").desc).toDF()
        .limit(10)
        .show(456,100,false)
    }
}