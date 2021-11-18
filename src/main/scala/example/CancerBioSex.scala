package example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object CancerBioSex{

    val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("Spark SQL - Different ways to create DataFrames")
    .getOrCreate()
    
    val sc = spark.sparkContext
        spark.sparkContext.setLogLevel("ERROR")


    val allDataDF = spark.read
        .parquet("/user/maria_dev/Mortality Data/Real Data/merged_data.parquet")

    def getCancerGroupBioSex() : Unit = {
        val cancerDF = allDataDF.where(col("39_cause_recode") > 5 && col("39_cause_recode") < 16)
        .select(col("39_cause_recode"), col("sex"))
        .groupBy(col("39_cause_recode"), col("sex"))
        .count()
        .orderBy(col("count").desc).toDF()

        cancerDF.show(1000,100,false)
    }
}