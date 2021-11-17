package ep

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StringType, StructType}


object test {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("Spark SQL - Functions")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
        
    println("\nReading the data Set...\n")
    val DF1 = spark.read.json("file:/home/maria_dev/2010_codes.json")
    val parqDF1 = spark.read.parquet("/user/maria_dev/merged_data.parquet")
    val part = Window.partitionBy("sex","marital_status").orderBy(col("count").desc)
    println("Starting the query...")
    val df2 = parqDF1
      .filter("detail_age > 18")
      .groupBy("sex", "marital_status","358_cause_recode")
      .count()
      .withColumn("CauseRank",row_number.over(part))
      .filter("CauseRank < 6")
      .orderBy("sex","marital_status")
    println("Starting the transformation...")

}
