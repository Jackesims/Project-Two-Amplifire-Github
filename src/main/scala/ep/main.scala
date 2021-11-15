package ep;

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._

object Main extends App{
    // Program reads from hdfs, creates a df from it, and prints out top 10 rows in column style then prints top10 rows who are women
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("Spark SQL - Functions")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    println("\nReading the data Set...\n")
    val parqDF1 = spark.read.parquet("/user/maria_dev/merged_data.parquet")
    println("Done...\n")
    println("Doing the marital query...")
    parqDF1
      .filter("detail_age > 18")
      .groupBy("sex", "marital_status")
      .agg(
        avg("detail_age").as("Avg Age"), 
        count("*").alias("Total Number") )
      .orderBy("sex","marital_status")
      .show();
    println("Doing Education Query...")
    parqDF1
      .filter("detail_age > 18")
      .groupBy("sex", "education_2003_revision")
      .agg(
        avg("detail_age").as("Avg Age"), 
        count("*").alias("Total Number") )
      .orderBy("sex","education_2003_revision")
      .show(40);
  
  println("DONE....\n")

}