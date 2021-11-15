package ep;

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._

object Main {
    // Program reads from hdfs, creates a df from it, and prints out top 10 rows in column style then prints top10 rows who are women
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("Spark SQL - Functions")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val eduList = List("'1'","'2'","'3'","'4'","'5'","'6'","'7'","'8'");
    val titleList = List("8th Grade Or Less","Some High School, No Diploma","High School Gradute or GED Completed","Some College Credit, But No Degree","Associate Degree","Bachelor's Degree","Master's Degree","Doctorate or Professional Degree");
    println("\nCreating the data Set...\n")
    val df1 = spark.read.parquet("/user/maria_dev/merged_data.parquet")
    println("Data set created\n")
    println("Creating edu parquet with partitions...")
    df1.write.partitionBy("sex","education_2003_revision").parquet("/user/maria_dev/edu_data.parquet")
    println("Read the paritioned parquet...")
    println("\nDone!")

}