package ep;

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._

object maritalStatus extends App {
    // Program reads from hdfs, creates a df from it, and prints out top 10 rows in column style then prints top10 rows who are women
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("Spark SQL - Functions")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val maritalStatusList = List("'M'","'S'","'W'","'D'");
    val sexList = List("'M'","'F'");
    val titleList = List("Married Men","Married Female", "Single Men", "Single Women", "Widowed Men", "Widowed Women","Divorced Men","Divorced Women");

    println("Reading from the file...")
    val parqDF2 = spark.read.parquet("/user/maria_dev/data_gm.parquet")
    println("Done...\n")

    var avgAgeArray = Array.ofDim[Double](titleList.length,2);

    var count = 0;


    parqDF2.filter("detail_age > 18").groupBy("sex", "marital_status").count().agg(avg("detail_age").as("Avg Age")).show();
    // Widowed + Married will be counted the same for "married vs single"
    // On the same note, divorced individuals will be counted as single for the broad analysis
    
    /*
    for(ms <- maritalStatusList) {
        for(s <- sexList) {
            var res = parqDF2.filter("sex == " +s ).filter("marital_status = " + ms).filter("detail_age > 18").agg(avg("detail_age").as("Avg Age " + titleList(count))).collect();
            
            for(row <- res) {
                println("Average Age of " + titleList(count))
                avgAgeArray(count)(0) = row.getDouble(0)
                println(avgAgeArray(count)(0));
            }
            avgAgeArray(count)(1) = parqDF2.filter("sex == " +s ).filter("marital_status = " + ms).filter("detail_age > 18").count();
            println(avgAgeArray(count)(1))
            parqDF2.filter("sex == " +s ).filter("marital_status = " + ms).filter("detail_age > 18").groupBy("358_cause_recode").count().orderBy(col("count").desc).show(10)
            count = count + 1;
        }
    }
    */
}