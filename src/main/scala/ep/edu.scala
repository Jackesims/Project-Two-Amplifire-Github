package ep;

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._

object edu{
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
    val parqDF2 = spark.read.parquet("/user/maria_dev/data_edu.parquet")

    var avgAgeArray = Array.ofDim[Double](titleList.length,2);

    var count = 0;
    
    var res1 = df1.filter("detail_age > 18").count(); // Total number of individuals

    // Widowed + Married will be counted the same for "married vs single"
    // On the same note, divorced individuals will be counted as single for the broad analysis
    

    println("Total Number of Individuals Above the Age of 18: " + res1);
    for(edu <- eduList) {
        var res = parqDF2.filter("education_2003_revision = " + edu).filter("detail_age > 18").agg(avg("detail_age").as("Avg Age " + titleList(count))).collect();
            
        for(row <- res) {
            println("Average Age of " + titleList(count))
            avgAgeArray(count)(0) = row.getDouble(0)
            println(avgAgeArray(count)(0));
        }
        avgAgeArray(count)(1) = parqDF2.filter("education_2003_revision = " + edu).filter("detail_age > 18").count();
        println(avgAgeArray(count)(1))
        count = count + 1;
    }

}