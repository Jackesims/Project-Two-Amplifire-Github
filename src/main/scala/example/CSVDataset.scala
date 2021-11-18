package example

import java.io.IOException
import scala.util.Try
// import os._

import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}
import scala.sys.process._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._
import org.apache.spark.sql.expressions.Window

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;
import java.io._
import scala.collection.mutable.ListBuffer
import scala.io.Source

class CSVDataset(var filename: String) {

    val path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/"

    def CheckFileExists(filename: String): Boolean = {
        println("Checking to see if file already exists")
        var CheckExistFlag = Files.exists(Paths.get(filename))
        return CheckExistFlag
    }

    def copyFromMariaDev(): Unit = {
        val src = "/home/maria_dev/" + filename
        var srcExistsFlag = CheckFileExists(src) 
        if(srcExistsFlag) {

            // If the file in question exists on /home/maria_dev, execute this block of code.

            println("File exists on maria_dev. Checking to see if file exists in target location.")
            var target = path + filename
            var targetExistsFlag = CheckFileExists(target)
            if(!(targetExistsFlag)) {

                // If the file in question does not exist on hdfs (/user/maria_dev), execute this block of code.

                println(s"Copying local file $src to $target ...")
                val conf = new Configuration()
                val fs = FileSystem.get(conf)
                val localpath = new Path("file://" + src)
                val hdfspath = new Path(target)
                fs.copyFromLocalFile(true, true, localpath, hdfspath)
                println(s"Done copying local file $src to $target ...")
            } else {
            println("File in question already exists on HDFS. Moving on to next file.")
            } 
        } else {
            println{"File in question does not exist on local (/home/maria_dev). Please upload and re-run .jar file"}
        }
    }
}

object CSVDataset {

    // Put any global variables here.

    def start : Unit = {
        var CSVInstance = new CSVDataset("merged_data.csv")
        // Move merged file to HDFS
        CSVInstance.copyFromMariaDev()
    }

    // Global functions go here.

    def readFiletoList(filename: String): Seq[String] = {
        // This function opens any file which has lines delimited by returns (\n), and returns
        // a list of those lines.
        val bufferedSource = scala.io.Source.fromFile(filename)
        val lines = (for (line <- bufferedSource.getLines()) yield line).toList
        bufferedSource.close
        return lines
    }

    // def writeFile(filename: String, lines: Seq[String]): Unit = {
    //     // This function takes a list of strings, and writes them to a file.
    //     val file = new File(filename)
    //     val bw = new BufferedWriter(new FileWriter(file))
    //     for (line <- lines) {
    //         bw.write(line)
    //     }
    //     bw.close()
    // }

}

object Main extends App {
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

  var quit = false;
  var option = 0;
  do {
    println("What query would you like running?")
    println("Print marital analysis (1)")
    println("Print education analysis (2)")
    println("To quit (13)")
    try {
      option = scala.io.StdIn.readInt();
      if(option >0 && option < 14)
      {
        if(option == 1) {
          // Marital Query
          println("Doing the marital query...")
          parqDF1
            .filter("detail_age > 18")
            .groupBy("sex", "marital_status")
            .agg(
              avg("detail_age").as("Avg Age"), 
              count("*").alias("Total Number") )
            .orderBy("sex","marital_status")
            .coalesce(1)
            .show(50)
            //.write.csv("/user/maria_dev/output1.csv");
          
          val part = Window.partitionBy("sex","marital_status").orderBy(col("count").desc)
          parqDF1
            .filter("detail_age > 18")
            .groupBy("sex", "marital_status","358_cause_recode")
            .count()
            .withColumn("CauseRank",row_number.over(part))
            .filter("CauseRank < 6")
            .orderBy("sex","marital_status")
            .coalesce(1)
            .show(50)
            //.write.csv("/user/maria_dev/output3.csv")

        }
        else if(option == 2) {
          // Education Query
          println("\nDoing Education Query...")
          parqDF1
            .filter("detail_age > 18")
            .groupBy("sex", "education_2003_revision")
            .agg(
              avg("detail_age").as("Avg Age"), 
              count("*").alias("Total Number") )
            .orderBy("sex","education_2003_revision")
            .coalesce(1)
            .show(50)
            //.write.csv("/user/maria_dev/output2.csv");
        }
        else if(option == 3) {
            
            //Jared
            val allDataDF = spark.read.parquet("/user/maria_dev/merged_data.parquet")   

            val lightningDF = parqDF1.where(col("358_cause_recode") === "416")
            .groupBy(col("age_recode_52"))
            .count()
            .orderBy(col("count").desc).toDF()
            .show(52,100, false)
            
            val avgerageAge = parqDF1.filter("358_cause_recode = 416")
            .agg( avg("detail_age").as("Avg Age") )
            .show()
        }
        else if(option == 4) {

            //Jared
            //First one gives the years, second gives the top deaths by cause
            val vitalDF = parqDF1.where(col("activity_code") === "4")
            .groupBy(col("current_data_year"))
            .count()
            .orderBy(col("count").desc).toDF()
            .show(13,100, false)

            val vitalDF2 = parqDF1.where(col("activity_code") === "4")
            .groupBy(col("358_cause_recode"))
            .count()
            .orderBy(col("count").desc).toDF()
            .limit(10)
            .show(456,100,false)
        }
        else if(option == 5) {

        }
        else if(option == 6) {

        }
        else if(option == 7) {

        }
        else if(option == 8) {

        }
        else if(option == 9) {

        }
        else if(option == 10) {

        }
        else if(option == 11) {

        }
        else if(option == 12) {

        }
        else if(option == 13) {
          println("Exiting...")
          quit = true;
        }

      }
      else
        println("Wrong option. Try again\n")
    }
    catch {
      case _: Throwable => println("Wrong input. Try again\n")
    }

  } while(!quit)   
}
