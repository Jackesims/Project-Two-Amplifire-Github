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
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConversions._
import org.apache.spark.sql.expressions.Window

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;
import java.io._
import scala.collection.mutable.ListBuffer
import scala.io.Source


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

    // creating a global_temp for patrick's queries
    val iawDF = parqDF1.filter(parqDF1("injury_at_work") === "Y");
    iawDF.createOrReplaceGlobalTempView("iawDFGlobalTemp");
 
 
    /**
      * Prompt to store the dataframe in a folder as a collection of CSV files
      *
      * @param df
      * @param folderPath
      */
    def storeCSV (df: DataFrame, folderPath: String) : Unit = {
        if (scala.io.StdIn.readLine("Store in CSV? >> (y/n)").toLowerCase == "y")
                df.coalesce(1)
                .write
                .mode("overwrite")
                .csv(s"$folderPath")
    }

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

            val lightningDF = parqDF1.where(col("358_cause_recode") === "416")
            .groupBy(col("age_recode_52"))
            .count()
            .orderBy(col("count").desc).toDF()

            lightningDF.show(52,100, false)
            
            val avgerageAge = parqDF1.filter("358_cause_recode = 416")
            .agg( avg("detail_age").as("Avg Age") )
            .show()
         
          storeCSV(lightningDF, "/user/maria_dev/lightningDeaths.csv")
        }
        else if(option == 4) {

            //Jared
            val cancerDF = parqDF1.where(col("39_cause_recode") > 5 && col("39_cause_recode") < 16)
            .select(col("39_cause_recode"), col("sex"))
            .groupBy(col("39_cause_recode"), col("sex"))
            .count()
            .orderBy(col("count").desc).toDF()
            
            val cancerDF2 = cancerDF
            .agg(sum("count").as("Sum of people sho died due to cancer"))

            val cancerDF3 = allDataDF.select(col("39_cause_recode"), col("current_data_year"))
            .where(col("39_cause_recode") > 5 && col("39_cause_recode") < 16)
            .groupBy(col("current_data_year"))
            .count()
            .orderBy(col("count").asc).toDF()

            cancerDF.show(1000,100,false)
            cancerDF2.show(1000,100,false)
            cancerDF3.show(1000,100,false)
        }
        else if(option == 5) {
         
          // Query 1: Manner of Death % Each Year
          parqDF1.createOrReplaceTempView("ParquetTable")
          val parkSQL2005 = spark.sql("SELECT manner_of_death, current_data_year, COUNT(manner_of_death) AS MOD from ParquetTable group BY current_data_year, manner_of_death Order By current_data_year desc")
          //parkSQL2005.repartition(1).write.csv("/user/maria_dev/MannerOutput2005.csv")
          parkSQL2005.show(70,100,false)
          storeCSV(parkSQL2005, "/user/maria_dev/MannerOutput2005.csv")
        }
        else if(option == 6) {
         
          //Query Two: Top Five Infant Death Causes
          parqDF1.createOrReplaceTempView("ParquetTable")
          val parkSQL2 = spark.sql("SELECT 130_infant_cause_recode, COUNT(130_infant_cause_recode) AS ICR from ParquetTable GROUP BY 130_infant_cause_recode ORDER BY ICR DESC ")
          parkSQL2.limit(5)
          parkSQL2.show(5)
          //parkSQL2.repartition(1).write.csv("/user/maria_dev/babyoutouttrue.csv")
          storeCSV(parkSQL2, "/user/maria_dev/babyoutouttrue.csv")
        }
        else if(option == 7) {

        }
        else if(option == 8) {

        }
        else if(option == 9) {

        }
        else if(option == 10) {

          val iawDFAct = spark.sql("SELECT injury_at_work, activity_code FROM global_temp.iawDFGlobalTemp");

          val iawActTotal = iawDFAct.count();
            println(s"""
  Between 2005 and 2015, $iawActTotal people were reported to have died from an injury at work.
            """);
          
          // group by activity
          val iawGBAct = iawDFAct.groupBy("activity_code");

          // count deaths with each activity
          val iawActivityCount = iawGBAct.count();

          iawActivityCount.show()

          storeCSV(iawActivityCount, "/user/maria_dev/injured_at_work/by_activity.csv")

        }
        else if(option == 11) {

          // only show iaw and educ from iaw gt
          val iawDFEd = spark.sql("select injury_at_work, education_reporting_flag, education_1989_revision, education_2003_revision from global_temp.iawDFGlobalTemp")
    
          // "1989 revision of education item on certificate"
          val iawEdu1989DF = iawDFEd.where(col("education_reporting_flag") === "0")
            .groupBy("education_1989_revision")
            .count()
            .orderBy(col("education_1989_revision").desc).toDF

          iawEdu1989DF.show(false)

          storeCSV(iawEdu1989DF, "/user/maria_dev/injured_at_work/by_edu_1989.csv")

          // "2003 revision of education item on certificate"
          val iawEdu2003DF = iawDFEd.where(col("education_reporting_flag") === "1")
            .groupBy("education_2003_revision")
            .count()
            .orderBy(col("education_2003_revision").desc).toDF

          iawEdu2003DF.show(false)

          storeCSV(iawEdu2003DF, "/user/maria_dev/injured_at_work/by_edu_2003.csv")

        }
        else if(option == 12) {

          val iawUnfilterdAgeDF = spark.sql("SELECT injury_at_work, detail_age_type, detail_age FROM global_temp.iawDFGlobalTemp");
    
          val iawAgesDF = iawUnfilterdAgeDF.where(col("detail_age_type") === "1" && col("detail_age") < "999")
            .groupBy("detail_age")
            .count()
            .orderBy(col("detail_age").desc).toDF;

          iawAgesDF.show(125, false);

          storeCSV(iawAgesDF, "/user/maria_dev/injured_at_work/by_age.csv")

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
