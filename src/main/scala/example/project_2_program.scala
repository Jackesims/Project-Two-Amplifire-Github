package example

import java.io.IOException
import scala.util.Try
// import os._

import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}
import org.apache.spark.sql.types._
import scala.sys.process._

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
            println{"File in question does not exist on local (/home/maria_dev). Check /user/maria_dev for file; otherwise, please upload and re-run .jar file"}
        }
    }
}

object Project2Code{

    // Put any global variables here.
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[3]")
      .appName("Project2App")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext
    def main(args: Array[String]): Unit =  {
        var CSVInstance = new CSVDataset("merged_data.csv")
        // Move merged file to HDFS
        CSVInstance.copyFromMariaDev()
        Autopsy_Query()
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

    def Autopsy_Query(): Unit = {
        println("This query shows what percentage of deaths resulted in autopsies, versus no autopsies, from 2005-2015.")
        var autopsySchema = new StructType().add("autopsy", StringType, true).add("current_data_year", IntegerType, true)
        var autopsyDF = spark.read.format("csv").option("header",true).schema(autopsySchema).load("merged_data.csv")
        //autopsyDF.groupBy("current_data_year","autopsy").count().show(false)
        autopsyDF.show(false)
    }

    def Deadliest_Timeliens(): Unit = {
        println("This is the query for generating the deadliest day of the week, month, and year.")
        
        var dayofweekSchema =   new StructType().add("day_of_week_of_death", IntegerType, true).add("current_data_year", IntegerType, true)
        var dayofweekDF = spark.read.format("csv").option("header",true).schema(dayofweekSchema).load("merged_data.csv")
        dayofweekDF.show(false)

        var monthDeathsSchema = new StructType().add("month_of_death", IntegerType, true).add("current_data_year", IntegerType, true)
        var monthDeathsDF = spark.read.format("csv").option("header",true).schema(monthDeathsSchema).load("merged_data.csv")
        //var processeddayofweekDF = dayofweekDF.groupBy("current_data_year").count()
        monthDeathsDF.show()

        var yearDeathsSchema =  new StructType().add("current_data_year", IntegerType, true)
        var yearDeathsDF = spark.read.format("csv").option("header",true).schema(yearDeathsSchema).load("merged_data.csv")
        yearDeathsDF.show()
    }
}