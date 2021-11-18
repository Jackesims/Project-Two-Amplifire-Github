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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


class CSVDataset(var filename: String) {

    val path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/"

    def CheckFileExists(filename: String): Boolean = {
        println("Checking to see if file or folder already exists")
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
    clear()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val sc = spark.sparkContext
    clear()
    val MappingDataFrame = spark.read.option("header",true).csv("All_Schemas.csv")

    def main(args: Array[String]): Unit =  {
        var CSVInstance = new CSVDataset("mortData")
        // Move merged file to HDFS
        CSVInstance.copyFromMariaDev()
        val mainDataframe  = spark.read.parquet("/user/maria_dev/mortData/part-*")
        //clear()
        //Autopsy_Query(mainDataframe)
        MainDeathsInactive(mainDataframe)
        //println(mainDataframe.show(false))
    }

    // Global functions go here.

    def clear() : Unit = {
        "clear".!
        Thread.sleep(1000)
    }

    def readFiletoList(filename: String): Seq[String] = {
        // This function opens any file which has lines delimited by returns (\n), and returns
        // a list of those lines.
        val bufferedSource = scala.io.Source.fromFile(filename)
        val lines = (for (line <- bufferedSource.getLines()) yield line).toList
        bufferedSource.close
        return lines
    }

        //Convert the mutable listBuffer object into an immutable list
    def writeFile(filename: String, lines: Seq[String]): Unit = {
        // This function takes a list of strings, and writes them to a file.
        val file = new File(filename)
        val bw = new BufferedWriter(new FileWriter(file))
        for (line <- lines) {
            bw.write(line)
        }
        bw.close()
    }

    def MergeFiles(filelist: Seq[String], merged_filename: String): Unit = {
        var listBufferCSVData = ListBuffer[String]()
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        var FileIterator: Int = 0
        for(file <- filelist) {
            var split1 = file.split(".")(0)
            var year = split1.split("__")(1)
            var LineIterator: Int = 0
            var bufferedSource = Source.fromFile(file)
            val srcPath=new Path(file)
            for (line <- bufferedSource.getLines) {
                if((LineIterator == 0)){
                // If first line of CSV and first file to be accessed, include the line into the listBuffer. This is the CSV file header
                    listBufferCSVData += s"$year\n"
                    listBufferCSVData += line
                }else{
                // If not first line of CSV, then it is a record entry. Include in listBuffer.
                    listBufferCSVData += line
                }
                LineIterator += 1
            }
            bufferedSource.close
            println(srcPath)
            fs.delete(srcPath,true)
            FileIterator += 1
        }
        var listCSVData = listBufferCSVData.toList
        writeFile(merged_filename, listCSVData)
    }

    def getListOfFiles(dir: String):List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            var fileList = d.listFiles.filter(_.isFile).toList
            return fileList
        } else {
            var fileList = List[File]()
            return fileList
        }
    }

    def Autopsy_Query(dataframe: DataFrame): Unit = {
        println("This query shows what percentage of deaths resulted in autopsies, versus no autopsies, from 2005-2015.")
        dataframe.createOrReplaceTempView("FindYears")
        var YearSQL = spark.sql("SELECT DISTINCT current_data_year FROM FindYears ORDER BY current_data_year")
        var YearList = YearSQL.collect().toList 
        dataframe.createOrReplaceTempView("AutopsyView")
        var CSVDataYearList = ListBuffer[String]()
        for(year <- YearList){
            var year_input = year(0)
            var AutopsySQL = spark.sql(s"SELECT autopsy, current_data_year FROM AutopsyView WHERE current_data_year=$year_input")
            var CountDF = AutopsySQL.groupBy("autopsy").count().as("count")
            var TotalCountDF = CountDF.select(sum("count").as("total_cases"))
            var RelativeDF = CountDF.crossJoin(TotalCountDF).withColumn("Relative Perc.", col("count")/col("total_cases"))
            RelativeDF.drop("total_cases")
            RelativeDF.coalesce(1).write.option("header", "true").mode("overwrite").csv(s"datacsv__$year_input.csv")
            var newlist = getListOfFiles(s"/user/maria_dev/datacsv__$year_input.csv")
            print(newlist)
            CSVDataYearList += s"datacsv__$year_input.csv/part-00*"
            //println(CSVDataYearList)
        }
        //MergeFiles(CSVDataYearList, "merged_autopsy_data.csv")
    }

    def MainDeathsInactive(dataframe: DataFrame): Unit = {
        println("This is the query for generating the top five most common ways by which people die while eating, sitting, or resting.")
        dataframe.createOrReplaceTempView("FindInactiveDeaths")
        var YearSQL = spark.sql("SELECT DISTINCT current_data_year FROM FindInactiveDeaths ORDER BY current_data_year")
        var YearList = YearSQL.collect().toList 
        dataframe.createOrReplaceTempView("InactiveDeaths")
        var CSVDataYearList = ListBuffer[String]()
        //MappingDataFrame.show(false)
        MappingDataFrame.createOrReplaceTempView("MappingCodesView")
        for(year <- YearList){
            var year_input = year(0)
            var InactiveSQL = spark.sql(s"SELECT 358_cause_recode, current_data_year FROM InactiveDeaths WHERE ((activity_code=4) AND (current_data_year=$year_input))")
            var CodeColumn = "358_cause_recode"
            var CountDF = InactiveSQL.groupBy("358_cause_recode").count().as("count").sort(col("count").desc).withColumnRenamed("count",s"Cases ($year_input)")
            var TotalCountDF = CountDF.select(sum(s"Cases ($year_input)").as("Total_Cases"))
            var RelativeDF = CountDF.crossJoin(TotalCountDF).withColumn("Relative Perc.", col(s"Cases ($year_input)")/col("Total_Cases")*100)
            RelativeDF.drop("Total_Cases")
            RelativeDF.createOrReplaceTempView("CodeTranslationView")
            RelativeDF.limit(5).show(false)
            var yearString = s"_ - current_data_year - $year_input"
            var CodeSQL = spark.sql("SELECT 358_cause_recode FROM CodeTranslationView")
            var CodeList = CodeSQL.collect().toList
            var CodeReplacement = ListBuffer[Seq[String]]()
            var count = 1
            for(code <- CodeList){
                if(count < 6) {
                    var code_input = code(0).toString()
                    var String2Search : String = s"_ - $CodeColumn - $code_input"
                    var CodeColumnDF = MappingDataFrame.select(s"$String2Search").first()(0)
                    var CodeColumnString = CodeColumnDF.toString()
                    CodeReplacement += Seq(code_input, CodeColumnString)
                    count += 1
                }
            }
            var CodeStringList = CodeReplacement.toList
            println(CodeStringList)
            //var rdd = sc.parallelize(CodeStringList)
            //var schema = Seq("358_cause_recode", "Cause")
            //val data=spark.createDataFrame(CodeStringList).toDF(schema:_*)
            //var Hello = spark.createDataFrame(rdd)
            //rdd.collect().foreach(println)
            //var List2Dataframe = spark.createDataFrame(rdd)
            //List2Dataframe.show(false)
            //var df1 = CodeStringList.toDF()
            //df1.show(false)
            RelativeDF.coalesce(1).write.option("header", "true").mode("overwrite").csv(s"vital_datacsv__$year_input.csv")
            }
        }
    }