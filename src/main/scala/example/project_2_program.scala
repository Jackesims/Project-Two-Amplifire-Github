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
import org.apache.spark.sql.expressions.Window

class Dataset(var filename: String) {

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
    val MappingDataFrame = spark.read.option("header",true).csv("mortData/All_Schemas.csv")

    def main(args: Array[String]): Unit =  {
        var CSVInstance = new Dataset("mortData")
        // Move merged file to HDFS
        CSVInstance.copyFromMariaDev()
        val mainDataframe  = spark.read.parquet("/user/maria_dev/mortData/Merged_Parquet/part-*")
        //clear()
        //Autopsy_Query(mainDataframe)
        //MainDeathsInactive(mainDataframe)
        //println(mainDataframe.show(false))
          var quit = false;
        var option = 0;
        do {
            println("What query would you like running?")
            println("Print marital analysis (1)")
            println("Print education analysis (2)")
            println("Print autopsy versus no autopsy (11)")
            println("Print leading causes of death while eating, sleeping, and resting (12)")
            println("To quit (13)")
            try {
            option = scala.io.StdIn.readInt();
            if(option >0 && option < 14)
            {
                if(option == 1) {
                // Marital Query
                    println("Doing the marital query...")
                    mainDataframe
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
                    mainDataframe
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
                    mainDataframe
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
                    val lightningDF = mainDataframe.where(col("358_cause_recode") === "416")
                    .groupBy(col("age_recode_52"))
                    .count()
                    .orderBy(col("count").desc).toDF()
                    .show(52,100, false)
                    
                    val avgerageAge = mainDataframe.filter("358_cause_recode = 416")
                    .agg( avg("detail_age").as("Avg Age") )
                    .show()
                }
                else if(option == 4) {
                    val cancerDF = mainDataframe.where(col("39_cause_recode") > 5 && col("39_cause_recode") < 16)
                    .select(col("39_cause_recode"), col("sex"))
                    .groupBy(col("39_cause_recode"), col("sex"))
                    .count()
                    .orderBy(col("count").desc).toDF()

                    cancerDF.show(1000,100,false)
                }
                else if(option == 5) {
                    println("This is option 5")
                }
                else if(option == 6) {
                    println("This is option 6")
                }
                else if(option == 7) {
                    println("This is option 7")
                }
                else if(option == 8) {
                    println("This is option 8")
                }
                else if(option == 9) {
                    println("This is option 9")
                }
                else if(option == 10) {
                    println("This is option 10")
                }
                else if(option == 11) {
                    Autopsy_Query(mainDataframe)
                }
                else if(option == 12) {
                    MainDeathsInactive(mainDataframe)
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
        } 
        while(!quit)
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


    def Autopsy_Query(dataframe: DataFrame): Unit = {
        println("This query shows what percentage of deaths resulted in autopsies, versus no autopsies, from 2005-2015.")
        dataframe.createOrReplaceTempView("FindYears")
        var YearSQL = spark.sql("SELECT DISTINCT current_data_year FROM FindYears ORDER BY current_data_year")
        var YearList = YearSQL.collect().toList 
        dataframe.createOrReplaceTempView("AutopsyView")
        println("What type of analysis to return? (Provide 'Total' or 'Annual')")
        var UserInput = scala.io.StdIn.readLine()
        if(UserInput=="Total"){
            var AutopsySQL = spark.sql(s"SELECT autopsy, current_data_year FROM AutopsyView")
            var CountDF = AutopsySQL.groupBy("autopsy").count().as("count")
            var TotalCountDF = CountDF.select(sum("count").as("total_cases"))
            var RelativeDF = CountDF.crossJoin(TotalCountDF).withColumn("Relative Perc.", col("count")/col("total_cases"))
            println(s"The autopsy statistics for 2005-2015 are presented in the following table:")
            RelativeDF.show(false)
            RelativeDF.coalesce(1).write.option("header", "true").mode("overwrite").csv(s"mortData/Autopsies/Autopsies_2005_through_2015.csv")
        }else if (UserInput=="Annual") {
            var CSVDataYearList = ListBuffer[String]()
            for(year <- YearList){
                var year_input = year(0)
                var AutopsySQL = spark.sql(s"SELECT autopsy, current_data_year FROM AutopsyView WHERE current_data_year=$year_input")
                var CountDF = AutopsySQL.groupBy("autopsy").count().as("count").withColumnRenamed("count",s"Cases ($year_input)")
                var TotalCountDF = CountDF.select(sum(s"Cases ($year_input)").as("Total"))
                var RelativeDF = CountDF.crossJoin(TotalCountDF).withColumn("Relative Perc.", col(s"Cases ($year_input)")/col("Total")*100)
                println(s"The autopsy statistics for $year_input are presented in the following table:")
                RelativeDF.drop(RelativeDF("Total"))
                RelativeDF.show(false)
                RelativeDF.coalesce(1).write.option("header", "true").mode("overwrite").csv(s"mortData/Autopsies/Yearly/datacsv__$year_input.csv")
            }
        }
    }

    def MainDeathsInactive(dataframe: DataFrame): Unit = {
        println("This is the query for generating the top five most common ways by which people die while eating, sitting, or resting.")
        dataframe.createOrReplaceTempView("FindInactiveDeaths")
        var YearSQL = spark.sql("SELECT DISTINCT current_data_year FROM FindInactiveDeaths ORDER BY current_data_year")
        var YearList = YearSQL.collect().toList 
        dataframe.createOrReplaceTempView("InactiveDeaths")
        var CSVDataYearList = ListBuffer[String]()
        MappingDataFrame.createOrReplaceTempView("MappingCodesView")
        for(year <- YearList){
            var year_input = year(0)
            var InactiveSQL = spark.sql(s"SELECT 358_cause_recode, current_data_year FROM InactiveDeaths WHERE ((activity_code=4) AND (current_data_year=$year_input))")
            var CountDF = InactiveSQL.groupBy("358_cause_recode").count().as("count").sort(col("count").desc).withColumnRenamed("count",s"Cases ($year_input)")
            var TotalCountDF = CountDF.select(sum(s"Cases ($year_input)").as("Total_Cases"))
            var RelativeDF = CountDF.crossJoin(TotalCountDF).withColumn("Relative Perc.", col(s"Cases ($year_input)")/col("Total_Cases")*100)
            RelativeDF.createOrReplaceTempView("CodeTranslationView")
            RelativeDF.limit(5).show(false)
            var yearString = s"_ - current_data_year - $year_input"
            var CodeSQL = spark.sql("SELECT 358_cause_recode FROM CodeTranslationView")
            var CodeList = CodeSQL.collect().toList
            var CodeReplacement = ListBuffer[String]()
            var count = 1
            for(code <- CodeList){
                if(count < 6) {
                    var code_input = code(0).toString()
                    var String2Search : String = s"_ - 358_cause_recode - $code_input"
                    var CodeColumnDF = MappingDataFrame.select(s"$String2Search").first()(0)
                    var CodeColumnString = CodeColumnDF.toString()
                    CodeReplacement += CodeColumnString
                    count += 1
                }
            }
            var CodeStringList = CodeReplacement.toList
            var CodeDF = CodeStringList.toDS().toDF()
            //empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full").show(truncate=False)
            CodeDF.show(false)
            //var PresentDF = RelativeDF.withColumn("Cause", CodeDF("value"))
            //PresentDF.show(false)
            RelativeDF.coalesce(1).write.option("header", "true").mode("overwrite").csv(s"mortData/InactiveDeaths/Yearly/vital_datacsv__$year_input.csv")
            }
        }
    }