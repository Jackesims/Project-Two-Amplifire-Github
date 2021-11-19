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
    println("Print lightning analysis (3)")
    println("Print cancer analysis (4)")
    println("Print Manner of Death analysis (5)")
    println("Print Infant Death analysis (6)")
    println("Print Deaths each Month (7)")
    println("Print Deaths each Week (8)")
    println("Print Monthly Sport Deaths (9)")
    println("Print Injury at Work by Activity (10)")
    println("Print Injury at Work by Education (11)")
    println("Print Injury at Work by Age (12)")
    println("Print Autopsy analysis (13)")
    println("Print Inactive Death analysis (14)")
    println("To quit (15)")
    try {
      option = scala.io.StdIn.readInt();
      if(option >0 && option < 16)
      {
        if(option == 1) {
          // Marital Query
          println("Doing the marital query...")
          val output1 = parqDF1
            .filter("detail_age > 18")
            .groupBy("sex", "marital_status")
            .agg(
              avg("detail_age").as("Avg Age"), 
              count("*").alias("Total Number") )
            .orderBy("sex","marital_status")
            .coalesce(1)
            
            output1.show(50)
            //.write.csv("/user/maria_dev/output1.csv");
            storeCSV(output1, "/user/maria_dev/output1.csv")

          val part = Window.partitionBy("sex","marital_status").orderBy(col("count").desc)
          val output2 = parqDF1
            .filter("detail_age > 18")
            .groupBy("sex", "marital_status","358_cause_recode")
            .count()
            .withColumn("CauseRank",row_number.over(part))
            .filter("CauseRank < 6")
            .orderBy("sex","marital_status")
            .coalesce(1)
            
            output2.show(50)
            //.write.csv("/user/maria_dev/output3.csv")
            storeCSV(output2, "/user/maria_dev/output2.csv")

        }
        else if(option == 2) {
          // Education Query
          println("\nDoing Education Query...")
          val output3 = parqDF1
            .filter("detail_age > 18")
            .groupBy("sex", "education_2003_revision")
            .agg(
              avg("detail_age").as("Avg Age"), 
              count("*").alias("Total Number") )
            .orderBy("sex","education_2003_revision")
            .coalesce(1)
            
            output3.show(50)
            //.write.csv("/user/maria_dev/output2.csv");
            storeCSV(output3, "/user/maria_dev/output2.csv")
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

            val cancerDF3 = parqDF1.select(col("39_cause_recode"), col("current_data_year"))
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
            println("Showing deaths by month...")
            val jan = parqDF1.filter(parqDF1("month_of_death") === 1).count()
            val feb = parqDF1.filter(parqDF1("month_of_death") === 2).count()
            val mar = parqDF1.filter(parqDF1("month_of_death") === 3).count()
            val apr = parqDF1.filter(parqDF1("month_of_death") === 4).count()
            val may = parqDF1.filter(parqDF1("month_of_death") === 5).count()
            val jun = parqDF1.filter(parqDF1("month_of_death") === 6).count()
            val jul = parqDF1.filter(parqDF1("month_of_death") === 7).count()
            val aug = parqDF1.filter(parqDF1("month_of_death") === 8).count()
            val sep = parqDF1.filter(parqDF1("month_of_death") === 9).count()
            val oct = parqDF1.filter(parqDF1("month_of_death") === 10).count()
            val nov = parqDF1.filter(parqDF1("month_of_death") === 11).count()
            val dec = parqDF1.filter(parqDF1("month_of_death") === 12).count()
            println(s"Deaths in January: $jan")
            println(s"Deaths in February: $feb")
            println(s"Deaths in March: $mar")
            println(s"Deaths in April: $apr")
            println(s"Deaths in May: $may")
            println(s"Deaths in June: $jun")
            println(s"Deaths in July: $jul")
            println(s"Deaths in August: $aug")
            println(s"Deaths in September: $sep")
            println(s"Deaths in October: $oct")
            println(s"Deaths in November: $nov")
            println(s"Deaths in December: $dec\n\n")
        }
        else if(option == 8) {
            println("\nShowing deaths by day of the week...")
            val sun = parqDF1.filter(parqDF1("day_of_week_of_death") === 1).count()
            val mon = parqDF1.filter(parqDF1("day_of_week_of_death") === 2).count()
            val tue = parqDF1.filter(parqDF1("day_of_week_of_death") === 3).count()
            val wed = parqDF1.filter(parqDF1("day_of_week_of_death") === 4).count()
            val thu = parqDF1.filter(parqDF1("day_of_week_of_death") === 5).count()
            val fri = parqDF1.filter(parqDF1("day_of_week_of_death") === 6).count()
            val sat = parqDF1.filter(parqDF1("day_of_week_of_death") === 7).count()
            println(s"Deaths on Sundays: $sun")
            println(s"Deaths on Mondays: $mon")
            println(s"Deaths on Tuesdays: $tue")
            println(s"Deaths on Wednesdays: $wed")
            println(s"Deaths on Thursdays: $thu")
            println(s"Deaths on Fridays: $fri")
            println(s"Deaths on Saturdays: $sat\n\n")
        }
        else if(option == 9) {
            println("\nShowing deaths while engaged in sports activities by month...")
            val jan = parqDF1.filter(parqDF1("month_of_death") === 1 && parqDF1("activity_code") === 0).count()
            val feb = parqDF1.filter(parqDF1("month_of_death") === 2 && parqDF1("activity_code") === 0).count()
            val mar = parqDF1.filter(parqDF1("month_of_death") === 3 && parqDF1("activity_code") === 0).count()
            val apr = parqDF1.filter(parqDF1("month_of_death") === 4 && parqDF1("activity_code") === 0).count()
            val may = parqDF1.filter(parqDF1("month_of_death") === 5 && parqDF1("activity_code") === 0).count()
            val jun = parqDF1.filter(parqDF1("month_of_death") === 6 && parqDF1("activity_code") === 0).count()
            val jul = parqDF1.filter(parqDF1("month_of_death") === 7 && parqDF1("activity_code") === 0).count()
            val aug = parqDF1.filter(parqDF1("month_of_death") === 8 && parqDF1("activity_code") === 0).count()
            val sep = parqDF1.filter(parqDF1("month_of_death") === 9 && parqDF1("activity_code") === 0).count()
            val oct = parqDF1.filter(parqDF1("month_of_death") === 10 && parqDF1("activity_code") === 0).count()
            val nov = parqDF1.filter(parqDF1("month_of_death") === 11 && parqDF1("activity_code") === 0).count()
            val dec = parqDF1.filter(parqDF1("month_of_death") === 12 && parqDF1("activity_code") === 0).count()
            println(s"Deaths while engaged in sports activities in January: $jan")
            println(s"Deaths while engaged in sports activities in February: $feb")
            println(s"Deaths while engaged in sports activities in March: $mar")
            println(s"Deaths while engaged in sports activities in April: $apr")
            println(s"Deaths while engaged in sports activities in May: $may")
            println(s"Deaths while engaged in sports activities in June: $jun")
            println(s"Deaths while engaged in sports activities in July: $jul")
            println(s"Deaths while engaged in sports activities in August: $aug")
            println(s"Deaths while engaged in sports activities in September: $sep")
            println(s"Deaths while engaged in sports activities in October: $oct")
            println(s"Deaths while engaged in sports activities in November: $nov")
            println(s"Deaths while engaged in sports activities in December: $dec\n\n")
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
          Autopsy_Query(parqDF1)
        }
        else if(option == 14) {
          MainDeathsInactive(parqDF1)
        }
        else if(option == 15) {
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
      val MappingDataFrame = spark.read.option("header",true).csv("./All_Schemas.csv")
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
