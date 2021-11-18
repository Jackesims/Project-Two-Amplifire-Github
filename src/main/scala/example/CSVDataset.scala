package example

import java.io.IOException
import scala.util.Try
// import os._

import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}
import scala.sys.process._

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;
import java.io._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.io.StdIn.readInt


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

    val spark: SparkSession = SparkSession
        .builder()
        .master("local[3]")
        .appName("ProjectApp")
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext
    

    // Put any global variables here.

    def main(args: Array[String]): Unit =  {
        var CSVInstance = new CSVDataset("mortality")
        // Move merged file to HDFS
        CSVInstance.copyFromMariaDev()
        val df = spark.read.parquet("/user/maria_dev/mortality/part-*")

        var quit = false;
        var option = 0;
        do {
            println("What query would you like running?")
            println("Show deaths by month (1)")
            println("Show deaths by day of the week (2)")
            println("Show deaths while engaged in sports activities by month (3)")
            println("TO quit (13)")
            try {
                option = scala.io.StdIn.readInt();
                if(option >0 && option <14)
                {
                    if(option == 1) {
                        println("Showing deaths by month...")
                        val jan = df.filter(df("month_of_death") === 1).count()
                        val feb = df.filter(df("month_of_death") === 2).count()
                        val mar = df.filter(df("month_of_death") === 3).count()
                        val apr = df.filter(df("month_of_death") === 4).count()
                        val may = df.filter(df("month_of_death") === 5).count()
                        val jun = df.filter(df("month_of_death") === 6).count()
                        val jul = df.filter(df("month_of_death") === 7).count()
                        val aug = df.filter(df("month_of_death") === 8).count()
                        val sep = df.filter(df("month_of_death") === 9).count()
                        val oct = df.filter(df("month_of_death") === 10).count()
                        val nov = df.filter(df("month_of_death") === 11).count()
                        val dec = df.filter(df("month_of_death") === 12).count()
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
                        println(s"Deaths in December: $dec")
                        
                    }
                    else if(option == 2) {
                        println("\nShowing deaths by day of the week...")
                        val sun = df.filter(df("day_of_week_of_death") === 1).count()
                        val mon = df.filter(df("day_of_week_of_death") === 2).count()
                        val tue = df.filter(df("day_of_week_of_death") === 3).count()
                        val wed = df.filter(df("day_of_week_of_death") === 4).count()
                        val thu = df.filter(df("day_of_week_of_death") === 5).count()
                        val fri = df.filter(df("day_of_week_of_death") === 6).count()
                        val sat = df.filter(df("day_of_week_of_death") === 7).count()
                        println(s"Deaths on Sundays: $sun")
                        println(s"Deaths on Mondays: $mon")
                        println(s"Deaths on Tuesdays: $tue")
                        println(s"Deaths on Wednesdays: $wed")
                        println(s"Deaths on Thursdays: $thu")
                        println(s"Deaths on Fridays: $fri")
                        println(s"Deaths on Saturdays: $sat")
                    }
                    else if(option == 3) {
                        println("\nShowing deaths while engaged in sports activities by month...")
                        val jan = df.filter(df("month_of_death") === 1 && df("activity_code") === 0).count()
                        val feb = df.filter(df("month_of_death") === 2 && df("activity_code") === 0).count()
                        val mar = df.filter(df("month_of_death") === 3 && df("activity_code") === 0).count()
                        val apr = df.filter(df("month_of_death") === 4 && df("activity_code") === 0).count()
                        val may = df.filter(df("month_of_death") === 5 && df("activity_code") === 0).count()
                        val jun = df.filter(df("month_of_death") === 6 && df("activity_code") === 0).count()
                        val jul = df.filter(df("month_of_death") === 7 && df("activity_code") === 0).count()
                        val aug = df.filter(df("month_of_death") === 8 && df("activity_code") === 0).count()
                        val sep = df.filter(df("month_of_death") === 9 && df("activity_code") === 0).count()
                        val oct = df.filter(df("month_of_death") === 10 && df("activity_code") === 0).count()
                        val nov = df.filter(df("month_of_death") === 11 && df("activity_code") === 0).count()
                        val dec = df.filter(df("month_of_death") === 12 && df("activity_code") === 0).count()
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
                        println(s"Deaths while engaged in sports activities in December: $dec")
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
                case _: Throwable =>("Wrong input. Try again\n")
            }
        } while(!quit)
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