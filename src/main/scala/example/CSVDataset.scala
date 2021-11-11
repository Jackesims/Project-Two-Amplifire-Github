package example

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


class CSVDataset(var filename: String) {

    val path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/"

    def CheckFileExists(filename: String): Boolean = {
        println("Checking to see if file already exists")
        var CheckExistFlag = Files.exists(Paths.get(filename))
        println(CheckExistFlag)
        return CheckExistFlag
    }

    def copyFromMariaDev(): Unit = {
        val src = "/home/maria_dev/" + filename
        var srcExistsFlag = CheckFileExists(src) 
        println(srcExistsFlag)
        if(srcExistsFlag) {

            // If the file in question exists on /home/maria_dev, execute this block of code.

            println("File exists on maria_dev. Checking to see if file exists in target location.")
            var target = path + filename
            println(target)
            var targetExistsFlag = CheckFileExists(target)
            if(!(targetExistsFlag)) {

                // If the file in question does not exist on hdfs (/user/maria_dev), execute this block of code.

                println(s"Copying local file $src to $target ...")
                val conf = new Configuration()
                val fs = FileSystem.get(conf)
                val localpath = new Path("file://" + src)
                val hdfspath = new Path(target)
                fs.copyFromLocalFile(false, localpath, hdfspath)
                println(s"Done copying local file $src to $target ...")
            } else {
            println("File in question already exists on HDFS. Moving on to next file.")
            } 
        } else {
            println{"File in question does not exist on local (/home/maria_dev). Please upload and re-run .jar file"}
        }
    }
}

object Project2Code{

    // Put any global variables here.

    def main(args: Array[String]): Unit =  {
        // Open the CSV file "csvFileList.csv", read the file, and make a list of strings representing .csv files
        var CSVFileList = readFiletoList("csvFileList.csv")
        // Iterate over the CSVFileList, and upload that file from /home/maria_dev to /user/maria_dev on HDFS.
        for(file <- CSVFileList) {
            var CSVInstance = new CSVDataset(file)
            CSVInstance.copyFromMariaDev()
        }
        println("Starting Project 2 Demo")
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
}