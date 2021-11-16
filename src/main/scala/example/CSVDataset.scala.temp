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

object Project2Code{

    // Put any global variables here.

    def main(args: Array[String]): Unit =  {
        // Open the CSV file "csvFileListTest.csv", read the file, and make a list of strings representing .csv files
        var CSVFileList = readFiletoList("csvFileListTest.csv")
        // Create ListBuffer object that serves as a list, but is mutable.
        var listBufferCSVData = ListBuffer[String]()
        var FileIterator = 0
        // Iterate over the CSVFileList, and extract each line to listBufferCSVData. Also, don't include multiple headers, and append a last column to include the year.
        for(file <- CSVFileList) {
            var year = file.split("_")(0)
            var LineIterator = 0
            var bufferedSource = Source.fromFile(file)
            for (line <- bufferedSource.getLines) {
                if((LineIterator == 0) & (FileIterator==0)){
                // If first line of CSV and first file to be accessed, include the line into the listBuffer. This is the CSV file header
                    println(s"Merging $file")
                    listBufferCSVData += line + ",year\n"
                }else if((LineIterator == 0) & (FileIterator!=0)){
                // If first line of CSV but not first file to be accessed, don't include the line into the listBuffer. This is a redundant header row
                    println(s"Merging $file")
                }else{
                // If not first line of CSV, then it is a record entry. Include in listBuffer.
                    listBufferCSVData += line + s",$year\n"
                }
                LineIterator += 1
            }
            bufferedSource.close
            FileIterator += 1
        // Convert the mutable listBuffer object into an immutable list
        var listCSVData = listBufferCSVData.toList
        // Create a CSV file representing all of the merged data
        writeFile("Merged_CSV.csv", listCSVData)
        var CSVInstance = new CSVDataset("Merged_CSV.csv")
        // Move merged file to HDFS
        CSVInstance.copyFromMariaDev()
        }
    }

    // Global functions go here.

    def writeFile(filename: String, lines: Seq[String]): Unit = {
        // This function takes a list of strings, and writes them to a file.
        val file = new File(filename)
        val bw = new BufferedWriter(new FileWriter(file))
        for (line <- lines) {
            bw.write(line)
        }
        bw.close()
    }

    def readFiletoList(filename: String): Seq[String] = {
        // This function opens any file which has lines delimited by returns (\n), and returns
        // a list of those lines.
        val bufferedSource = scala.io.Source.fromFile(filename)
        val lines = (for (line <- bufferedSource.getLines()) yield line).toList
        bufferedSource.close
        return lines
    }
}