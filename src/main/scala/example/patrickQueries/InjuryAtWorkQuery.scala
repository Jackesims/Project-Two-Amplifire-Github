package example.patrickQueries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class InjuryAtWorkQuery {

    def main: Unit = {

      val spark: SparkSession = SparkSession
        .builder()
        .master("local[3]")
        .appName("PatrickQuery")
        .getOrCreate()

      val sc = spark.sparkContext

      // get the filepath for the parquet file
      val mortDataP = "filepath here"

      // from Ajay's examples:

      // Read Parquet file into DataFrame.
      // println("Read the Parquet file...")
      // val parqDF = spark.read.parquet("/tmp/output/people.parquet")
      // parqDF.printSchema()
      // parqDF.show()

      // Using SQL queries on Parquet.
      // println("Create View for Parquet...")
      // parqDF.createOrReplaceTempView("ParquetTable")
      // println("Explain the SQL statement to be exeucted on the Parquet View...")
      // spark.sql("select * from ParquetTable where salary >= 4000").explain()
      // println("Fetch data from the view using SQL syntax...")
      // val parkSQL = spark.sql("select * from ParquetTable where salary >= 4000")
      // parkSQL.show()
      // parkSQL.printSchema()

      // read the parquet file into a dataframe
      val mortDF = spark.read.parquet(mortDataP)

      // filter the data based on injuries at work
      val iawDF = mortDF.filter(mortDF("injury_at_work") === "Y")
      val iawDFLimited = spark.sql("select injury_at_work, activity_code from iawDF where activity_code > 9")

      // count how many people died from an injury at work
      val iawTotal = iawDFLimited.count()

      // group by activity
      val iawGBActivity = iawDFLimited.groupBy("activity_code")

      // count deaths with each activity
      val iawActivityCount = iawGBActivity.count()



    }

}
