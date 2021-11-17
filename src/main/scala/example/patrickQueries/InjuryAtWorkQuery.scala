package example.patrickQueries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class InjuryAtWorkQuery {

    def main: Unit = {

      val spark: SparkSession = SparkSession
        .builder()
        .master("local")
        .appName("PatrickQuery")
        .getOrCreate()

      val sc = spark.sparkContext

      // get the filepath for the parquet file
      val mortDataP = "filepath here"
      
      // read the parquet file into a dataframe
      val mortDF = spark.read.parquet(mortDataP)

      // filter the data based on injuries at work
      val iawDF = mortDF.filter(mortDF("injury_at_work") === "Y")
      iawDF.createOrReplaceGlobalTempView("iawDFGlobalTemp")
      val iawDFAct = spark.sql("select injury_at_work, activity_code from iawDFGlobalTemp where activity_code > 9")

      // count how many people died from an injury at work
      val iawActTotal = iawDFAct.count()

      // group by activity
      val iawGBAct = iawDFAct.groupBy("activity_code")

      // count deaths with each activity
      val iawActivityCount = iawGBAct.count()

    }

}
