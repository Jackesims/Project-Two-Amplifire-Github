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
      // read the parquet file into a dataframe
      // filter the data based on injuries at work
      val mortDataP = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/mortData";
      val mortDF = spark.read.parquet(mortDataP);
      val iawDF = mortDF.filter(mortDF("injury_at_work") === "Y");

      iawDF.count();

      // create a globaltemp view for sql queries
      // select relevant columns
      iawDF.createOrReplaceGlobalTempView("iawDFGlobalTemp");
      val iawDFAct = spark.sql("SELECT injury_at_work, activity_code FROM global_temp.iawDFGlobalTemp");

      // count how many people died from an injury at work
      val iawActTotal = iawDFAct.count();

      // group by activity
      val iawGBAct = iawDFAct.groupBy("activity_code");

      // count deaths with each activity
      val iawActivityCount = iawGBAct.count().show();

    }

}
