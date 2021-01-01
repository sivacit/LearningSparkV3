import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Avgframe extends App{
  val spark = SparkSession.builder().master("local").appName("Group and Average").getOrCreate()
  val data_df = spark.createDataFrame(Seq(("Broker", 20), ("Yaanu", 9), ("Siva", 20), ("Siva", 38), ("Krishnasamy", 77))).toDF("Name", "Salary")
  val avgDF = data_df.groupBy("Name").agg(avg("Salary"))
  avgDF.show(10)
}
