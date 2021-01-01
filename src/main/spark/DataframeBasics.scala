import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object DataframeBasics extends App {

  //Step 1
  val spark = SparkSession.builder().appName("Dataframe basics").config("spark.master", "local").getOrCreate()

  //Step 2
//  option 1
//  val firstDF = spark.read.option("inferSchema", "true").format("json").load("src/main/resources/data/cars.json")
  val firstDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")
  // show first 20 records
  // firstDF.show()
  firstDF.printSchema()
  firstDF.take(10).foreach(println)

//  Spark Types
  val longTime = LongType

// Schema
  val carSchema = StructType(Array(
    StructField("Name", StringType, nullable = true)
  ))
  val phoneSchema = StructType(Array(
    StructField("Name", StringType),

  ))
  var phones = Seq(
    ("Iphone", "7", "6\"", "24 Megapixel"),
    ("Samsug", "10", "6\"", "48 Megapixel"),
    ("MI-5", "11", "7\"", "48 Megapixel")
  )
  val phonesDf =  spark.createDataFrame(phones)
  phonesDf.printSchema()
}
