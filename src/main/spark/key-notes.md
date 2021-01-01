````
// In Scala
import org.apache.spark.sql.SparkSession
// Build SparkSession
val spark = SparkSession
.builder
.appName("LearnSpark")
.config("spark.sql.shuffle.partitions", 6)
.getOrCreate()
...
// Use the session to read JSON
val people = spark.read.json("...")
...
// Use the session to issue a SQL query
val resultsDF = spark.sql("SELECT city, pop, state, zip FROM table_name")
````
### Repartition
````
# In Python
log_df = spark.read.text("path_to_large_text_file").repartition(8)
print(log_df.rdd.getNumPartitions())
````
The performance improvements in Spark 2.x and Spark 3.0, due to the Catalyst optimizer for SQL and Tungsten for compact code generation, have made life for data
engineers much easier

## Narrow and Wide Transformations
Single transformation is called as Narrow and multiple transformation is wide.

````
// In Scala
import org.apache.spark.sql.types._
val schema = StructType(Array(StructField("author", StringType, false),
StructField("title", StringType, false),
StructField("pages", IntegerType, false)))

other way
// In Scala
val schema = "author STRING, title STRING, pages INT"

val schema = StructType(Array(StructField("Id", IntegerType, false),
StructField("First", StringType, false),
StructField("Last", StringType, false),
StructField("Url", StringType, false),
StructField("Published", StringType, false),
StructField("Hits", IntegerType, false),
StructField("Campaigns", ArrayType(StringType), false)))
// Create a DataFrame by reading from the JSON file
// with a predefined schema
val blogsDF = spark.read.schema(schema).json(jsonFile)
// Show the DataFrame schema as output
blogsDF.show(false)
````

