import org.apache.spark.sql.SparkSession


object DatasetBasics extends App{

  case class DeviceIoTData (battery_level: Long, c02_level: Long,
                            cca2: String, cca3: String, cn: String, device_id: Long,
                            device_name: String, humidity: Long, ip: String, latitude: Double,
                            lcd: String, longitude: Double, scale:String, temp: Long,
                            timestamp: Long){}


  val spark = SparkSession.builder().appName("Case classes").master("local").getOrCreate()
  import spark.implicits._

  var iotDF = spark.read.json("src/main/resources/data/iot_devices.json").as[DeviceIoTData]
  iotDF.printSchema()
  iotDF = iotDF.filter(d => { d.temp > 30 && d.humidity > 70})
  iotDF.show(5, false)

  case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long,
                                 cca3: String)
  val dsTemp = iotDF
    .filter(d => {d.temp > 25})
    .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
    .toDF("temp", "device_name", "device_id", "cca3").as[DeviceTempByCountry]

}
