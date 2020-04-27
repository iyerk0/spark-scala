import java.text.SimpleDateFormat
import java.time.temporal.TemporalUnit
import java.time.{Duration, Period}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}
import util.SessionGenerator


object ProcessEvents extends App {
  val sg = new SessionGenerator
  lazy val spark = SparkSession.builder
    .master("local[1]")
    .appName("Process EVents")
    .getOrCreate()
  lazy val SQLContext = spark.sqlContext


  val input = Array("u1,2018-01-22 13:04:32",
    "u2,2018-01-22 13:04:35",
    "u2,2018-01-22 15:04:35",
    "u2,2018-01-22 17:04:35",
    "u2,2018-01-25 18:55:08",
    "u3,2018-01-25 18:56:17",
    "u1,2018-01-25 20:51:43",
    "u2,2018-01-31 07:48:43",
    "u3,2018-01-31 07:48:48",
    "u1,2018-02-02 09:40:58",
    "u2,2018-02-02 09:41:01",
    "u1,2018-02-05 14:03:27",
    "u1,2018-02-05 14:14:27",
    "u1,2018-02-05 14:59:27",
    "u1,2018-02-05 15:03:27",
    "u1,2018-02-05 16:03:27",
    "u1,2018-02-05 17:00:27",
    "u1,2018-02-05 18:03:27",
    "u1,2018-02-05 20:01:27",
    "u1,2018-02-05 23:01:27",
    "u1,2018-02-06 00:01:27",
    "u1,2018-02-06 01:01:27",
    "u1,2018-02-06 03:01:27",
    "u1,2018-02-06 13:00:27",
    "u1,2018-02-06 23:03:27")
  val input1 = input.map(Row(_))
  val inputRdd = spark.sparkContext.parallelize(input)

  val inputRdd1 = inputRdd.map(i => {
    val tokens = i.split(",")
    println(s"tokens: ${tokens.mkString(">>")}")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    Row(tokens(0), new java.sql.Timestamp(dateFormat.parse(tokens(1)).getTime))
  })

  import spark.implicits._

  val schema = StructType(Array(
    StructField("userid", StringType, true),
    StructField("ts", TimestampType, true)
  ))

  import org.apache.spark.sql.functions._

  val inputDf = spark.createDataFrame(inputRdd1, schema).withColumn("ts_long", col("ts").cast(LongType))
  val durationInHours = (hours: Int) => Duration.ofHours(hours).getSeconds
  val window = Window.partitionBy("userid").orderBy(inputDf("ts_long"))
  val inputDf1 = inputDf
    .withColumn("session", sg(inputDf("ts_long"),lit(durationInHours(10))).over(window))
  inputDf1.show(40)
  print()
}