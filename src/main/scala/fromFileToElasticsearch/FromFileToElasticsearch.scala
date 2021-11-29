package fromFileToElasticsearch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

class FromFileToElasticsearch {

  def sendDataFromFileToKafka(file: String, topic: String, brokers: String): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("fromFileToKafka")
      .getOrCreate()

    LoggerFactory.getLogger(spark.getClass)
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .option("header", "true")
      .load(file)

    df.printSchema()
    df.show(5)

    import org.apache.spark.sql.functions._
    val colToEdit = "title"
    val dfLowerCase = df.withColumn(colToEdit, lower(col(colToEdit)))

    dfLowerCase.printSchema()
    println("dfLowerCase has " + dfLowerCase.count() + " rows")
    dfLowerCase.show(5)

    //write to kafka
    dfLowerCase.select(to_json(struct("*")).alias("value"))

    dfLowerCase
      .selectExpr("to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", topic)
      .save()

    spark.close()
  }

  def sendDataFromKafkaToES(topic: String, brokers: String): Unit = {

    val schema = StructType(
      StructField("article_id", StringType) ::
        StructField("category", IntegerType) ::
        StructField("timestamp", TimestampType) ::
        StructField("namespace", IntegerType) ::
        StructField("redirect", IntegerType) ::
        StructField("title", StringType) ::
        StructField("related_page", IntegerType) :: Nil
    )

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("fromKafkaToES")
      .getOrCreate()

    LoggerFactory.getLogger(spark.getClass)
    spark.sparkContext.setLogLevel("WARN")

    val df = spark
      .read
      .format("kafka")
      .option("subscribe", topic)
      .option("kafka.bootstrap.servers", brokers)
      .load()

    val dfValue = df.selectExpr("CAST(value AS STRING)")

    import org.apache.spark.sql.functions._
    val dfWithColumns = dfValue
      .withColumn("value", from_json(col("value"), schema))

    val esConfig: Map[String, String] = Map(
      "es.mapping.id" -> "value.article_id"
    )

    //write to ES
    import org.elasticsearch.spark.sql._
    dfWithColumns.saveToEs("spark_titles", esConfig)

    spark.close()
  }

}

object FromFileToElasticsearch {

  def main(args: Array[String]): Unit = {

    val file = s"/home/dule/intellijProjects/part6/fromFileToElasticsearch" +
      "/src/main/resources/titlesTest.tsv"
    val topic = "spark-titles-topic"
    val brokers = "localhost:9092"

    val fromFileToElasticsearch = new FromFileToElasticsearch
    fromFileToElasticsearch.sendDataFromFileToKafka(file, topic, brokers)
    fromFileToElasticsearch.sendDataFromKafkaToES(topic, brokers)
  }

}
