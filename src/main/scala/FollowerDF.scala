/**
  * Created by ziruizhu on 4/19/17.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object FollowerDF {
  def main(args: Array[String]){
    val spark = SparkSession
      .builder()
      .appName("followerDF")
      .getOrCreate()

    //get input RDD
    val sc = spark.sparkContext
    val input = sc.textFile("s3://cmucc-datasets/p42/Graph")
    val inputRDD = input.map(line => (line.split("\t")(0), line.split("\t")(1))).distinct
    //create data frame schema
    val schemaString = "follower followee"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = inputRDD
      .map(attributes => Row(attributes._1, attributes._2.trim))
    // Apply the schema to the RDD
    val followerDF = spark.createDataFrame(rowRDD, schema)
    followerDF.createOrReplaceTempView("follower")
    val results = spark.sql("SELECT followee, COUNT(*) as count FROM follower GROUP BY followee ORDER BY count(*) DESC LIMIT 100")
    results.write.format("parquet").save("hdfs:///followerDF-output")
  }

}


