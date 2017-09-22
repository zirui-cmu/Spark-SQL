/**
  * Created by ziruizhu on 4/19/17.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

object FollowerRDD {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("followerRDD")
    val sc = new SparkContext(conf)
    val input = sc.textFile("s3://cmucc-datasets/p42/Graph")
    val followee = input.distinct.map(line => line.split("\t")(1))
    val result = followee.map(f => (f,1)).reduceByKey(_+_)
      .sortBy(f => f._2, false).take(100).map(tuples => s"${tuples._1}\t${tuples._2}")
    sc.makeRDD(result).saveAsTextFile("hdfs:///followerRDD-output")
  }
}
