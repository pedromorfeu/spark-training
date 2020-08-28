package org.spark.training

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.spark.training.partition.{MyPartitionKey, MyPartitioner}

object Main extends App {

  val session = SparkSession
    .builder
    .master("local[*]")
    .appName("Simple Application")
    .getOrCreate()
  session.sparkContext.setLogLevel("ERROR")

  val data = Array((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"))
  val data1 = Array((6, "f"), (7, "g"), (1, "j"))
  val rdd: RDD[(Int, String)] = session.sparkContext.parallelize(data)
  val rdd1: RDD[(Int, String)] = session.sparkContext.parallelize(data1, 4)
  println(s"rdd partitions: ${rdd.partitions.size}")
  println(s"rdd1 partitions: ${rdd1.partitions.size}")

  private val unionRDD1: RDD[(Int, String)] = rdd.union(rdd1)
  println(s"union partitions: ${unionRDD1.partitions.size}")
  unionRDD1
    .mapPartitionsWithIndex((index: Int, it: Iterator[(Int, String)]) => {
      val list = it.toList
      println(s"part-$index: ${list.mkString(",")}")
      list.iterator
    })
    .count()

  val myPartitioner = new MyPartitioner(4)
  val partRDD: RDD[(MyPartitionKey, (Int, String))] = rdd
    .map(t => (MyPartitionKey(t._1), t))
    .partitionBy(myPartitioner)
  println(s"partRDD partitioner: ${partRDD.partitioner}")
  println(s"partRDD partitions: ${partRDD.partitions.size}")

  val partRDD1: RDD[(MyPartitionKey, (Int, String))] = rdd1
    .map(t => (MyPartitionKey(t._1), t))
    .partitionBy(myPartitioner)
  println(s"partRDD1 partitioner: ${partRDD1.partitioner}")
  println(s"partRDD1 partitions: ${partRDD1.partitions.size}")

  private val partUnionRDD: RDD[(MyPartitionKey, (Int, String))] = partRDD.union(partRDD1)
  println(s"union partitions: ${partUnionRDD.partitions.size}")

  partUnionRDD
    .mapPartitionsWithIndex((index: Int, it: Iterator[(MyPartitionKey, (Int, String))]) => {
      val list = it.toList
      println(s"part-$index: ${list.map(_._2).mkString(",")}")
      list.iterator
    })
    .count()

  //import session.implicits._
  //val df: DataFrame = Seq(1, 2, 3, 4, 5).toDF
  //df.show

  session.stop()

}
