package org.spark.training.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.spark.training.checkpoint.CheckpointApp.session
import org.spark.training.pairrdd.Person

object CacheApp extends App {

  val session = SparkSession
    .builder
    .master("local[*]")
    .appName("Pair RDD")
    .getOrCreate()
  session.sparkContext.setLogLevel("ERROR")

  private val rdd1: RDD[Person] = session.sparkContext.parallelize(Array(
    Person("a", 1, "type_1"),
    Person("b", 2, "type_2"),
    Person("c", 3, "type_1"),
    Person("d", 4, "type_1"),
    Person("e", 5, "type_2"),
    Person("f", 6, "type_1")
  ))

  println(rdd1.count)
  println(rdd1.toDebugString)

  private val rdd2: RDD[Person] = rdd1
    .map(p => {
      p.id = p.id * 2
      println(s"  transformed to ${p.id * 2}")
      p
    })

  rdd2.cache
  println("rdd2:\n" + rdd2.toDebugString)
  println("rdd2 counting...")
  println("rdd2 count: " + rdd2.count)
  println("rdd2:\n" + rdd2.toDebugString)

  private val rdd3: RDD[Person] = rdd2
    .map(p => {
      p.id = p.id * 2
      println(s"  transformed to ${p.id + 1}")
      p
    })

  println("rdd3:\n" + rdd3.toDebugString)
  println("rdd3 counting...")
  println("rdd3 count: " + rdd3.count)

}
