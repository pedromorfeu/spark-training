package org.spark.training.pairrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class Person(name: String, var id: Int, var typeOfPerson: String)

object PairRDDMain extends App {

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

  private val rdd2: RDD[Person] = rdd1.map(p => {
    p.id = p.id * 2
    println(s"  transformed to ${p.id * 2}")
    p
  })
  println(rdd2.count)
  println(rdd2.toDebugString)
  println(rdd2.count)

  private val type1RDD: RDD[Person] = rdd2.filter(_.typeOfPerson == "type_1")
  private val type2RDD: RDD[Person] = rdd2.filter(_.typeOfPerson == "type_2")

  println("counting type RDDs...")
  println(type1RDD.toDebugString)
  println(type1RDD.count)
  println(type2RDD.toDebugString)
  println(type2RDD.count)

}
