package org.spark.training.partition

import org.apache.spark.Partitioner

case class MyPartitionKey(number: Int)

class MyPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    val k: MyPartitionKey = key.asInstanceOf[MyPartitionKey]
    val partition = k.number % numPartitions
    //println(s"key ${k.number} partition $partition")
    partition
  }
}
