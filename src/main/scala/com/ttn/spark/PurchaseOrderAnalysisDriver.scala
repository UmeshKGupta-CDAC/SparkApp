package com.ttn.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object PurchaseOrderAnalysisDriver {
  def main(args: Array[String]): Unit = {
    // Setup configuration and create spark context
    val conf = new SparkConf().setAppName("PO analysis").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Load the source data file into RDD
    val poDetailsFile: RDD[String] = sc.textFile("src/main/resources/purchase_orders.txt")
    println(poDetailsFile.foreach(println))

    //Create key value pairs
    val kv = poDetailsFile.map(_.split(",")).map(v => (v(0), v(1)))

    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    println(uniqueByKey.collectAsMap())
  }

}