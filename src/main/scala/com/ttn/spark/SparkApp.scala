package com.ttn.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("My First App")
    val sc = new SparkContext(conf)

    val myFile: RDD[String] = sc.textFile("/home/umesh/spark/emp.txt")

    val empRecords = myFile.map(_.split(","))

    // Find the Average salary in for Every Designation
    val avgOfSalaryForEachDesignation = empRecords
      .map(rec => (rec(3), rec(2).toLong))
      .mapValues((_, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(value => (1.0 * value._1) / value._2)
      .collectAsMap()
    println(avgOfSalaryForEachDesignation)

  }
}
