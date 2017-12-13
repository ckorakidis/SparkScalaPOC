package com.mysoft.spark

import org.apache.log4j._
import org.apache.spark._

object SpentByPerson {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "SpentByPerson")
    
    // Load each line of my book into an RDD
    val input = sc.textFile("/Users/ck186031/workspace/SparkScalaPOC/src/main/resources/customer-orders.csv")
    
    // Split using a regular expression that extracts words
    val values = input.map(x => x.split(","))

    val spent = values.map(x => (x(0).toInt, x(2).toFloat))

    val spentByPerson = spent.reduceByKey((x, y) => x + y)

    val sorted = spentByPerson.map(x => (x._2, x._1)).sortByKey().map(x => (x._2, x._1))

    //can work without that, wrong local config for spark
//    val collected = sorted.collect()

    for (result <- sorted) {
      val id = result._1
      val amount = result._2
      println(s"$id: $amount")
    }
    
  }
  
}

