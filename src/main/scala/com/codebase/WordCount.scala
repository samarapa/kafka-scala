package com.codebase

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by samarapa on 2/1/2017.
  */
object SparkWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("First Program")
    val sc = new SparkContext(conf);
    //    val inputFile = args(0)
    val inputFile = "C:/Senthil/Arul.txt"
    val outputFile = "C:/Senthil/Arul_new.txt"
    //    val outputFile = args(1)
    val input = sc.textFile(inputFile)
    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into pairs and count.
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile)

  }

}
