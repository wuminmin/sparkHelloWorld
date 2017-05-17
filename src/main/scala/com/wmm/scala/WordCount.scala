package com.wmm.scala

/**
  * Created by wuminmin on 2017/5/16.
  */

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("spark hello world");
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext("local","wordcount",conf)
    val line = sc.textFile(args(0))

    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)

    sc.stop()
  }

}
