/**
  * Created by rizkaz on 3/30/16.
  */

import org.apache.spark.{SparkConf, SparkContext}

object SparkApp {
  def main(args: Array[String]) {

    //any file on system
    if (args.length > 0) {
      val fileName = args(0)
      val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
      val sc = new SparkContext(conf)
      val logData = sc.textFile(fileName)

      //parse lines into words
      val words = logData.flatMap[String](line => line.split(" "))

      //filter based on size
      val wordsGT3 = words.filter(word => word.toString.length > 3).count()
      println("Number of words of length greater than 3 in file = " + wordsGT3)

      sc.stop()
    }

  }
}

