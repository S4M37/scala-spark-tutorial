package com.sparkTutorial.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, _}

object WordCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val words = lines.flatMap(line => line.split(" "))

    val wordCounts = words.countByValue()
    // for ((word, count) <- wordCounts) println(word + " : " + count)


    val eventsRddLines = sc.textFile("in/events.rdd.txt")

    eventsRddLines.map(line => {
      val parts = line.split(" ")
      Event(
        user = parts(0),
        item = parts(1),
        action = parts(2),
        t = parts(3)
      )
    }).groupBy(record => (record.user, record.item))
      .map(groupItem => {
        var score = 0
        groupItem._2.foreach(f => {
          if (f.action == "click") {
            score += 2
          } else if (f.action == "view") {
            score += 1
          }
        })
        (groupItem._1._1, groupItem._1._2, groupItem._2.map { e => e.t }.collectFirst { case e => e }.get, score)
      })
      .collect().foreach(println)
  }

}

case class Event(user: String, item: String, action: String, t: String)
