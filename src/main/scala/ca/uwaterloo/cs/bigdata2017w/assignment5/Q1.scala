package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scala.collection.mutable.StringBuilder
//import org.apache.spark.sql.SparkSession


/**
  * Created by yanglinguan on 17/2/17.
  */

class Conf(args: Seq[String]) extends ScallopConf(args) {
//  mainOptions = Seq(input, date, text, parquet)
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "selection date", required = true)
  val text = opt[Boolean](descr = "text")
  val parquet = opt[Boolean](descr = "parquet")
  verify()
}

class Comp() extends Serializable {
  def compare(date: String, shipDate: String): Boolean = {
    val d = date.split('-')
    val sd = shipDate.split('-')
    val size = sd.length
    var i = -1
    val t = sd.count(x => {
      i = i + 1
      x == d(i)
    })
    size == t
  }
}


object Q1 {

  val log = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]) {

    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args.input() + "/lineitem.tbl");

    val d = args.date().toString
    val comp = new Comp()

    val query = textFile
      .flatMap(line => {
        val t = line.split('|')
        List(t(10))
      })
      .filter(x => comp.compare(x, d))
      .count()

    println("ANSWER=" + query)
  }
}