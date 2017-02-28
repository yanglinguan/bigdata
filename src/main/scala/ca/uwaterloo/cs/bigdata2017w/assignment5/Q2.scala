package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yanglinguan on 17/2/17.
  */

object Q2 {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val args = new Conf(argv)

    val comp = new Comp()

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl");
    val orders = sc.textFile(args.input() + "/orders.tbl");
    val d = args.date().toString()

    val l = lineitem
      .flatMap(line => {
        val t = line.split('|')
        List((t(10), t(0)))
      })
      .filter(x => comp.compare(x._1, d))
      .map(x => (x._2, "l"))

    val o = orders
      .flatMap(line => {
        val t = line.split('|')
        List((t(0), t(6)))
      })

    val x = l.cogroup(o)
      .map(item => {
        item._2._2.map(t=> {
          comp.print(List(t, item._1))
//          println("(" + t + "," + item._1 + ")")
        })
      }).count()
//    println("hello: " + x.count())
  }
}
