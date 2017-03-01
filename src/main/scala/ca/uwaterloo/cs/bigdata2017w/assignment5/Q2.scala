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

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl");
    val orders = sc.textFile(args.input() + "/orders.tbl");
    val d = args.date().toString()

    val o = orders
      .flatMap(line => {
        val t = line.split('|')
        //t(0) o_orderkey
        //t(6) o_clerk
        List((t(0), t(6)))
      })

    lineitem
      .flatMap(line => {
        val t = line.split('|')
        // t(0) l_orderkey
        // t(10) l_shipdate
        List((t(0), t(10)))
      })
      .filter(x => comp.compare(x._2, d))
      .cogroup(o)
      .flatMap(item => {
        item._2._1.flatMap(t => {
          item._2._2.map(y => {
            (y, item._1)
          })
        })
      })
      .sortBy(x => x._2)
      .take(20)
      .map(x => {
        println(x._1, x._2)
      })
  }
}
