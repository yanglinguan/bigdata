package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yanglinguan on 17/2/27.
  */

object Q4 {

  val log = Logger.getLogger(getClass().getName())


  def main(argv: Array[String]) {

    val args = new Conf(argv)

    val comp = new Comp()

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)
    val d = args.date().toString()

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
    val orders = sc.textFile(args.input() + "/orders.tbl")
    val customer = sc.textFile(args.input() + "/customer.tbl")
    val nation = sc.textFile(args.input() + "/nation.tbl")

    val l = lineitem
      .flatMap(line => {
        val t = line.split('|')
        List((t(0), t(10)))
      })
      .filter(x => comp.compare(x._2, d))

    val o = orders
      .flatMap(line => {
        val t = line.split('|')
        List((t(0), t(1)))
      })

    val c = customer
      .flatMap(line => {
        val t = line.split('|')
        List((t(0), t(3)))
      })

    val n = nation
      .flatMap(line => {
        val t = line.split('|')
        List((t(0), t(1)))
      })

    val co = l.cogroup(o)
      .flatMap(x => {
        x._2._1.flatMap(y => {
          x._2._2.map(z => {
            (z, 1)
          })
        })
      })
      .cogroup(c)
      .flatMap(x => {
        x._2._1.flatMap(y => {
          x._2._2.map(z => {
            (z, 1)
          })
        })
      })
      .cogroup(n)
      .flatMap(x => {
        x._2._1.flatMap(y => {
          x._2._2.map(z => {
            ((x._1, z), 1)
          })
        })
      })
      .groupByKey()
      .sortBy(x => {
        x._1._1.toInt
      })
      .map(x => {
        val co = x._2.reduce(_+_)
        println("(" + x._1._1 + "," + x._1._2 + "," + co.toString() + ")")
      }).count()
  }

}
