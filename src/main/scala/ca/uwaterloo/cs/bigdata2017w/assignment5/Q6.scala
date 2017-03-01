package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yanglinguan on 17/2/27.
  */
object Q6 {

  val log = Logger.getLogger(getClass.getName)


  def main(argv: Array[String]) {

    val args = new Conf(argv)

    val comp = new Comp()

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)
    val d = args.date().toString()

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")

    lineitem
      .flatMap(line => {
        val t = line.split('|')
        // t(4) quantity ._1
        // t(5) extendedprice ._2
        // t(6) discount ._3
        // t(7) tax ._4
        // t(10) shipdate ._5
        List(((t(8), t(9)), (t(4).toInt, t(5).toFloat, t(6).toFloat, t(7).toFloat, t(10))))
      })
      .filter(x => {
        comp.compare(x._2._5, d)
      })
      .groupByKey()
      .collect()
      .map(x => {
        val s = x._2.map(t => {
          (1, t._1, t._2, t._2 *(1-t._3), t._2 * (1 - t._3) * (1 + t._4), t._3)
        }).reduce((y1, y2) => {
          (y1._1 + y2._1, y1._2 + y2._2, y1._3 + y2._3, y1._4 + y2._4, y1._5 + y2._5, y1._6 + y2._6)
        })

        val avg_qty = s._2.toFloat / s._1
        val avg_price = s._3.toFloat / s._1
        val avg_disc = s._6.toFloat / s._1
        println(x._1._1, x._1._2 , s._2 , s._3, s._4, s._5, avg_qty, avg_price, avg_disc, s._1 )
      })
  }
}
