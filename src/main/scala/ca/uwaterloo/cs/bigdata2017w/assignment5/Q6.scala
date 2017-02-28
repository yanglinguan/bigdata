package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yanglinguan on 17/2/27.
  */
object Q6 {

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

    val l = lineitem
      .flatMap(line => {
        val t = line.split('|')
        List(((t(8), t(9)), (t(4), t(5), t(6), t(7), t(10))))
      })
      .filter(x => {
        comp.compare(x._2._5, d)
      })
      .groupByKey()
      .map(x => {
        val c = x._2.count(x => true)
        val s = x._2.reduce((y1, y2) => {
          val sum_qty = (y1._1.toInt + y2._1.toInt).toString
          val sum_base_price = (y1._2.toFloat + y2._2.toFloat).toString
          val sum_disc_price = ((y1._2.toFloat * (1 - y1._3.toFloat)) + (y2._2.toFloat * (1 - y2._3.toFloat))).toString
          val sum_charge = ((y1._2.toFloat * (1 - y1._3.toFloat) * (1 + y1._4.toFloat))
            + y2._2.toFloat * (1 - y2._3.toFloat) * (1 + y2._4.toFloat)).toString
          val sum_discount = (y1._3.toFloat + y2._3.toFloat).toString

          (sum_qty , sum_base_price, sum_disc_price, sum_charge,
            sum_discount)

        })
        val avg_qty = (s._1.toFloat / c.toFloat).toString
        val avg_price = (s._2.toFloat / c.toInt).toString
        val avg_disc = (s._5.toFloat / c.toInt).toString
        println("(" + x._1._1 + "," + x._1._2 + "," + s._1 , s._2, s._3, s._4,
          s._5, avg_qty, avg_price, avg_disc + ")")
        ""

      }).count()


  }

}
