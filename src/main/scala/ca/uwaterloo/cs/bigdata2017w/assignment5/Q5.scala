package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yanglinguan on 17/2/28.
  */
object Q5 {

  val log = Logger.getLogger(getClass().getName())


  def main(argv: Array[String]) {

    val args = new Conf(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("Q5")

    if(args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitem = sparkSession.read.parquet(args.input() + "/lineitem").rdd
      val orders = sparkSession.read.parquet(args.input() + "/orders").rdd
      val customer = sparkSession.read.parquet(args.input() + "/customer").rdd
      val nation = sparkSession.read.parquet(args.input() + "/nation").rdd

      val o = orders
        .map(row => {
          (row.getInt(0), row.getInt(1))
        })

      val c = customer
        .map(row => {
          (row.getInt(0), row.getInt(3))
        })

      val n = nation
        .map(row => {
          (row.getInt(0), row.getString(1))
        }).filter(x => x._2.toUpperCase() == "CANADA" || x._2.toUpperCase() == "UNITED STATES")

      lineitem
        .map(row => {
          (row.getInt(0), row.getString(10).dropRight(3))
        })
        .cogroup(o)
        .flatMap(x => {
          x._2._1.flatMap(y => {
            // y shipdate
            x._2._2.map(z => {
              // z custkey
              (z, y)
            })
          })
        })
        .cogroup(c)
        .flatMap(x => {
          x._2._1.flatMap(y => {
            // y: shipdate
            x._2._2.map(z => {
              // z: nationalkey
              (z, y)
            })
          })
        })
        .cogroup(n)
        .flatMap(x => {
          x._2._1.flatMap(y => {
            // y shipdate
            x._2._2.map(z => {
              // z name
              ((x._1, z, y), 1)
            })
          })
        })
        .groupByKey()
        .sortByKey()
        .collect()
        .map(x => {
          println(x._1._1, x._1._2, x._1._3, x._2.sum)
        })
    } else {
      val sc = new SparkContext(conf)

      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      val orders = sc.textFile(args.input() + "/orders.tbl")
      val customer = sc.textFile(args.input() + "/customer.tbl")
      val nation = sc.textFile(args.input() + "/nation.tbl")

      val o = orders
        .flatMap(line => {
          val t = line.split('|')
          // t(0) o_orderkey
          // t(1) o_custkey
          List((t(0).toInt, t(1).toInt))
        })

      val c = customer
        .flatMap(line => {
          val t = line.split('|')
          List((t(0).toInt, t(3).toInt))
        })

      val n = nation
        .flatMap(line => {
          val t = line.split('|')
          List((t(0).toInt, t(1)))
        })
        .filter(x => x._2 == "CANADA" || x._2 == "UNITED STATES")

      lineitem
        .flatMap(line => {
          val t = line.split('|')
          List((t(0).toInt, t(10).dropRight(3)))
        })
        .cogroup(o)
        .flatMap(x => {
          x._2._1.flatMap(y => {
            // y shipdate
            x._2._2.map(z => {
              // z custkey
              (z, y)
            })
          })
        })
        .cogroup(c)
        .flatMap(x => {
          x._2._1.flatMap(y => {
            // y: shipdate
            x._2._2.map(z => {
              // z: nationalkey
              (z, y)
            })
          })
        })
        .cogroup(n)
        .flatMap(x => {
          x._2._1.flatMap(y => {
            // y shipdate
            x._2._2.map(z => {
              // z name
              ((x._1, z, y), 1)
            })
          })
        })
        .groupByKey()
        .sortByKey()
        .collect()
        .map(x => {
          println(x._1._1, x._1._2, x._1._3, x._2.sum)
        })
    }
  }
}
