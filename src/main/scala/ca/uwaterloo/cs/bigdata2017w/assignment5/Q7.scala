package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by yanglinguan on 17/2/28.
  */


object Q7 {
  val log = Logger.getLogger(getClass.getName)

  def compareDate(d: String, gd: String, smaller: Boolean): Boolean = {
    val t = d.split('-').map(x => x.toInt)
    val dt = gd.split('-').map(x => x.toInt)
    if(t(0) < dt(0)) {
      smaller
    } else if(t(0) == dt(0)) {
      if(t(1) < dt(1)) {
        smaller
      } else if(t(1) == dt(1)) {
        if(t(2) < dt(2)) {
          smaller
        } else if(t(2) == dt(2)) {
          false
        } else {
          !smaller
        }
      } else {
        !smaller
      }
    } else {
      !smaller
    }
  }


  def main(argv: Array[String]) {

    val args = new Conf(argv)

    val d = args.date().toString()

    val conf = new SparkConf().setAppName("Q7")

    if(args.parquet()) {

      // spark sql
//      val sparkSession = SparkSession.builder.getOrCreate
//      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
//      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
//      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
//
//      lineitemDF.createOrReplaceTempView("lineitem")
//      ordersDF.createOrReplaceTempView("orders")
//      customerDF.createOrReplaceTempView("customer")
//
//      val namesDF = sparkSession.sql("select\n  c_name,\n  l_orderkey,\n  sum(l_extendedprice*(1-l_discount)) as revenue,\n  " +
//        "o_orderdate,\n  o_shippriority\nfrom customer, orders, lineitem\n" +
//        "where\n  c_custkey = o_custkey and\n  l_orderkey = o_orderkey and\n  o_orderdate < \"1996-01-01\" and\n  l_shipdate > \"1996-01-01\"\n" +
//        "group by\n  c_name,\n  l_orderkey,\n  o_orderdate,\n  o_shippriority\norder by\n  revenue desc")
//
//     // println("size: " + namesDF.count() )
//      namesDF.rdd.map(row => {
//        // x._1._1 c_name
//        //          // x._1._2 orderkey
//        //          // x._1._3 o_orderdate
//        //          // x._1._4 o_shippriority
//        //          // x._2 revenue
//        //          println(x._1._1, x._1._2, x._2, x._1._3, x._1._4)
//        println(row.getString(0), row.getInt(1), row.getDouble(2), row.getString(3), row.getInt(4))
//
//      }).collect()



      val sparkSession = SparkSession.builder.getOrCreate

      val lineitem = sparkSession.read.parquet(args.input() + "/lineitem").rdd
      val orders = sparkSession.read.parquet(args.input() + "/orders").rdd
      val customer = sparkSession.read.parquet(args.input() + "/customer").rdd

      val o = orders
        .map(row => {
          // t(0) o_orderkey
          // t(1) o_custkey
          // t(4) o_orderdate
          // t(7) o_shippriority
          (row.getInt(0), (row.getInt(1), row.getString(4), row.getInt(7)))
        })
        .filter(x => compareDate(x._2._2, d, true))

      val c = customer
        .map(row => {
          // t(0) c_custkey
          // t(1) c_name
          (row.getInt(0), row.getString(1))
        })

      lineitem
        .map(row => {
          // t(0) l_orderkey
          // t(5) extendedprice
          // t(6) discount
          // t(10) shipdate
          (row.getInt(0), (row.getDouble(5), row.getDouble(6), row.getString(10)))
        })
        .filter(x => compareDate(x._2._3, d, false))
        .cogroup(o)
        .flatMap(x => {
          // key: orderkey
          x._2._2.flatMap(y => {
            //y._1: o_custkey
            //y._2: o_orderdate
            //y._3: o_shippriority
            x._2._1.map(z => {
              // z._1: l_extendedprice
              // z._2: l_discount
              (y._1, (x._1, y._2, y._3, z._1, z._2))
            })
          })
        })
        .cogroup(c)
        .flatMap(x => {
          // key: custkey
          x._2._1.flatMap(y => {
            //y._1: orderkey
            //y._2: o_orderdate
            //y._3: o_shippriority
            //y._4: l_extendedprice
            //y._5: l_discount
            x._2._2.map(z => {
              //z c_name
              ((z, y._1, y._2, y._3), (y._4, y._5))
            })
          })
        })
        .groupByKey()
        .map(x => {
          val revenue = x._2.map(y => {
            y._1 * (1 - y._2)
          }).sum
          (x._1, revenue)
        })
        .sortBy(x => {
          x._2
        }, false)
        .collect()
        .take(10)
        .map(x => {
          // x._1._1 c_name
          // x._1._2 orderkey
          // x._1._3 o_orderdate
          // x._1._4 o_shippriority
          // x._2 revenue
          println(x._1._1, x._1._2, x._2, x._1._3, x._1._4)
        })
    } else {

      val sc = new SparkContext(conf)


      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      val orders = sc.textFile(args.input() + "/orders.tbl")
      val customer = sc.textFile(args.input() + "/customer.tbl")

      val o = orders
        .flatMap(line => {
          val t = line.split('|')
          // t(0) o_orderkey
          // t(1) o_custkey
          // t(4) o_orderdate
          // t(7) o_shippriority
          List((t(0).toInt, (t(1).toInt, t(4), t(7).toInt)))
        })
        .filter(x => compareDate(x._2._2, d, true))

      val c = customer
        .flatMap(line => {
          val t = line.split('|')
          // t(0) c_custkey
          // t(1) c_name
          List((t(0).toInt, t(1)))
        }).collectAsMap()

      val bcC = sc.broadcast(c)

      lineitem
        .flatMap(line => {
          val t = line.split('|')
          // t(0) l_orderkey
          // t(5) extendedprice
          // t(6) discount
          // t(10) shipdate
          List((t(0).toInt, (t(5).toDouble, t(6).toDouble, t(10))))
        })
        .filter(x => compareDate(x._2._3, d, false))
        .cogroup(o)
        .flatMap(x => {
          // key: orderkey
          x._2._2.flatMap(y => {
            //y._1: o_custkey
            //y._2: o_orderdate
            //y._3: o_shippriority
            x._2._1.map(z => {
              // z._1: l_extendedprice
              // z._2: l_discount
              val name = bcC.value.getOrElse(y._1, "")
              ((name, x._1, y._2, y._3), z._1 * (1 - z._2))
            })
          })
        })
        .reduceByKey(_+_)
        .sortBy(x => {
          x._2
        }, false)
        .collect()
        .take(10)
        .map(x => {
          // x._1._1 c_name
          // x._1._2 orderkey
          // x._1._3 o_orderdate
          // x._1._4 o_shippriority
          // x._2 revenue
          println(x._1._1, x._1._2, x._2, x._1._3, x._1._4)
        })
    }
  }
}
