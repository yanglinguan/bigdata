package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yanglinguan on 17/2/27.
  */
object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val args = new Conf(argv)

    val comp = new Comp()

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q3")
    val d = args.date().toString()

    if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitem = sparkSession.read.parquet(args.input() + "/lineitem").rdd
      val part = sparkSession.read.parquet(args.input() + "/part").rdd
      val supplier = sparkSession.read.parquet(args.input() + "/supplier").rdd

      val p = part
        .map(row => {
          (row.getInt(0), row.getString(1))
        })

      val s = supplier
        .map(row => {
          (row.getInt(0), row.getString(1))
        })

      lineitem
        .map(row => {
          // 0: orderkey
          // 1: partkey
          // 2: supplierkey
          // 10: shipdate
          (row.getInt(1), (row.getInt(2), row.getInt(0), row.getString(10)))
        })
        .filter(x => comp.compare(x._2._3, d))
        .cogroup(p)
        .flatMap(x => {
          x._2._1.flatMap(y => {
            // y._1 l_supplierkey
            // y._2 l_orderkey
            x._2._2.map(z => {
              // z p_name
              (y._1, (y._2, z))
            })
          })
        })
        .cogroup(s)
        .flatMap(x => {
          // x._1 l_supplierkey
          x._2._1.flatMap(y => {
            // y._1 l_orderkey
            // y._2 p_name
            x._2._2.map(z => {
              // z s_name
              (y._1, (y._2, z))
            })
          })
        })
        .sortByKey()
        .collect()
        .take(20)
        .map(x => {
          println(x._1, x._2._1, x._2._2)
        })
    } else {
      val sc = new SparkContext(conf)


      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      val part = sc.textFile(args.input() + "/part.tbl")
      val supplier = sc.textFile(args.input() + "/supplier.tbl")

      val p = part
        .flatMap(line => {
          val t = line.split('|')
          List((t(0).toInt, t(1)))
        })

      val s = supplier
        .flatMap(line => {
          val t = line.split('|')
          List((t(0).toInt, t(1)))
        })

      lineitem
        .flatMap(line => {
          val t = line.split('|')
          // 0: orderkey
          // 1: partkey
          // 2: supplierkey
          // 10: shipdate
          List((t(1).toInt, (t(2).toInt, t(0).toInt, t(10))))
        })
        .filter(x => comp.compare(x._2._3, d))
        .cogroup(p)
        .flatMap(x => {
          x._2._1.flatMap(y => {
            // y._1 l_supplierkey
            // y._2 l_orderkey
            x._2._2.map(z => {
              // z p_name
              (y._1, (y._2, z))
            })
          })
        })
        .cogroup(s)
        .flatMap(x => {
          // x._1 l_supplierkey
          x._2._1.flatMap(y => {
            // y._1 l_orderkey
            // y._2 p_name
            x._2._2.map(z => {
              // z s_name
              (y._1, (y._2, z))
            })
          })
        })
        .sortByKey()
        .collect()
        .take(20)
        .map(x => {
          println(x._1, x._2._1, x._2._2)
        })
    }
  }
}
