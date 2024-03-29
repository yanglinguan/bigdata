package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by yanglinguan on 17/1/23.
  */


object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyStripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val strips:mutable.Map[String, mutable.Map[String, Float]] = mutable.Map[String, mutable.Map[String, Float]]()
          tokens.sliding(2).foreach(p => {
            val prev = p(0)
            val cur = p(1)
            val strip = strips.get(prev)
            if(strip.isEmpty) {
              val strip:mutable.Map[String, Float] = mutable.Map[String, Float]()
              strip += cur -> 1.0f
              strips += prev -> strip
            } else {
              val tt = strip.get.get(cur)
              if(tt.isEmpty) {
                strip.get += cur -> 1.0f
              } else {
                strip.get(cur) += 1.0f
              }
            }
          })
          strips.toList
        } else {
          List()
        }
      })

      .partitionBy(new HashPartitioner(args.reducers()))
      //.persist()
      //.sortByKey()
      .reduceByKey((x, y) => {
        x ++ y.map{ case (k,v) => k -> (v + x.getOrElse(k, 0f))}
      })
      .map(x =>{
        var sum = 0.0f
        x._2.foreach(t => {
          sum += t._2
        })

        (x._1, x._2.mapValues(t => t/sum).toList)

      }).map(x => {
      val s = new StringBuilder(x._1)
      s.append(" {")
      x._2.foreach(p=> {
        s.append(p._1).append("=").append(p._2).append(", ")
      })
      s.dropRight(2).append("}").toString()
//      x._1 + " {" + x._2.map(p => {
//        p._1 + "=" + p._2
//      }).toString().drop(5).dropRight(1) + "}"
    })
    counts.saveAsTextFile(args.output())
  }
}
