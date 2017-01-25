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
        var strips:mutable.HashMap[String, mutable.HashMap[String, Float]] = new mutable.HashMap[String, mutable.HashMap[String, Float]]()
        if (tokens.length > 1) {
          tokens.sliding(2).foreach(p => {
            val prev = p(0)
            val cur = p(1)
            if(strips.contains(prev)) {
              val strip:mutable.HashMap[String, Float] = strips(prev)
              if(strip.contains(cur)) {
                strips(prev)(cur) += 1.0f
              } else {
                strips(prev) += cur -> 1.0f
              }
            } else {
              var strip:mutable.HashMap[String, Float] = new mutable.HashMap[String, Float]()
              strip += cur -> 1.0f
              strips += prev -> strip
            }
          })
          strips.toList
        } else {
          List()
        }
      })
      .partitionBy(new HashPartitioner(args.reducers()))
      .persist()
      .reduceByKey((x, y) => {
        var comMap:mutable.HashMap[String, Float] = new mutable.HashMap[String, Float]()
        x.foreach(t => {
          comMap += t._1 -> t._2
        })
        y.foreach(t => {
          if(comMap.contains(t._1)) {
            comMap(t._1) += t._2
          } else {
            comMap += t._1 -> t._2
          }
        })
         comMap
      })
      .sortByKey()
      .map(x =>{
        var sum = 0.0f
        x._2.foreach(t => {
          sum += t._2
        })

        var comMap:mutable.HashMap[String, Float] = new mutable.HashMap[String, Float]()
        x._2.foreach(t => {
          //x._2(t._1) = t._2/sum
          comMap += t._1 -> t._2/sum
        })
        (x._1, comMap.toList)
      }).map(x => {
      x._1 + " {" + x._2.map(p => {
        p._1 + "=" + p._2
      }).toString().drop(5).dropRight(1) + "}"
    })
    counts.saveAsTextFile(args.output())
  }
}
