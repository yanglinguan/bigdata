package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by yanglinguan on 17/1/25.
  */
object StripesPMI extends Tokenizer{

  val log = Logger.getLogger(getClass().getName())

  implicit def mapAccum = new AccumulableParam[mutable.Map[String, Float], (String, Float)] {
    override def addInPlace(t1: mutable.Map[String, Float], t2: mutable.Map[String, Float]): mutable.Map[String, Float] = {
      t2.foreach(x => {
        if(t1.contains(x._1)) {
          t1(x._1) += x._2
        } else {
          t1 += x._1 -> x._2
        }
      })
      t1
    }

    override def addAccumulator(t1: mutable.Map[String, Float], t2: (String, Float)): mutable.Map[String, Float] = {
      if(t1.contains(t2._1)) {
        t1(t2._1) += t2._2
      } else {
        t1 += t2._1 -> t2._2
      }
      t1
    }

    override def zero(t: mutable.Map[String, Float]): mutable.Map[String, Float] = mutable.Map[String, Float]()
  }
  def main(argv: Array[String]) {
    val args = new PMIConf(argv)
    val threshold = args.threshold()

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("PairsPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())


    val words: Accumulable[mutable.Map[String, Float], (String, Float)] = sc.accumulable(mutable.Map[String, Float]())

    var lines = sc.accumulator(0f)

    val co = textFile.flatMap(line => {
      lines += 1f
      val tokens = tokenize(line)
      var countWords:scala.collection.mutable.HashSet[String] = scala.collection.mutable.HashSet[String]()
      val size = Math.min(39, tokens.size - 1)

      for( i <- 0 to size ) {
        countWords += tokens(i)
      }
//      tokens.foreach(x => {
//        countWords += x
//      })

      countWords.foreach(x => {
        words += (x, 1.0f)
      })

      var strips:mutable.Map[String, mutable.Map[String, Float]] = mutable.Map()
      countWords.foreach(x => {
        countWords.foreach(y => {
          if(x != y) {
            if(strips.contains(x)) {
              val strip:mutable.Map[String, Float] = strips(x)
              if(strip.contains(y)) {
                strips(x)(y) += 1.0f
              } else {
                strips(x) += y -> 1.0f
              }
            } else {
              var strip:scala.collection.mutable.Map[String, Float] = scala.collection.mutable.Map()
              strip += y -> 1.0f
              strips += x -> strip
            }
          }
        })
      })
      strips.toList
    })
      .partitionBy(new HashPartitioner(args.reducers()))
      .persist()
      .reduceByKey((x, y) => {
        var comMap:mutable.Map[String, Float] = mutable.Map()
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

    val bWords = sc.broadcast(words.value)
    val bLineNo = sc.broadcast(lines.value)

    //    log.warn("size: " + bWords.value.size)
    //    log.warn("line: " + lines.value)

    val counts = co.map(x => {
      var result = mutable.Map[String, (Float, Float)]()

      x._2.filter(t => t._2 >= threshold)
        .foreach(t => {
          val pmi = Math.log10((t._2 * bLineNo.value)/ (bWords.value(t._1) * bWords.value(x._1)))
          result += t._1 -> (pmi.toFloat, t._2)

        })
      (x._1, result.toList)
    }).map(x => {
      x._1 + " {" + x._2.map(p => {
        p._1 + "=" + p._2
      }).drop(5).dropRight(1) + "}"
    })
    counts.saveAsTextFile(args.output())

  }
}
