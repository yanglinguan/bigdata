package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark._

import scala.collection.mutable
/**
  * Created by yanglinguan on 17/1/25.
  */
object StripesPMI extends Tokenizer{

  val log = Logger.getLogger(getClass().getName())

  implicit def mapAccum = new AccumulableParam[mutable.Map[String, Float], (String, Float)] {
    override def addInPlace(t1: mutable.Map[String, Float], t2: mutable.Map[String, Float]): mutable.Map[String, Float] = {
      t1 ++ t2.map{ case (k,v) => k -> (v + t1.getOrElse(k, 0f))}
    }

    override def addAccumulator(t1: mutable.Map[String, Float], t2: (String, Float)): mutable.Map[String, Float] = {
      val x = t1.getOrElse(t2._1, 0)
      if(x == 0) {
        t1 += t2._1 -> t2._2
      } else {
        t1(t2._1) += t2._2
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
      var countWords:mutable.HashSet[String] = new mutable.HashSet[String]()
      val size = Math.min(39, tokens.size - 1)

      for( i <- 0 to size ) {
        countWords += tokens(i)
      }

      countWords.foreach(x => {
        words += (x, 1.0f)
      })

      var strips:mutable.Map[String, mutable.Map[String, Float]] = mutable.Map[String, mutable.Map[String, Float]]()
      countWords.foreach(x => {
        countWords.foreach(y => {
          if(x != y) {
            val strip = strips.get(x)
            if(strip.isEmpty) {
              val strip:mutable.Map[String, Float] = mutable.Map[String, Float]()
              strip += y -> 1.0f
              strips += x -> strip
            } else {
              val tt = strip.get.get(y)
              if(tt.isEmpty) {
                strips(x) += y -> 1.0f
              } else {
                strips(x)(y) += 1.0f
              }
            }
          }
        })
      })
      strips.toList
    })
      .partitionBy(new HashPartitioner(args.reducers()))
      .sortByKey()
      .reduceByKey((x, y) => {
        x ++ y.map{ case (k,v) => k -> (v + x.getOrElse(k, 0f))}

      })


    val bWords = sc.broadcast(words.value)
    val bLineNo = sc.broadcast(lines.value)

    val counts = co.map(x => {

//      val result = new mutable.HashMap[String, (Float, Float)]()

      (x._1, x._2.filter(t => t._2 >= threshold)
        .map(t => {
          val pmi = Math.log10((t._2 * bLineNo.value)/ (bWords.value(t._1) * bWords.value(x._1)))
          (pmi.toFloat, t._2)
        }).toList)
//      (x._1, result.toList)
    }).filter(x => {
      x._2.size > 0
    })
      .map(x => {
        val s = new StringBuilder(x._1)
        s.append(" {")
        x._2.foreach(p=> {
          s.append(p._1).append("=").append(p._2).append(", ")
        })
        s.dropRight(2).append("}").toString()
    })
    counts.saveAsTextFile(args.output())

  }
}
