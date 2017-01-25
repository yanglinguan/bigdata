package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark._
import org.rogach.scallop.ScallopConf
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by yanglinguan on 17/1/24.
  */

class PMIConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  verify()
}

class PMIPartitioner(partitions: Int) extends Partitioner {

  override def numPartitions: Int = partitions
  override def getPartition(key: Any): Int =
  {
    val k = key.asInstanceOf[(String, String)];
    (k._1.hashCode() & Integer.MAX_VALUE) % partitions;
  }

}

object PairsPMI extends Tokenizer {
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

      val size = Math.min(39, tokens.size)

      for( i <- 0 to size ) {
        countWords += tokens(i)
      }
      //      tokens.foreach(x => {
//        countWords += x
//      })

      countWords.foreach(x => {
        words += (x, 1.0f)
      })

      var lword = new ListBuffer[(String, String)]()
      countWords.foreach(x => {
        countWords.foreach(y => {
          if(x != y) {
            val p = (x, y)
            lword += p
          }
        })
      })

      lword.toList
    })
      .map(x => (x, 1f))
      .partitionBy(new PMIPartitioner(args.reducers()))
      .persist()
      .reduceByKey(_+_)
      .filter(x => x._2 >= threshold)
      .sortByKey()

    val bWords = sc.broadcast(words.value)
    val bLineNo = sc.broadcast(lines.value)

//    log.warn("size: " + bWords.value.size)
//    log.warn("line: " + lines.value)

    val counts = co.map(x => {
      val pmi = Math.log10((x._2 * bLineNo.value) / (bWords.value(x._1._1) * bWords.value(x._1._2)))
      (x._1, (pmi, x._2))
    }).map(x => {
      x._1 + " " + x._2
    })

    counts.saveAsTextFile(args.output())

  }
}
