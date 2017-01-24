package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

/**
  * Created by yanglinguan on 17/1/21.
  */

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

class MyPartitioner(partitions: Int) extends Partitioner {

  override def numPartitions: Int = partitions
  override def getPartition(key: Any): Int =
  {
    val k = key.toString().split(" ");
    (k(0).hashCode() & Integer.MAX_VALUE) % partitions;
  }

}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {

  val log = Logger.getLogger(getClass().getName())


  def main(argv: Array[String]) {
    val args = new Conf(argv)
    var marginal = 0f;

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyPairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val l1 = tokens.sliding(2).toList.map(l => List(l.head, "*")).map(p => p.mkString(" "))
          val l2 = tokens.sliding(2).map(p => p.mkString(" ")).toList
          l1:::l2
        } else {
            List()
        }
      })
      .map(bigram => (bigram, 1))
      .partitionBy(new MyPartitioner(args.reducers()))
      .persist()
      .reduceByKey(_+_)
      .sortByKey()
      .map(x => {
        val l = x._1.split(" ")
        if(l(1) == "*") {
          marginal = x._2
          ((l(0), l(1)), x._2)
        } else {
          ((l(0), l(1)), x._2.toFloat/marginal)
        }
      })

    counts.saveAsTextFile(args.output())
  }
}
