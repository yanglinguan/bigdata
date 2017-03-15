package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

/**
  * Created by yanglinguan on 17/3/13.
  */

class Conf1(args: Seq[String]) extends ScallopConf(args) {
  //  mainOptions = Seq(input, date, text, parquet)
  mainOptions = Seq(input, model, output)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object ApplySpamClassifier {

  val log = Logger.getLogger(getClass().getName())
  def main(argv: Array[String]) {
    val args = new Conf1(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")

    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val model = sc.textFile(args.model() + "/part-00000")

    val modelmap = model
        .map(line => {
        val token = line.split(",")
        val key = token(0).drop(1).toInt
        val value = token(1).dropRight(1).toDouble
        (key, value)
      }).collectAsMap()

    val bcModel = sc.broadcast(modelmap)

    val testfile = sc.textFile(args.input())
    val test = testfile
      .map(line => {
        val tokens = line.split(" ")
        val docid = tokens(0)
        val isSpam = tokens(1)
        val features = tokens.drop(2).map(x=> x.toInt)
        var score = 0d
        features.foreach(f=> score += bcModel.value.getOrElse[Double](f, 0.0))
        val label = if(score > 0) "spam" else "ham"

        (docid, isSpam, score, label)
      })

    test.saveAsTextFile(args.output())
  }



}
