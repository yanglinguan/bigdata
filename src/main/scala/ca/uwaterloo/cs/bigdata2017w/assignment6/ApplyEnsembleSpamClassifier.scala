package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

/**
  * Created by yanglinguan on 17/3/13.
  */

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, output, method)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val method = opt[String](descr = "method average or vote", required = true)
  verify()
}


object ApplyEnsembleSpamClassifier {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)
    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")

    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val modelX = sc.textFile(args.model() + "/part-00000")
    val modelY = sc.textFile(args.model() + "/part-00001")
    val modelB = sc.textFile(args.model() + "/part-00002")

    val modelXmap = modelX
      .map(line => {
        val token = line.split(",")
        val key = token(0).drop(1).toInt
        val value = token(1).dropRight(1).toDouble
        (key, value)
      }).collectAsMap()

    val modelYmap = modelY
      .map(line => {
        val token = line.split(",")
        val key = token(0).drop(1).toInt
        val value = token(1).dropRight(1).toDouble
        (key, value)
      }).collectAsMap()

    val modelBmap = modelB
      .map(line => {
        val token = line.split(",")
        val key = token(0).drop(1).toInt
        val value = token(1).dropRight(1).toDouble
        (key, value)
      }).collectAsMap()

    val bcModelX = sc.broadcast(modelXmap)
    val bcModelY = sc.broadcast(modelYmap)
    val bcModelB = sc.broadcast(modelBmap)

    val method = args.method().toString

    val testfile = sc.textFile(args.input())
    val test = testfile
      .map(line => {
        val tokens = line.split(" ")
        val docid = tokens(0)
        val isSpam = tokens(1)
        val features = tokens.drop(2).map(x=> x.toInt)
        var scoreX = 0d
        var scoreY = 0d
        var scoreB = 0d
        features.foreach(f=> scoreX += bcModelX.value.getOrElse[Double](f, 0.0))
        features.foreach(f=> scoreY += bcModelY.value.getOrElse[Double](f, 0.0))
        features.foreach(f=> scoreB += bcModelB.value.getOrElse[Double](f, 0.0))
        if(method == "average") {
          val avg = (scoreX + scoreB + scoreY) / 3
          val label = if(avg > 0) "spam" else "ham"
          (docid, isSpam, avg, label)
        } else {
          val voteX = if(scoreX > 0) 1 else -1
          val voteY = if(scoreY > 0) 1 else -1
          val voteB = if(scoreB > 0) 1 else -1
          val vote = voteX + voteY + voteB
          val label = if(vote > 0) "spam" else "ham"
          (docid, isSpam, vote, label)
        }
      })
    test.saveAsTextFile(args.output())
  }
}
