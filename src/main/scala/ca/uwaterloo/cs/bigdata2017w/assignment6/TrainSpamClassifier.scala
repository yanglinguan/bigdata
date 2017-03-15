package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scala.collection.mutable
import scala.math._
import scala.util.Random

/**
  * Created by yanglinguan on 17/3/13.
  */

class Conf(args: Seq[String]) extends ScallopConf(args) {
  //  mainOptions = Seq(input, date, text, parquet)
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model output path", required = true)
  val shuffle = opt[Boolean] (descr = "shuffe")
  verify()
}
object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  val w: mutable.Map[Int, Double] = mutable.Map[Int, Double]()
  val delta = 0.002
  // Scores a document based on its list of features.
  def spamminess(features: Array[Int]) : Double = {
    var score = 0d
//    features.foreach(f => if (w.contains(f)) score += w(f))
    features.foreach(f=>score += w.getOrElse[Double](f, 0.0))
    score
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")

    val sc = new SparkContext(conf)

    val random = new Random()

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), 1)

    if(args.shuffle()) {
      val trained = textFile
        .map(line=> {
          val tokens = line.split(" ")
          val docid = tokens(0)
          val isSpam = if (tokens(1) == "spam") 1.0 else 0.0
          val features = tokens.drop(2).map(x => x.toInt)
          val key = random.nextInt()
          (key, (docid, isSpam, features))
        })
        .sortByKey()
        .map(x => (0, (x._2._1, x._2._2, x._2._3)))
        .groupByKey(1)
        .flatMap(x => {
          x._2.foreach(x => {
//            var score = 0d
//            x._3.foreach(f => score += w.getOrElse[Double](f, 0.0))
            val score = spamminess(x._3)
            val prob = 1.0 / (1 + exp(-score))
            x._3.foreach(f => {
              w(f) = w.getOrElse[Double](f, 0.0) + (x._2 - prob) * delta
            })
          })
          w
        })
      trained.saveAsTextFile(args.model())

    } else {
      val trained = textFile
        .map(line => {
          val tokens = line.split(" ")
          val docid = tokens(0)
          val isSpam = if (tokens(1) == "spam") 1.0 else 0.0
          val features = tokens.drop(2).map(x => x.toInt)
          (0, (docid, isSpam, features))
        })
        .groupByKey(1)
        .flatMap(x => {
          x._2.foreach(x => {
//            var score = 0d
//            x._3.foreach(f => score += w.getOrElse[Double](f, 0.0))
            val score = spamminess(x._3)
            val prob = 1.0 / (1 + exp(-score))
            x._3.foreach(f => {
              //            if (w.contains(f)) {
              //              w(f) += (x._2 - prob) * delta
              //            } else {
              //              w(f) = (x._2 - prob) * delta
              //            }
              w(f) = w.getOrElse[Double](f, 0.0) + (x._2 - prob) * delta
            })
          })
          w
        })
      trained.saveAsTextFile(args.model())
    }




  }


}
