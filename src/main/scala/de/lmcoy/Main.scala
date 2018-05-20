package de.lmcoy

import org.apache.spark.sql.SparkSession

object Main extends App {

  def f(x: List[Double]): Double = x(0)*x(1)

  implicit val spark = SparkSession.builder.appName("VEGAS").master("local[*]")
    .getOrCreate()

  val t1 = System.nanoTime
  val ndim = 2
  val nbin = 50
  val vegas = VegasRandom(ndim = ndim, nbin = nbin, f)

  val n1 = 100000
  val it = vegas.iteration(n1).iteration(n1).iteration(n1).iteration(n1).iteration(n1)
  println(s"integral: ${it.integralTotal}")

  val n2 = 1000000
  val it2 = it.iteration(n2).iteration(n2).iteration(n2).iteration(n2).iteration(n2)

  println(s"integral: ${it2.integralTotal}")

  val duration = (System.nanoTime - t1) / 1e9d
  println( s"duration: $duration s")
}
