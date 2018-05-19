package de.lmcoy

import de.lmcoy.VegasRandom.{Evaluation, Result, refineGrid}
import org.apache.spark.sql.SparkSession
import scala.annotation.tailrec
import scala.util.Random

case class VegasRandom(ndim: Int, nbin: Int, xi: Matrix, allEvaluations: Evaluation) {

  case class FunctionArgs(wgt: Double, x: List[Double], ia: List[Int])

  private def functionArgs: FunctionArgs = {
    @tailrec
    def go(j: Int, wgt: Double, xs: List[Double], ias: List[Int]) : FunctionArgs = {
      if (j < 0) FunctionArgs(wgt, xs, ias)
      else {
        val u = Random.nextDouble()
        val xn = (1.0 - u) * nbin + 1.0
        val ia = Integer.min(xn.toInt, nbin)
        val x1 = xi.element(j, ia - 1)
        val x2 = if (ia > 1) xi.element(j, ia - 2) else 0.0
        val delta_x = x1 - x2
        val x = x2 + (xn - ia.toDouble) * delta_x
        go(j - 1, wgt * delta_x * nbin.toDouble, x :: xs, ia :: ias)
      }
    }
    go(ndim-1, 1.0, Nil, Nil)
  }

  private def add(args: FunctionArgs, f: Double) = {
    val fx = args.wgt * f
    val fx2 = fx * fx

    @tailrec
    def go(j : Int, ds: Matrix) : Matrix = {
      if (j == ndim) ds
      else go(j+1, ds.addToElement(args.ia(j) - 1, j, fx2))
    }

    val d = go(0, Matrix(nbin, ndim))
    Evaluation(1, fx, fx2, d)
  }

  def iteration(n: Int)(implicit spark: SparkSession): VegasRandom = {
    val iteration = it(n)

    val integral_it = integral(iteration.n.toDouble, iteration.f, iteration.f2)
    println(integral_it)

    val dp = iteration.d.mulFactor(1.0 / Math.pow(iteration.n.toDouble,2.0))
    val nxi = refineGrid(ndim, nbin, xi, dp)

    new VegasRandom(ndim, nbin, nxi, allEvaluations + iteration)
  }

  def it(n:Int)(implicit spark: SparkSession) : Evaluation = {
    val partitions = 4
    val init = spark.sparkContext.parallelize(Seq.fill(partitions)(n/partitions),partitions)
    val randomNumbers = init.flatMap(numRnd => Seq.fill(numRnd)(functionArgs))

    def f(x:List[Double]): Double = x(0)*x(1)
    val withF = randomNumbers.map(fargs => (fargs,f(fargs.x)))

    val values = withF.map(x => add(x._1,x._2))
    values.reduce((a, b) => a + b)
  }

  def integralTotal : Result = integral(allEvaluations.n.toDouble, allEvaluations.f, allEvaluations.f2)

  private def integral(N: Double, f: Double, f2: Double): Result = {
    val jac = 1.0 / N
    val dv2g = 1.0 / (N - 1.0)
    val integral =  f * jac
    val tmp2 = Math.sqrt(f2 * jac * jac * N)
    val f2t = (tmp2 - integral) * (tmp2 + integral)
    val sigma = Math.sqrt(f2t * dv2g)
    Result(integral, sigma)
  }
}

object VegasRandom {
  def apply(ndim: Int, nbin: Int): VegasRandom = {
    new VegasRandom(ndim, nbin, initGrid(ndim, nbin), Evaluation(0,0,0, Matrix(nbin,ndim)))
  }

  def resetGrid(ndim: Int, nbin: Int) : Matrix = {
    @tailrec
    def go(j: Int, acc: Matrix):Matrix = {
      if (j == ndim) acc
      else go(j+1, acc.updateElement(j, 0, 1.0))
    }
    go(0, Matrix(ndim, nbin))
  }

  def initGrid(ndim: Int, nbin: Int):Matrix = {
    val r = Seq.fill(nbin)(1.0)

    @tailrec
    def go(j: Int, grid: Matrix) : Matrix = {
      if (j == ndim) grid
      else go(j+1, rebin(1.0 / nbin.toDouble, nbin, r, grid, j))
    }
    go(0, resetGrid(ndim, nbin))
  }

  private def rebin(rc: Double, nd: Int, r: Seq[Double], xi: Matrix, j: Int): Matrix = {
    var dr: Double = 0.0
    var k = 0
    val xin = Array.fill(nd - 1)(0.0)

    for (i <- 0 until nd - 1) {
      while (rc > dr) {
        dr += r(k)
        k += 1
      }
      val xo = if (k > 1) xi.element(j, k-2) else 0.0
      val xn = xi.element(j,k-1)
      dr -= rc
      xin.update(i, xn - (xn - xo) * dr / r(k - 1))
    }
    var xiout = xi
    for (i <- 0 until nd - 1) {
      xiout = xiout.updateElement(j, i, xin(i))
    }
    xiout.updateElement(j, nd -1, 1.0)
  }

  private def refineGrid(ndim: Int, nbin: Int, xi: Matrix, d: Matrix) : Matrix = {
    val ALPH = 1.5
    val TINY = 1e-30
    val dt = Array.fill(ndim)(0.0)
    val r = Array.fill(nbin)(0.0)
    var d1 = d
    var nxi = xi
    for (j <- 0 until ndim) {
      var xo = d.element(0,j)
      var xn = d.element(1,j)
      d1 = d1.updateElement(0,j, (xo+xn)/2.0)
      dt.update(j, d1.element(0,j))
      for (i <- 2 until nbin) {
        val rc = xo + xn
        xo = xn
        xn = d1.element(i,j)
        d1 = d1.updateElement(i-1, j, (rc + xn) / 3.0)
        dt.update(j, dt(j) + d1.element(i-1, j))
      }
      d1 = d1.updateElement(nbin-1, j, (xo + xn) / 2.0)
      dt.update(j, dt(j) + d1.element(nbin-1, j))
    }
    for (j<-0 until ndim) {
      var rc = 0.0
      for (i <- 0 until nbin) {
        var dij = d1.element(i,j)
        if (dij < TINY) {
          d1 = d1.updateElement(i,j, TINY)
          dij = TINY
        }
        r.update(i, Math.pow((1.0 - dij / dt(j)) / (Math.log(dt(j)) - Math.log(dij)), ALPH))
        rc += r(i)
      }
      nxi = rebin(rc/nbin.toDouble,nbin,r,nxi,j)
    }
    nxi
  }

  case class Evaluation(n: Int, f: Double, f2: Double, d: Matrix) {
    def +(other: Evaluation) : Evaluation = {
      Evaluation(n + other.n, f + other.f, f2 + other.f2, d + other.d)
    }
  }

  case class Result(value: Double, err: Double)
}
