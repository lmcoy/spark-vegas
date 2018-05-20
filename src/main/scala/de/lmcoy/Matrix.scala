package de.lmcoy

case class Matrix(nrow: Int, ncol: Int, data: Array[Double]) {
  def element(row: Int, col: Int) : Double = data(col + row * ncol)

  def updateElement(row: Int, col: Int, value: Double) : Matrix = {
    new Matrix(nrow, ncol, data.updated(col + row * ncol, value))
  }

  def addToElement(row: Int, col: Int, value: Double) : Matrix = {
    val index = col + row * ncol
    new Matrix(nrow, ncol, data.updated(index, data(index) + value))
  }

  def add(row: Int, col: Int, value: Double) = {
    val index = col + row * ncol
    data.update(index, data(index) + value)
  }

  def mulFactor(value: Double) : Matrix =
    new Matrix(nrow, ncol, data.map(_ * value))

  def +(other:Matrix) : Matrix =
    new Matrix(nrow, ncol, data.zip(other.data).map(x => x._1 + x._2))
}

object Matrix {
  def apply(nrow: Int, ncol: Int): Matrix =
    new Matrix(nrow, ncol, Array.fill(nrow*ncol)(0.0))

}
