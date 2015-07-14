/*
  Load a matrix and perform spectral clustering using gaussian random features
  note that the maximum number of rows in the matrix is limited in LoadMSIData 
  (only for csv format)
 */

package org.apache.spark.mllib.linalg.distributed

import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.log4j.PropertyConfigurator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Matrices, DenseMatrix, Matrix, DenseVector, Vector, SparseVector}
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Axis, qr, svd, sum, SparseVector => BSV}
import breeze.numerics.{cos => brzCos}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

import org.apache.spark.sql.{SQLContext, Row => SQLRow}
import scala.collection.mutable.ListBuffer

object SpectralClusterExample {

  def fromBreeze(mat: BDM[Double]): DenseMatrix = {
    new DenseMatrix(mat.rows, mat.cols, mat.data, mat.isTranspose) 
  }

  def fromBreezeV(v: BDV[Double]): DenseVector = {
    new DenseVector(v.data)
  }

  def loadMSIData(sc: SparkContext, matkind: String, shape: Tuple2[Int, Int], inpath: String, nparts: Int = 0) = {

      val sqlctx = new org.apache.spark.sql.SQLContext(sc)
      import sqlctx.implicits._

      val mat0: IndexedRowMatrix = 
        if(matkind == "csv") {
          /* weird way to input data:
           * We assume the data is stored as an m-by-n matrix, with each
           * observation as a column
           * we pass in the matrix dimensions as (n,m) = shape
           * the following code loads the data into an n-by-m matrix
           * so that the observations are now rows of the matrix
           */
          val nonzeros = sc.textFile(inpath).map(_.split(",")).
          map(x => new MatrixEntry(x(1).toLong, x(0).toLong, x(2).toDouble))
          val coomat = new CoordinateMatrix(nonzeros, shape._1, shape._2)
          val mat = coomat.toIndexedRowMatrix()

          // HARD CODE SIZE LIMIT
          val MAXNUMROWS = 2000000
          val lessrows = mat.rows.filter(row => row.index < MAXNUMROWS)
          new IndexedRowMatrix(lessrows, math.min(MAXNUMROWS, mat.numRows), mat.numCols.toInt)
        } else if(matkind == "idxrow") {
          val rows = sc.objectFile[IndexedRow](inpath)
          new IndexedRowMatrix(rows, shape._1, shape._2)
        } else if(matkind == "df") {
          val numRows = if(shape._1 != 0) shape._1 else sc.textFile(inpath + "/rowtab.txt").count.toInt
          val numCols = if(shape._2 != 0) shape._2 else sc.textFile(inpath + "/coltab.txt").count.toInt
          val rows =
            sqlctx.parquetFile(inpath + "/matrix.parquet").rdd.map {
              case SQLRow(index: Long, vector: Vector) =>
                new IndexedRow(index, vector)
            }
          new IndexedRowMatrix(rows, numRows, numCols)
        } else {
          throw new RuntimeException(s"unrecognized matkind: $matkind")
        }
        
      if (nparts == 0) {
        mat0
      } else {
        new IndexedRowMatrix(mat0.rows.coalesce(nparts), mat0.numRows, mat0.numCols.toInt) 
      }
  }

  def evalRandomFeatureMapping(mat: IndexedRowMatrix, numRandFeats: Int, randSeed: Long) : IndexedRowMatrix = {
    val rng = new java.util.Random
    rng.setSeed(randSeed)
    val freqs = DenseMatrix.randn(numRandFeats, mat.numCols.toInt, rng).toBreeze.asInstanceOf[BDM[Double]]

    def applyRFM(row: Vector) : DenseVector = {
      fromBreezeV(brzCos(freqs * row.toBreeze.asInstanceOf[BSV[Double]]))
    }

    new IndexedRowMatrix(mat.rows.map(row => new IndexedRow(row.index, applyRFM(row.vector))))
  }

  def appMain(sc: SparkContext, args: Array[String]) {

    val inpath = args(0)
    val numrows = args(1).toInt
    val numcols = args(2).toInt
    val k = args(3).toInt
    val numRandFeats = args(4).toInt
    val maxIters = 30
    val numparts = 20

    val rng = new java.util.Random
    val randSeed = rng.nextLong

    val log = LogManager.getRootLogger()
    log.setLevel(Level.INFO)

    // load the data as an IndexedRowMatrix
    log.info("Loading data")
    var mat = loadMSIData(sc, "csv", (numrows, numcols), inpath, nparts = numparts)
    log.info("Finished loading data")
    mat.rows.cache()

    // evaluate the random feature mappings 
    var randomfeatures = evalRandomFeatureMapping(mat, numRandFeats, randSeed)
    randomfeatures.rows.cache()

    // compute the top-k left singularvectors using ARPACK
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = randomfeatures.toRowMatrix.computeSVD(k, true)
    val u = svd.U.toBreeze.asInstanceOf[BDM[Double]]
    val urows = ListBuffer.empty[Vector]
    for(rownum <- 0 until u.rows) {
      urows += fromBreezeV(u(rownum, ::).t.toDenseVector)
    }
    val urdd = sc.parallelize(urows)
    
    // do k-means clustering on the left singular vectors
    val clustering = KMeans.train(urdd, k, maxIters)
    val clusterCost = clustering.computeCost(urdd)

    // print WSSE 
    val writer = new java.io.PrintWriter("/home/ubuntu/log.out")
    writer.write(f"WSSE = ${clusterCost}")
    writer.close()
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PureScalaSpectralClustering")
    conf.set("spark.task.maxFailures", "1")
    val sc = new SparkContext(conf)

    appMain(sc, args)
  }
}

