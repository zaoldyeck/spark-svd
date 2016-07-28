import java.util.concurrent.Executors

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix, SingularValueDecomposition, Vector}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

/**
  * Created by zaoldyeck on 2016/3/24.
  */
class DimensionalityReduction(implicit @transient val sc: SparkContext) extends DataTransformable with Exportable with Serializable {
  override val schema: String = "parquet.`/user/kigo/dataset/modeling_data`"
  override val path: String = "evaluate.csv"

  def run(): Unit = {
    val userVectors: RDD[(Int, Vector)] = getUserVectors.persist
    val playersId: Broadcast[Array[Int]] = sc.broadcast(getPlayersIdByGameId(229).collect)
    val gameIdMappingIndex: Broadcast[Map[Int, Int]] = sc.broadcast(getGameIdMappingIndex)

    val svd: SingularValueDecomposition[RowMatrix, Matrix] = new RowMatrix(userVectors.values).computeSVD(34, computeU = true)
    val diagS: DenseMatrix = DenseMatrix.diag(svd.s)
    val dimensionalityReductionMatrix: RDD[Vector] = svd.U.multiply(diagS).multiply(svd.V.transpose).rows
    val zipWithUserId: RDD[(Int, Vector)] = dimensionalityReductionMatrix.zip(userVectors.map(_._1)).map {
      case (vector, id) => (id, vector)
    }

    val sortByValue: RDD[Int] = zipWithUserId.sortBy({
      case (id, vector) => vector(gameIdMappingIndex.value(229))
    }, false).map(_._1).persist
    val count: Long = sortByValue.count

    val executorService: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(10))
    val futures: IndexedSeq[Future[Unit]] = (0.01 to 0.3 by 0.01).map(percent => Future {
      val numUser: Int = (count * percent).toInt
      val double: Double = playersId.value.length.toDouble
      val recall: Double = sortByValue.take(numUser).count(playersId.value.contains(_)) / double
      writeFile(s"$percent,$recall,$numUser")
    }(executorService))

    Await.result(Future.sequence(futures), Duration.Inf)
  }
}
