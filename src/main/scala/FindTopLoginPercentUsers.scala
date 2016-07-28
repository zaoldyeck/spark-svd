import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix, SingularValueDecomposition, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

/**
  * Created by zaoldyeck on 2016/3/7.
  */
class FindTopLoginPercentUsers(implicit @transient val sc: SparkContext) extends DataTransformable with Exportable with Serializable {
  override val schema: String = "parquet.`/user/kigo/dataset/modeling_data`"
  override val path: String = "evaluate.csv"
  private val gameId: Int = 192
  private val playersId: Broadcast[Array[Int]] = sc.broadcast(getPlayersIdByGameId(gameId).collect)
  private val gameIdMappingIndex: Broadcast[Map[Int, Int]] = sc.broadcast(getGameIdMappingIndex)
  private val percent: Double = 0.2
  private val userVectors: RDD[(Int, Vector)] = getUserVectors.persist

  private val svd: SingularValueDecomposition[RowMatrix, Matrix] = new RowMatrix(userVectors.values).computeSVD(30, computeU = true)

  DenseMatrix.diag(svd.s)

  private val userToGames: RDD[(Int, Vector)] = svd.U.multiply(svd.V.transpose).rows.zip(userVectors.keys) map {
    case (vector, userId) => (userId, vector)
  }

  private val sortedUserGames: RDD[(Int, Vector)] = userToGames.sortBy({
    case (userId, vector) => vector(gameIdMappingIndex.value(gameId))
  }, false)
  val userGamesSize: Long = sortedUserGames.count

  private val takeXPercentUsers: RDD[(Int, Vector)] = sortedUserGames.zipWithIndex.filter {
    case (userGames, index) => index < (userGamesSize * percent).toInt
  } map (_._1)

  private val recall: Double = (takeXPercentUsers.repartition(takeXPercentUsers.getNumPartitions).map {
    case (userId, vector) => if (playersId.value.contains(userId)) 1 else 0
  } sum) / playersId.value.length.toDouble

  writeFile("%.4f,%.4f".format(percent, recall))
}