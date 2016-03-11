import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

/**
  * Created by zaoldyeck on 2016/3/7.
  */
class FindTopLoginPercentUsers(implicit sc: SparkContext) extends Serializable with Exportable {
  val schema: String = "parquet.`/user/atkins/kigo_sucks/transform/by_user`"
  private val evaluatePath: String = "evaluate"
  private val dataTransformer: DataTransformer = new DataTransformer("parquet.`/user/kigo/dataset/modeling_data`")
  private val gameId: Int = 192
  private val playersId: Array[Int] = sc.broadcast(dataTransformer.getPlayersIdByGameId(gameId).collect).value
  private val gameIdMappingIndex: Map[Int, Int] = dataTransformer.getGameIdMappingIndex
  private val percent: Double = 0.2
  private val userVectors: RDD[(Int, Vector)] = dataTransformer.getUserVectors.persist

  private val svd: SingularValueDecomposition[RowMatrix, Matrix] = new RowMatrix(userVectors.values).computeSVD(30, computeU = true)
  private val userToGames: RDD[(Int, Vector)] = svd.U.multiply(svd.V.transpose).rows.zip(userVectors.keys) map {
    case (vector, userId) => (userId, vector)
  }

  private val sortedUserGames: RDD[(Int, Vector)] = userToGames.sortBy({
    case (userId, vector) => vector(69)
  }, false)
  val userGamesSize: Long = sortedUserGames.count

  private val takeXPercentUsers: RDD[(Int, Vector)] = sortedUserGames.zipWithIndex.filter {
    case (userGames, index) => index < (userGamesSize * percent).toInt
  } map (_._1)

  private val recall: Double = (takeXPercentUsers.map {
    case (userId, vector) => if (playersId.contains(userId)) 1 else 0
  } sum) / playersId.length.toDouble

  writeFile(evaluatePath, "%.4f,%.4f".format(percent, recall))
}