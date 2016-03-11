import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Created by zaoldyeck on 2016/3/2.
  */
class DataTransformer(schema: String)(implicit @transient sc: SparkContext) extends Serializable {
  val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)

  def getSavingData: RDD[(Int, Int, Int)] = {
    sqlContext.sql(s"select unique_id,game_index,revenue from $schema where web_mobile=2").rdd map {
      case Row(unique_id: Long, game_index: Int, revenue) =>
        (unique_id.toInt, game_index, Some(revenue).getOrElse(0).asInstanceOf[Long].toInt)
    }
  }

  def getLoginData: RDD[(Int, Int, Int)] = {
    sqlContext.sql(s"select unique_id,game_index,unique_login_days from $schema where web_mobile=2").rdd map {
      case Row(unique_id: Long, game_index: Int, unique_login_days) =>
        (unique_id.toInt, game_index, Some(unique_login_days).getOrElse(0).asInstanceOf[Long].toInt)
    }
  }

  def getPlayersIdByGameId(gameId: Int): RDD[Int] = {
    val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    sqlContext.sql(s"select unique_id from $schema where game_index=$gameId").rdd map {
      case Row(unique_id: Long) => unique_id.toInt
    }
  }

  def getGameIdMappingIndex: Map[Int, Int] = {
    val gameIdMappingIndex: Map[Int, Int] = getSavingData map {
      case (userId, gameId, value) => gameId
    } distinct() sortBy (gameId => gameId) zipWithIndex() map {
      case (g, index) => (g, index.toInt)
    } collect() toMap

    sc.broadcast(gameIdMappingIndex).value
  }

  def getUserVectors: RDD[(Int, Vector)] = {
    val gameIdMappingIndex: Map[Int, Int] = getGameIdMappingIndex

    getSavingData.groupBy {
      case (userId, gameId, value) => userId
    } map {
      case (userId, userToGames) =>
        val elements: Seq[(Int, Double)] = userToGames map {
          case (u, g, v) => (gameIdMappingIndex.get(g).get, v.toDouble)
        } toSeq

        (userId, Vectors.sparse(gameIdMappingIndex.size, elements))
    }
  }
}