import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector}
import org.apache.spark.rdd.RDD

/**
  * Created by zaoldyeck on 2016/3/21.
  */
class SingularValues(implicit @transient val sc: SparkContext) extends DataTransformable with Exportable with Serializable {
  override val schema: String = "parquet.`/user/kigo/dataset/modeling_data`"
  override val path: String = "singularValues.csv"
  private val userVectors: RDD[(Int, Vector)] = getUserVectors.persist
  private val gameIdMappingIndex: Broadcast[Map[Int, Int]] = sc.broadcast(getGameIdMappingIndex)

  val svd: SingularValueDecomposition[RowMatrix, Matrix] = new RowMatrix(userVectors.values).computeSVD(gameIdMappingIndex.value.size, computeU = true)
  val singularValues: Array[Double] = svd.s.toArray
  val sum: Double = singularValues.sum

  private val weights: List[Double] = singularValues.foldLeft((List[Double](), 0D)) {
    case ((list, s), value) => (list ++ List((s + value) / sum), s + value)
  } _1

  private val slope: List[Double] = weights.foldLeft((List[Double](), 0D)) {
    case ((list, lastValue), value) => (list ++ List(value - lastValue), value)
  } _1

  val content: String = slope.mkString(",")
  writeFile(content)
}
