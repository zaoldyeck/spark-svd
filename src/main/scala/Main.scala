import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zaoldyeck on 2016/3/2.
  */
object Main {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("SVD")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.instances", "10")
      .registerKryoClasses(
        Array(
          classOf[DataTransformable],
          classOf[Exportable],
          classOf[FindTopLoginPercentUsers],
          classOf[SingularValues],
          classOf[DimensionalityReduction]))

    implicit val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")

    //new FindTopLoginPercentUsers
    //new SingularValues
    new DimensionalityReduction().run()
    sc.stop
  }
}