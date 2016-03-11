import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zaoldyeck on 2016/3/2.
  */
object SVDStarter {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("SVD")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.dynamicAllocation.maxExecutors", "10")
//      .set("spark.executor.cores", "16")
//      .set("spark.executor.memory", "24576m")
//      .set("spark.yarn.executor.memoryOverhead", "8192")
    conf.registerKryoClasses(Array(classOf[FindTopLoginPercentUsers], classOf[DataTransformer], classOf[Exportable]))

    implicit val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")

    new FindTopLoginPercentUsers

    sc.stop
  }
}