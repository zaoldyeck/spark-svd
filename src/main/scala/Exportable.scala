import java.io.{FileOutputStream, PrintWriter}

/**
  * Created by zaoldyeck on 2016/3/2.
  */
trait Exportable extends Serializable {
  val path: String

  def writeFile(content: String): Unit = {
    val printWriter: PrintWriter = new PrintWriter(new FileOutputStream(path, true))
    try {
      printWriter.append(content)
      printWriter.println()
    } catch {
      case e: Exception => Logger.log.error(e.printStackTrace())
    } finally printWriter.close()
  }
}