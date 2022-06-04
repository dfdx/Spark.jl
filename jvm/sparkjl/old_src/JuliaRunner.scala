package org.apache.spark.api.julia

import scala.collection.JavaConversions._

/**
 * Class for execution of Julia scripts on a cluster.
 * WARNING: this class isn't used currently, will be utilized later
 */
object JuliaRunner {

  def main(args: Array[String]): Unit = {
    val juliaScript = args(0)
    val scriptArgs = args.slice(1, args.length)
    val pb = new ProcessBuilder(Seq("julia", juliaScript) ++ scriptArgs)
    val process = pb.start()
    StreamUtils.redirectStreamsToStderr(process.getInputStream, process.getErrorStream)
    val errorCode = process.waitFor()
    if (errorCode != 0) {
      throw new RuntimeException("Julia script exited with an error")
    }
  }

}
