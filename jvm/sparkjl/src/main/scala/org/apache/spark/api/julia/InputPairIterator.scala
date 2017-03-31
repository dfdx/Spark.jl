package org.apache.spark.api.julia

import java.io.{BufferedInputStream, DataInputStream, EOFException}
import java.net.Socket

import org.apache.commons.compress.utils.Charsets
import org.apache.spark._


/**
 * Iterator that connects to a Julia process and reads data back to JVM as pair tuples.
 * */
class InputPairIterator(context: TaskContext, worker: Socket, outputThread: OutputThread) extends Iterator[(Any,Any)] with Logging {

  val BUFFER_SIZE = 65536
  
  val env = SparkEnv.get
  val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, BUFFER_SIZE))

  override def next(): (Any, Any) = {
    val obj = _nextObj
    if (hasNext) {
      _nextObj = read()
    }
    obj
  }

  private def read(): (Any, Any) = {
    if (outputThread.exception.isDefined) {
      throw outputThread.exception.get
    }
    try {
      val obj = JuliaRDD.readValueFromStream(stream)
      obj match {
        case pair: Tuple2[Any, Any] =>
          pair
      case other =>
        if (obj == null) {
          (null, null)
        } else {
          throw new SparkException("Unexpected element type " + other.getClass)
        }
      }
    } catch {

      case e: Exception if context.isInterrupted =>
        logDebug("Exception thrown after task interruption", e)
        throw new TaskKilledException

      case e: Exception if env.isStopped =>
        logDebug("Exception thrown after context is stopped", e)
        (null, null)  // exit silently

      case e: Exception if outputThread.exception.isDefined =>
        logError("Julia worker exited unexpectedly (crashed)", e)
        logError("This may have been caused by a prior exception:", outputThread.exception.get)
        throw outputThread.exception.get

      case eof: EOFException =>
        throw new SparkException("Julia worker exited unexpectedly (crashed)", eof)
    }
  }

  var _nextObj : (Any, Any) = read()

  override def hasNext: Boolean = _nextObj != (null, null)

}
