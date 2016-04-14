package org.apache.spark.api.julia

import java.io.{BufferedInputStream, DataInputStream, EOFException}
import java.net.Socket

import org.apache.commons.compress.utils.Charsets
import org.apache.spark._

/**
 * Iterator that connects to a Julia process and reads data back to JVM.
 * */
class InputIterator(context: TaskContext, worker: Socket, outputThread: OutputThread) extends Iterator[Array[Byte]] with Logging {

  val BUFFER_SIZE = 65536
  
  val env = SparkEnv.get
  val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, BUFFER_SIZE))

  override def next(): Array[Byte] = {
    val obj = _nextObj
    if (hasNext) {
      _nextObj = read()
    }
    obj
  }

  private def read(): Array[Byte] = {
    if (outputThread.exception.isDefined) {
      throw outputThread.exception.get
    }
    try {
      stream.readInt() match {
        case length if length > 0 =>
          val obj = new Array[Byte](length)
          stream.readFully(obj)
          obj
        case 0 => Array.empty[Byte]
        case SpecialLengths.JULIA_EXCEPTION_THROWN =>
          // Signals that an exception has been thrown in julia
          val exLength = stream.readInt()
          val obj = new Array[Byte](exLength)
          stream.readFully(obj)
          throw new Exception(new String(obj, Charsets.UTF_8),
            outputThread.exception.getOrElse(null))
        case SpecialLengths.END_OF_DATA_SECTION =>
          if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
            null
          } else {
            throw new RuntimeException("Protocol error")
          }

      }
    } catch {

      case e: Exception if context.isInterrupted =>
        logDebug("Exception thrown after task interruption", e)
        throw new TaskKilledException

      case e: Exception if env.isStopped =>
        logDebug("Exception thrown after context is stopped", e)
        null  // exit silently

      case e: Exception if outputThread.exception.isDefined =>
        logError("Julia worker exited unexpectedly (crashed)", e)
        logError("This may have been caused by a prior exception:", outputThread.exception.get)
        throw outputThread.exception.get

      case eof: EOFException =>
        throw new SparkException("Julia worker exited unexpectedly (crashed)", eof)
    }
  }

  var _nextObj = read()

  override def hasNext: Boolean = _nextObj != null

}
