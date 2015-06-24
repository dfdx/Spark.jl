package sparta

import java.io._
import java.net.ServerSocket
import java.util.{Map => JMap}

import scala.collection.JavaConversions._
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkEnv, SparkException, TaskContext}

import sparta.SerDe._

private abstract class JuliaRRDD[T: ClassTag, U: ClassTag](
                                                           parent: RDD[T],
                                                           numPartitions: Int,
                                                           func: Array[Byte],
                                                           deserializer: String,
                                                           serializer: String,
                                                           packageNames: Array[Byte],
                                                           rLibDir: String,
                                                           broadcastVars: Array[Broadcast[Object]])
  extends RDD[U](parent) {
  protected var dataStream: DataInputStream = _
  private var bootTime: Double = _

  override def getPartitions = parent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {

    // Timing start
    bootTime = System.currentTimeMillis / 1000.0

    // The parent may be also an RRDD, so we should launch it first.
    val parentIterator = firstParent[T].iterator(split, context)

    // we expect two connections
    val serverSocket = new ServerSocket(0, 2)
    val listenPort = serverSocket.getLocalPort()

    // The stdout/stderr is shared by multiple tasks, because we use one daemon
    // to launch child process as worker.
    val errThread = JuliaRDD.createJuliaWorker(rLibDir, listenPort)

    // We use two sockets to separate input and output, then it's easy to manage
    // the lifecycle of them to avoid deadlock.
    // TODO: optimize it to use one socket

    // the socket used to send out the input of task
    serverSocket.setSoTimeout(10000)
    val inSocket = serverSocket.accept()
    startStdinThread(inSocket.getOutputStream(), parentIterator, split.index)

    // the socket used to receive the output of task
    val outSocket = serverSocket.accept()
    val inputStream = new BufferedInputStream(outSocket.getInputStream)
    dataStream = new DataInputStream(inputStream)
    serverSocket.close()

    try {

      return new Iterator[U] {
        def next(): U = {
          val obj = _nextObj
          if (hasNext) {
            _nextObj = read()
          }
          obj
        }

        var _nextObj = read()

        def hasNext(): Boolean = {
          val hasMore = (_nextObj != null)
          if (!hasMore) {
            dataStream.close()
          }
          hasMore
        }
      }
    } catch {
      case e: Exception =>
        // throw new SparkException("R computation failed with\n " + errThread.getLines())
        throw new Exception("Julia computation failed")
    }
  }

  /**
   * Start a thread to write RDD data to the R process.
   */
  private def startStdinThread[T](
                                   output: OutputStream,
                                   iter: Iterator[T],
                                   splitIndex: Int) = {

    val env = SparkEnv.get
    val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
    val stream = new BufferedOutputStream(output, bufferSize)

    new Thread("writer for R") {
      override def run() {
        try {
          SparkEnv.set(env)
          val dataOut = new DataOutputStream(stream)
          dataOut.writeInt(splitIndex)

          SerDe.writeString(dataOut, deserializer)
          SerDe.writeString(dataOut, serializer)

          dataOut.writeInt(packageNames.length)
          dataOut.write(packageNames)

          dataOut.writeInt(func.length)
          dataOut.write(func)

          dataOut.writeInt(broadcastVars.length)
          broadcastVars.foreach { broadcast =>
            // TODO(shivaram): Read a Long in R to avoid this cast
            dataOut.writeInt(broadcast.id.toInt)
            // TODO: Pass a byte array from R to avoid this cast ?
            val broadcastByteArr = broadcast.value.asInstanceOf[Array[Byte]]
            dataOut.writeInt(broadcastByteArr.length)
            dataOut.write(broadcastByteArr)
          }

          dataOut.writeInt(numPartitions)

          if (!iter.hasNext) {
            dataOut.writeInt(0)
          } else {
            dataOut.writeInt(1)
          }

          val printOut = new PrintStream(stream)

          def writeElem(elem: Any): Unit = {
            if (deserializer == SerializationFormats.BYTE) {
              val elemArr = elem.asInstanceOf[Array[Byte]]
              dataOut.writeInt(elemArr.length)
              dataOut.write(elemArr)
            } else if (deserializer == SerializationFormats.STRING) {
              // write string(for StringRRDD)
              printOut.println(elem)
            }
          }

          for (elem <- iter) {
            elem match {
              case (key, value) =>
                writeElem(key)
                writeElem(value)
              case _ =>
                writeElem(elem)
            }
          }
          stream.flush()
        } catch {
          // TODO: We should propogate this error to the task thread
          case e: Exception =>
            System.err.println("R Writer thread got an exception " + e)
            e.printStackTrace()
        } finally {
          Try(output.close())
        }
      }
    }.start()
  }

  protected def readData(length: Int): U

  protected def read(): U = {
    try {
      val length = dataStream.readInt()

      length match {
        case SpecialLengths.TIMING_DATA =>
          // Timing data from R worker
          val boot = dataStream.readDouble - bootTime
          val init = dataStream.readDouble
          val broadcast = dataStream.readDouble
          val input = dataStream.readDouble
          val compute = dataStream.readDouble
          val output = dataStream.readDouble
          logInfo(
            ("Times: boot = %.3f s, init = %.3f s, broadcast = %.3f s, " +
              "read-input = %.3f s, compute = %.3f s, write-output = %.3f s, " +
              "total = %.3f s").format(
                boot,
                init,
                broadcast,
                input,
                compute,
                output,
                boot + init + broadcast + input + compute + output))
          read()
        case length if length >= 0 =>
          readData(length)
      }
    } catch {
      case eof: EOFException =>
        throw new SparkException("R worker exited unexpectedly (cranshed)", eof)
    }
  }
}

object JuliaRDD {

  def createJuliaWorker(dir: String, port: Int) = ???

}

private object SpecialLengths {
  val TIMING_DATA   = -1
}
