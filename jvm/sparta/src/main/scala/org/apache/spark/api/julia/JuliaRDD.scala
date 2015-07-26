package org.apache.spark.api.julia

import java.io._
import java.net._
import java.util
import java.util.Collections
import java.util.{ArrayList => JArrayList}
import java.util.{List => JList}
import java.util.{Map => JMap}

import com.google.common.base.Charsets.UTF_8
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}
import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{RedirectThread, Utils}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.existentials
import scala.util.control.NonFatal

class JuliaRDD(
    @transient parent: RDD[_],
    command: Array[Byte]
//    envVars: JMap[String, String],
//    preservePartitioning: Boolean,
//    accumulator: Accumulator[JList[Array[Byte]]]
) extends RDD[Array[Byte]](parent) {

  val envVars = new util.HashMap[String, String]()  // TODO
  val preservePartitioning = true                   // TODO

  // @transient val juliaWorkerFactory: JuliaWorkerFactory = new JuliaWorkerFactory(envVars.toMap)

  val bufferSize = 65536
  val reuseWorker = true

  override def getPartitions: Array[Partition] = firstParent.partitions

  override val partitioner: Option[Partitioner] = {
    if (preservePartitioning) firstParent.partitioner else None
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val env = SparkEnv.get
    // return Collections.emptyList[Array[Byte]]().iterator()
    val worker: Socket = JuliaRDD.createWorker()

    // Start a thread to feed the process input from our parent's iterator
    val writerThread = new WriterThread(env, worker, split, context)
    writerThread.start()

    // Return an iterator that read lines from the process's stdout
    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))
    val stdoutIterator = new Iterator[Array[Byte]] {
      override def next(): Array[Byte] = {
        val obj = _nextObj
        if (hasNext) {
          _nextObj = read()
        }
        obj
      }

      private def read(): Array[Byte] = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          stream.readInt() match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
              println("OBJECT: " + new String(obj, "UTF-8"))
              obj
            case 0 => Array.empty[Byte]
            case SpecialLengths.JULIA_EXCEPTION_THROWN =>
              // Signals that an exception has been thrown in julia
              val exLength = stream.readInt()
              val obj = new Array[Byte](exLength)
              stream.readFully(obj)
              throw new Exception(new String(obj, UTF_8),
                writerThread.exception.getOrElse(null))
            case SpecialLengths.END_OF_DATA_SECTION =>
              println("We are done!")
              // We've finished the data section of the output, but we can still
              // read some accumulator updates:
//              val numAccumulatorUpdates = stream.readInt()
//              (1 to numAccumulatorUpdates).foreach { _ =>
//                val updateLen = stream.readInt()
//                val update = new Array[Byte](updateLen)
//                stream.readFully(update)
//                accumulator += Collections.singletonList(update)
//              }
//              // Check whether the worker is ready to be re-used.
//              if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
//                if (reuse_worker) {
//                  // TODO
//                  // env.releasePythonWorker(pythonExec, envVars.toMap, worker)
//                  released = true
//                }
//              }
              null
          }
        } catch {

          case e: Exception if context.isInterrupted =>
            logDebug("Exception thrown after task interruption", e)
            throw new TaskKilledException

          case e: Exception if env.isStopped =>
            logDebug("Exception thrown after context is stopped", e)
            null  // exit silently

          case e: Exception if writerThread.exception.isDefined =>
            logError("Julia worker exited unexpectedly (crashed)", e)
            logError("This may have been caused by a prior exception:", writerThread.exception.get)
            throw writerThread.exception.get

          case eof: EOFException =>
            throw new SparkException("Julia worker exited unexpectedly (crashed)", eof)
        }
      }

      var _nextObj = read()

      override def hasNext: Boolean = _nextObj != null
    }
    new InterruptibleIterator(context, stdoutIterator)
  }

  val asJavaRDD : JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)

  /**
   * The thread responsible for writing the data from the JuliaRDD's parent iterator to the
   * Julia process.
   */
  class WriterThread(env: SparkEnv, worker: Socket, split: Partition, context: TaskContext)
    extends Thread(s"stdout writer for julia") {

    @volatile private var _exception: Exception = null

    // setDaemon(true)

    /** Contains the exception thrown while writing the parent iterator to the Julia process. */
    def exception: Option[Exception] = Option(_exception)

    /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
    def shutdownOnTaskCompletion() {
      assert(context.isCompleted)
      this.interrupt()
    }

    override def run(): Unit = Utils.logUncaughtExceptions {
      try {
        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(split.index)
        dataOut.flush()
        // Serialized command:
        dataOut.writeInt(command.length)
        dataOut.write(command)
        // Data values
        JuliaRDD.writeIteratorToStream(firstParent.iterator(split, context), dataOut)
        dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
        dataOut.writeInt(SpecialLengths.END_OF_STREAM)
        dataOut.flush()
      } catch {
        case e: Exception if context.isCompleted || context.isInterrupted =>
          logDebug("Exception thrown after task completion (likely due to cleanup)", e)
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }

        case e: Exception =>
          // We must avoid throwing exceptions here, because the thread uncaught exception handler
          // will kill the whole executor (see org.apache.spark.executor.Executor).
          _exception = e
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }
      } finally {
        // Release memory used by this thread for shuffles
        env.shuffleMemoryManager.releaseMemoryForThisThread()
        // Release memory used by this thread for unrolling blocks
        env.blockManager.memoryStore.releaseUnrollMemoryForThisThread()
      }
    }
  }

}

private object SpecialLengths {
  val END_OF_DATA_SECTION = -1
  val JULIA_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
  val END_OF_STREAM = -4
  val NULL = -5
}

object JuliaRDD extends Logging {

  def fromJavaRDD[T](javaRdd: JavaRDD[T], command: Array[Byte]): JuliaRDD = new JuliaRDD(JavaRDD.toRDD(javaRdd), command)

  def createWorker(): Socket = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1).map(_.toByte)))

      // Create and start the worker
      val pb = new ProcessBuilder(Seq("julia", "-e", "using Sparta; Sparta.launch_worker()"))
      // val workerEnv = pb.environment()
      // workerEnv.putAll(envVars)
      val worker = pb.start()

      // Redirect worker stdout and stderr
      redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

      // Tell the worker our port
      val out = new OutputStreamWriter(worker.getOutputStream)
      out.write(serverSocket.getLocalPort + "\n")
      out.flush()

      // Wait for it to connect to our socket
      serverSocket.setSoTimeout(10000)
      try {
        val socket = serverSocket.accept()
        // workers.put(socket, worker)
        return socket
      } catch {
        case e: Exception =>
          throw new SparkException("Julia worker did not connect back in time", e)
      }
    } finally {
      if (serverSocket != null) {
        serverSocket.close()
      }
    }
    null
  }

  /**
   * Redirect the given streams to our stderr in separate threads.
   */
  private def redirectStreamsToStderr(stdout: InputStream, stderr: InputStream) {
    try {
      new RedirectThread(stdout, System.err, "stdout reader for julia").start()
      new RedirectThread(stderr, System.err, "stderr reader for julia").start()
    } catch {
      case e: Exception =>
        logError("Exception in redirecting streams", e)
    }
  }

  /**
   * Adapter for calling SparkContext#runJob from Python.
   *
   * This method will serve an iterator of an array that contains all elements in the RDD
   * (effectively a collect()), but allows you to run on a certain subset of partitions,
   * or to enable local execution.
   *
   * @return the port number of a local socket which serves the data collected from this job.
   */
  def runJob(
              sc: SparkContext,
              rdd: JavaRDD[Array[Byte]],
              partitions: JArrayList[Int],
              allowLocal: Boolean): Int = {
    type ByteArray = Array[Byte]
    type UnrolledPartition = Array[ByteArray]
    val allPartitions: Array[UnrolledPartition] =
      sc.runJob(rdd, (x: Iterator[ByteArray]) => x.toArray, partitions, allowLocal)
    val flattenedPartition: UnrolledPartition = Array.concat(allPartitions: _*)
    serveIterator(flattenedPartition.iterator,
      s"serve RDD ${rdd.id} with partitions ${partitions.mkString(",")}")
  }

  /**
   * A helper function to collect an RDD as an iterator, then serve it via socket.
   *
   * @return the port number of a local socket which serves the data collected from this job.
   */
  def collectAndServe[T](rdd: RDD[T]): Int = {
    serveIterator(rdd.collect().iterator, s"serve RDD ${rdd.id}")
  }

  def readRDDFromFile(sc: JavaSparkContext, filename: String, parallelism: Int):
  JavaRDD[Array[Byte]] = {
    val file = new DataInputStream(new FileInputStream(filename))
    try {
      val objs = new collection.mutable.ArrayBuffer[Array[Byte]]
      try {
        while (true) {
          val length = file.readInt()
          val obj = new Array[Byte](length)
          file.readFully(obj)
          objs.append(obj)
        }
      } catch {
        case eof: EOFException => {}
      }
      JavaRDD.fromRDD(sc.sc.parallelize(objs, parallelism))
    } finally {
      file.close()
    }
  }


  def writeIteratorToStream[T](iter: Iterator[T], dataOut: DataOutputStream) {

    def write(obj: Any): Unit = obj match {
      case null =>
        dataOut.writeInt(SpecialLengths.NULL)
      case arr: Array[Byte] =>
        dataOut.writeInt(arr.length)
        dataOut.write(arr)
      case str: String =>
        writeUTF(str, dataOut)
      case stream: PortableDataStream =>
        write(stream.toArray())
      case (key, value) =>
        write(key)
        write(value)
      case other =>
        throw new SparkException("Unexpected element type " + other.getClass)
    }

    iter.foreach(write)
  }


  def writeUTF(str: String, dataOut: DataOutputStream) {
    val bytes = str.getBytes(UTF_8)
    dataOut.writeInt(bytes.length)
    dataOut.write(bytes)
  }

  /**
   * Create a socket server and a background thread to serve the data in `items`,
   *
   * The socket server can only accept one connection, or close if no connection
   * in 3 seconds.
   *
   * Once a connection comes in, it tries to serialize all the data in `items`
   * and send them into this connection.
   *
   * The thread will terminate after all the data are sent or any exceptions happen.
   */
  private def serveIterator[T](items: Iterator[T], threadName: String): Int = {
    val serverSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"))
    // Close the socket if no connection in 3 seconds
    serverSocket.setSoTimeout(3000)

    new Thread(threadName) {
      setDaemon(true)
      override def run() {
        try {
          val sock = serverSocket.accept()
          val out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream))
//          Utils.tryWithSafeFinally {
//            writeIteratorToStream(items, out)
//          } {
//            out.close()
//          }
          try {
            writeIteratorToStream(items, out)
          } finally {
            out.close()
          }
        } catch {
          case NonFatal(e) =>
            logError(s"Error while sending iterator", e)
        } finally {
          serverSocket.close()
        }
      }
    }.start()

    serverSocket.getLocalPort
  }

}

private class BytesToString extends org.apache.spark.api.java.function.Function[Array[Byte], String] {
  override def call(arr: Array[Byte]) : String = new String(arr, UTF_8)
}

