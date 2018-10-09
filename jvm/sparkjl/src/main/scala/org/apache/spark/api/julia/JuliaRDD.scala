package org.apache.spark.api.julia

import java.io._
import java.net._
import sys.process.Process
import java.nio.file.Paths

import org.apache.commons.compress.utils.Charsets
import org.apache.spark._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.language.existentials
import scala.reflect.ClassTag

class AbstractJuliaRDD[T:ClassTag](
    @transient parent: RDD[_],
    command: Array[Byte]
) extends RDD[T](parent) {

  val preservePartitioning = true
  val reuseWorker = true

  override def getPartitions: Array[Partition] = firstParent.partitions

  // Note: needs to override in later versions of Spark
  // override def getNumPartitions: Int = firstParent.partitions.length

  override val partitioner: Option[Partitioner] = {
    if (preservePartitioning) firstParent.partitioner else None
  }


  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val worker: Socket = JuliaRDD.createWorker()
    // Start a thread to feed the process input from our parent's iterator
    val outputThread = new OutputThread(context, firstParent.iterator(split, context), worker, command, split)
    outputThread.start()
    // Return an iterator that read lines from the process's stdout
    val resultIterator = new InputIterator[T](context, worker, outputThread)
    new InterruptibleIterator(context, resultIterator)
  }
}


class JuliaRDD(@transient parent: RDD[_],command: Array[Byte]) extends AbstractJuliaRDD[Any](parent, command) {
  def asJavaRDD(): JavaRDD[Any] = {
    JavaRDD.fromRDD(this)
  }
}

private object SpecialLengths {
  val END_OF_DATA_SECTION = -1
  val JULIA_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
  val END_OF_STREAM = -4
  val NULL = -5
  val PAIR_TUPLE = -6
  val ARRAY_VALUE = -7
  val ARRAY_END = -8
  val INTEGER = -9
  val STRING_START = -100
}

object JuliaRDD extends Logging {

  def fromRDD[T](rdd: RDD[T], command: Array[Byte]): JuliaRDD =
    new JuliaRDD(rdd, command)

  def createWorker(): Socket = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1).map(_.toByte)))

      // Create and start the worker
      val juliaHome = sys.env.get("JULIA_HOME").getOrElse("")
      val juliaVersion = sys.env.get("JULIA_VERSION").getOrElse("v0.7")
      val juliaCommand = Paths.get(juliaHome, "julia").toString()
      val juliaPkgDir =  sys.env.get("JULIA_PKGDIR") match {
          case Some(i) => Paths.get(i, juliaVersion, "Spark").toString()
          case None => Process(juliaCommand +
            " -e println(dirname(dirname(Base.find_package(\"Spark\"))))").!!.trim
      }

      val pb = new ProcessBuilder(juliaCommand, Paths.get(juliaPkgDir, "src", "worker_runner.jl").toString())


      pb.directory(new File(SparkFiles.getRootDirectory()))
      // val workerEnv = pb.environment()
      // workerEnv.putAll(envVars)
      val worker = pb.start()

      // Redirect worker stdout and stderr
      StreamUtils.redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

      // Tell the worker our port
      val out = new OutputStreamWriter(worker.getOutputStream)
      out.write(serverSocket.getLocalPort + "\n")
      out.flush()

      // Wait for it to connect to our socket
      serverSocket.setSoTimeout(120000)
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

  def writeValueToStream[T](obj: Any, dataOut: DataOutputStream) {
    obj match {
      case arr: Array[Byte] =>
        dataOut.writeInt(arr.length)
        dataOut.write(arr)
      case tup: Tuple2[Any, Any] =>
        dataOut.writeInt(SpecialLengths.PAIR_TUPLE)
        writeValueToStream(tup._1, dataOut)
        writeValueToStream(tup._2, dataOut)
      case str: String =>
        val arr = str.getBytes(Charsets.UTF_8)
        dataOut.writeInt(-arr.length + SpecialLengths.STRING_START)
        dataOut.write(arr)
      case jac: java.util.AbstractCollection[_] =>
        writeValueToStream(jac.iterator, dataOut)
      case jit: java.util.Iterator[_] =>
        while (jit.hasNext) {
          dataOut.writeInt(SpecialLengths.ARRAY_VALUE)
          writeValueToStream(jit.next(), dataOut)
        }
        dataOut.writeInt(SpecialLengths.ARRAY_END)
      case ita: Iterable[_] =>
        writeValueToStream(ita.iterator, dataOut)
      case it: Iterator[_] =>
        while (it.hasNext) {
          dataOut.writeInt(SpecialLengths.ARRAY_VALUE)
          writeValueToStream(it.next(), dataOut)
        }
        dataOut.writeInt(SpecialLengths.ARRAY_END)
      case x: Int =>
        dataOut.writeInt(SpecialLengths.INTEGER)
        dataOut.writeLong(x)
      case x: java.lang.Long =>
        dataOut.writeInt(SpecialLengths.INTEGER)
        dataOut.writeLong(x)
      case x: java.lang.Integer =>
        dataOut.writeInt(SpecialLengths.INTEGER)
        dataOut.writeLong(x.longValue)
      case other =>
        throw new SparkException("Unexpected element type " + other.getClass)
    }
  }

  def readValueFromStream(stream: DataInputStream) : Any = {
    var typeLength = stream.readInt()
    typeLength match {
      case length if length > 0 =>
        val obj = new Array[Byte](length)
        stream.readFully(obj)
        obj
      case 0 => Array.empty[Byte]
      case SpecialLengths.PAIR_TUPLE =>
        (readValueFromStream(stream), readValueFromStream(stream))
      case SpecialLengths.JULIA_EXCEPTION_THROWN =>
        // Signals that an exception has been thrown in julia
        val exLength = stream.readInt()
        val strlength = -exLength + SpecialLengths.STRING_START
        val obj = new Array[Byte](strlength)
        stream.readFully(obj)
        val str = new String(obj, Charsets.UTF_8)
        throw new Exception(str)
      case SpecialLengths.ARRAY_VALUE =>
        val ab = new collection.mutable.ArrayBuffer[Any]()
        while(typeLength == SpecialLengths.ARRAY_VALUE) {
          ab += readValueFromStream(stream)
          typeLength = stream.readInt()
        }
        ab.toIterator
      case SpecialLengths.ARRAY_END =>
        new Array[Any](0)
      case SpecialLengths.INTEGER =>
        stream.readLong()
      case SpecialLengths.STRING_START =>
        ""
      case length if length < SpecialLengths.STRING_START =>
        val strlength = -length + SpecialLengths.STRING_START
        val obj = new Array[Byte](strlength)
        stream.readFully(obj)
        new String(obj, Charsets.UTF_8)
      case SpecialLengths.END_OF_DATA_SECTION =>
        if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
          null
        } else {
          throw new RuntimeException("Protocol error")
        }
    }

  }


  def readRDDFromFile(sc: JavaSparkContext, filename: String, parallelism: Int): JavaRDD[Any] = {
    val file = new DataInputStream(new FileInputStream(filename))
    try {
      val objs = new collection.mutable.ArrayBuffer[Any]
      try {
        while (true) {
          objs.append(readValueFromStream(file))
        }
      } catch {
        case eof: EOFException => // No-op
      }
      JavaRDD.fromRDD(sc.sc.parallelize(objs, parallelism))
    } finally {
      file.close()
    }
  }

  def cartesianSS(rdd1: JavaRDD[Any], rdd2: JavaRDD[Any]): JavaPairRDD[Any, Any] = {
    rdd1.cartesian(rdd2)
  }

  def collectToJulia(rdd: JavaRDD[Any]): Array[Byte] = {
    writeToByteArray[java.util.List[Any]](rdd.collect())
  }

  def collectToJuliaItr(rdd: JavaRDD[Any]): java.util.List[Any] = {
    return rdd.collect()
  }

  def writeToByteArray[T](obj: Any): Array[Byte] = {
    val byteArrayOut = new ByteArrayOutputStream()
    val dataStream = new DataOutputStream(byteArrayOut)
    writeValueToStream(obj, dataStream)
    dataStream.flush()
    byteArrayOut.toByteArray()
  }
}

class JuliaPairRDD(@transient parent: RDD[_],command: Array[Byte]) extends AbstractJuliaRDD[(Any, Any)](parent, command) {
  def asJavaPairRDD(): JavaPairRDD[Any, Any] = {
    JavaPairRDD.fromRDD(this)
  }
}

object JuliaPairRDD extends Logging {

  def fromRDD[T](rdd: RDD[T], command: Array[Byte]): JuliaPairRDD =
    new JuliaPairRDD(rdd, command)

  def collectToJulia(rdd: JavaPairRDD[Any, Any]): Array[Byte] = {
    JuliaRDD.writeToByteArray[java.util.List[(Any, Any)]](rdd.collect())
  }

  def collectToJuliaItr(rdd: JavaPairRDD[Any, Any]): java.util.List[(Any, Any)] = {
    return rdd.collect()
  }
}
