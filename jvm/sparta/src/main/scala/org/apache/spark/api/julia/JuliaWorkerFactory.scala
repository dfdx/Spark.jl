package org.apache.spark.api.julia

import java.io.InputStream
import java.io.OutputStreamWriter
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket

import org.apache.spark._
import org.apache.spark.util.RedirectThread

import scala.collection.JavaConversions._
import scala.collection.mutable

private[spark] class JuliaWorkerFactory(envVars: Map[String, String])
  extends Logging {


  var workers = new mutable.WeakHashMap[Socket, Process]()

  /**
   * Launch a worker by loading Sparta.Worker and telling it to connect to us.
   */
  def create(): Socket = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1).map(_.toByte)))

      // Create and start the worker
      val pb = new ProcessBuilder(Seq("julia", "-e", "using Sparta; Sparta.launch_worker()"))
      val workerEnv = pb.environment()
      workerEnv.putAll(envVars)
      val worker = pb.start()

      // Redirect worker stdout and stderr
      // TODO: why redirecting input stream?
      redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

      // Tell the worker our port
      val out = new OutputStreamWriter(worker.getOutputStream)
      out.write(serverSocket.getLocalPort + "\n")
      out.flush()

      // Wait for it to connect to our socket
      serverSocket.setSoTimeout(10000)
      try {
        val socket = serverSocket.accept()
        workers.put(socket, worker)
        return socket
      } catch {
        case e: Exception =>
          throw new SparkException("Python worker did not connect back in time", e)
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

  def stopWorker(worker: Socket) {
    workers.get(worker).foreach(_.destroy())
    worker.close()
  }

  def releaseWorker(worker: Socket) {
    // Cleanup the worker socket. This will also cause the Python worker to exit.
    try {
      worker.close()
    } catch {
      case e: Exception =>
        logWarning("Failed to close worker socket", e)
    }
  }

}
