package org.apache.spark.api.julia

import java.io.InputStream

import org.apache.spark.internal.Logging
import org.apache.spark.util.RedirectThread


object StreamUtils extends Logging {

  /**
   * Redirect the given streams to our stderr in separate threads.
   */
  def redirectStreamsToStderr(stdout: InputStream, stderr: InputStream) {
    try {
      new RedirectThread(stdout, System.err, "stdout reader for julia").start()
      new RedirectThread(stderr, System.err, "stderr reader for julia").start()
    } catch {
      case e: Exception =>
        logError("Exception in redirecting streams", e)
    }
  }

}
