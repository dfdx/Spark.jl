/**
* A simple scala class that can be used along with spark-submit to
* submit a Julia script to be run in a spark cluster. E.g.:
*
* $ spark-submit --class org.julialang.juliaparallel.SparkSubmitJulia \
*       --master yarn \
*       --deploy-mode cluster \
*       --driver-memory 4g \
*       --executor-memory 2g \
*       --executor-cores 1 \
*       spark-julia_2.11-1.0.jar \
*       /opt/julia/depot/helloworld.jl \
*       /usr/local/julia/bin/julia \
*       /opt/julia/depot
*
* To compile, use `src/main/scala/SparkSubmitJulia.scala` with a build.sbt like:
* ---------------------
* name := "Spark Submit Julia"
* version := "1.0"
* scalaVersion := "2.11.8"
* libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.4"
* ---------------------
*/
package org.julialang.juliaparallel

import scala.sys.process._
import org.apache.spark.sql.SparkSession

object SparkSubmitJulia {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Submit Julia")
      .getOrCreate()
    val script = args(0) // e.g.: "/opt/julia/depot/helloworld.jl"
    val juliapath = args(1) // e.g.: "/usr/local/julia/bin/julia"
    val juliadepotpath = args(2)  // e.g.: "/opt/julia/depot"
    val exitcode = Process(Seq(juliapath, script), None, "JULIA_DEPOT_PATH" -> juliadepotpath).!
    println(s"Completed with exitcode $exitcode")
    spark.stop()
  }
}
