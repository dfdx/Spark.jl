# Spark.jl

[![Build Status](https://travis-ci.org/dfdx/Spark.jl.svg?branch=master)](https://travis-ci.org/dfdx/Spark.jl)

Julia interface to Apache Spark. 

See [Roadmap](https://github.com/dfdx/Spark.jl/issues/1) for current status.

## Installation

Spark.jl requires at least Java 7 and [Maven](https://maven.apache.org/) to be installed and available in `PATH`.

```
Pkg.checkout("https://github.com/dfdx/Spark.jl")
```

This will download and build all Julia and Java dependencies. To use Spark.jl type:

```
using Spark
```

## Examples

All examples below are runnable from REPL

### Count lines in a text file

```
sc = SparkContext()
path = "file:///var/log/syslog"
txt = text_file(sc, path)
count(txt)
reduce(rdd, +)
close(sc)
```

### Map / Reduce on Standalone master and HDFS

```
sc = SparkContext(master="spark://spark-standalone:7077")
path = "hdfs://namenode:8020/user/hdfs/test.log"
txt = text_file(sc, path)
rdd = map(txt, line -> length(split(line)))
reduce(rdd, +)
close(sc)
```

**NOTE:** currently named Julia functions cannot be fully serialized, so functions passed to executors should be either already defined there (e.g. in preinstalled library) or be anonymous functions. 

### Map partitions on Mesos

```
sc = SparkContext(master="mesos://mesos-master:5050")
path = "hdfs://namenode:8020/user/hdfs/test.log"
txt = text_file(sc, path)
rdd = map_partitions(txt, it -> map(s -> length(split(s)), it))
reduce(rdd, +)
close(sc)
```

