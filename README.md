# Spark.jl

[![Build Status](https://travis-ci.org/dfdx/Spark.jl.svg?branch=master)](https://travis-ci.org/dfdx/Spark.jl)
[![Build status](https://ci.appveyor.com/api/projects/status/vf5w4l37icc8m35q?svg=true)](https://ci.appveyor.com/project/dfdx/spark-jl)

Julia interface to Apache Spark. 

See [Roadmap](https://github.com/dfdx/Spark.jl/issues/1) for current status.

## Installation

Spark.jl requires at least Java 7 and [Maven](https://maven.apache.org/) to be installed and available in `PATH`.

```
Pkg.clone("https://github.com/dfdx/Spark.jl")
Pkg.build("Spark")
# we also need latest master of JavaCall.jl
Pkg.checkout("JavaCall")

```

This will download and build all Julia and Java dependencies. To use Spark.jl type:

```
using Spark
```

## Examples

All examples below are runnable from REPL

#### Count lines in a text file

```
sc = SparkContext(master="local")
path = "file:///var/log/syslog"
txt = text_file(sc, path)
count(txt)
close(sc)
```

#### Map / Reduce on Standalone master, application name

```
sc = SparkContext(master="spark://spark-standalone:7077", appname="Say 'Hello!'")
path = "file:///var/log/syslog"
txt = text_file(sc, path)
rdd = map(txt, line -> length(split(line)))
reduce(rdd, +)
close(sc)
```

**NOTE:** currently named Julia functions cannot be fully serialized, so functions passed to executors should be either already defined there (e.g. in preinstalled library) or be anonymous functions. 

#### Map partitions on Mesos and HDFS

```
sc = SparkContext(master="mesos://mesos-master:5050")
path = "hdfs://namenode:8020/user/hdfs/test.log"
txt = text_file(sc, path)
rdd = map_partitions(txt, it -> filter(line -> contains(line, "a"), it))
collect(rdd)
close(sc)
```

For the full supported API see [the list of exported functions](https://github.com/dfdx/Spark.jl/blob/master/src/Spark.jl#L3).
