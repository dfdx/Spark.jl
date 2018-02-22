# Introduction

## Overview

Spark.jl is the package that allows the execution of Julia programs on the Apache Spark™ platform. It supports running pure Julia scripts on Julia data structures, while utilising the data and code distribution capabalities of Apache Spark. It supports multiple cluster types (in client mode), and can be consider as an analogue to PySpark or RSpark within the Julia ecosystem. It supports running within on-premise installations, as well as hosted instance such as Amazon EMR and Azure HDInsight. 

### Installation

Spark.jl requires at least Java 7 and Maven to be installed and available in PATH. 

```
Pkg.add("Spark.jl")
```

To link against a specific version of Spark, also run:

```
ENV["BUILD_SPARK_VERSION"] = "2.2.1"   # version you need
Pkg.build("Spark")
```

### Basic Usage

The `Spark.init()` method must to called at the beginning of a session to initialise the JVM. Subsequently a `SparkContext` is created to serve as the primary reference to a Spark instance.  

```
using Spark
Spark.init()
sc = SparkContext(master="local")
```

### Cluster Tyes

This package supports multiple cluster types (in client mode): `local`, `standalone`, `mesos` and `yarn`. The location of the cluster (in case of mesos or standalone) or the cluster type (in case of local or yarn) must be passed as a parameter `master` when creating a Spark context. For YARN based clusters, the cluster parameters are picked up from `spark-defaults.conf`, which must be accessible via a `SPARK_HOME` environment variable. 

## RDD Interface

The primary interface exposed vis this package is the Spark RDD object. RDD's may be created from any Julia iterator via the `parallelize` method. Alternatively, the `text_file` method may be used to read data from any Spark supported filesystem, such as `HDFS`

Julia functions are passed as parameters to the various Spark operations. These functions must either be anonymous functions defined inline within the spark call, or they must be available on all nodes. Functions may be made available by installing Julia packages on all nodes, or via the `@attach` macro that will make any julia script file available on all the workder nodes. 

### Example: Count lines in file

```
sc = SparkContext(master="local")
path = "file:///var/log/syslog"
txt = text_file(sc, path)
count(txt)
close(sc)
```

### Example: Map / Reduce on Standalone master

```
sc = SparkContext(master="spark://spark-standalone:7077", appname="Say 'Hello!'")
path = "file:///var/log/syslog"
txt = text_file(sc, path)
rdd = map(txt, line -> length(split(line)))
reduce(rdd, +)
close(sc)
```

### Example: Map partitions on Mesos and HDFS

```
sc = SparkContext(master="mesos://mesos-master:5050")
path = "hdfs://namenode:8020/user/hdfs/test.log"
txt = text_file(sc, path)
rdd = map_partitions(txt, it -> filter(line -> contains(line, "a"), it))
collect(rdd)
close(sc)
```

## SQL Interface

A recent addition to this package is a DataFrame+SQL interface to Spark. In the examples below, it is assumed that you have a file people.json with content like this:

```
{"name": "Alice", "age": 27}
{"name": "Bob", "age": 32}
```

### Example: Read dataframe from JSON and collect to a driver

```
spark = SparkSession()
df = read_json(spark, "/path/to/people.json")
collect(df)
```


### Example: Read JSON and write Parquet

```
spark = SparkSession()
df = read_json(spark, "/path/to/people.json")
write_parquet(df, "/path/to/people.parquet")
```

## Current Limitations

* Jobs can be submitted from Julia process attached to the cluster in `client` deploy mode. `Cluster` mode is not fully supported, and it is uncertain if it is useful in the Julia context. 
* Since records are serialised between Java and Julia at the edges, the maximum size of a single row in an RDD is 2GB, due to Java array indices being limited to 32 bits. 

## Trademarks

Apache®, [Apache Spark and Spark](http://spark.apache.org) are registered trademarks, or trademarks of the [Apache Software Foundation](http://www.apache.org/) in the United States and/or other countries.
