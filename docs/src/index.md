# Introduction

## Overview

Spark.jl provides an interface to Apache Spark™ platform, including SQL / DataFrame and Structured Streaming. It closely follows the PySpark API, making it easy to translate existing Python code to Julia.

Spark.jl supports multiple cluster types (in client mode), and can be considered as an analogue to PySpark or RSpark within the Julia ecosystem. It supports running within on-premise installations, as well as hosted instance such as Amazon EMR and Azure HDInsight.

### Installation

Spark.jl requires at least JDK 8/11 and Maven to be installed and available in PATH.

```julia
Pkg.add("Spark.jl")
```

To link against a specific version of Spark, also run:

```julia
ENV["BUILD_SPARK_VERSION"] = "3.2.1"   # version you need
Pkg.build("Spark")
```

### Quick Example

Note that most types in Spark.jl support dot notation for calling functions, e.g. `x.foo(y)` is expanded into `foo(x, y)`.

```julia
using Spark.SQL

spark = SparkSession.builder.appName("Main").master("local").getOrCreate()
df = spark.createDataFrame([["Alice", 19], ["Bob", 23]], "name string, age long")
rows = df.select(Column("age") + 1).collect()
for row in rows
    println(row[1])
end
```

### Cluster Types

This package supports multiple cluster types (in client mode): `local`, `standalone`, `mesos` and `yarn`. The location of the cluster (in case of mesos or standalone) or the cluster type (in case of local or yarn) must be passed as a parameter `master` when creating a Spark context. For YARN based clusters, the cluster parameters are picked up from `spark-defaults.conf`, which must be accessible via a `SPARK_HOME` environment variable.

## Current Limitations

* Jobs can be submitted from Julia process attached to the cluster in `client` deploy mode. `Cluster` mode is not fully supported, and it is uncertain if it is useful in the Julia context.
* Since records are serialised between Java and Julia at the edges, the maximum size of a single row in an RDD is 2GB, due to Java array indices being limited to 32 bits.

## Trademarks

Apache®, [Apache Spark and Spark](http://spark.apache.org) are registered trademarks, or trademarks of the [Apache Software Foundation](http://www.apache.org/) in the United States and/or other countries.
