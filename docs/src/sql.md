```@meta
CurrentModule = Spark
```

# SQL / DataFrames

This is a quick introduction into the Spark.jl core functions. It closely follows the official [PySpark tutorial](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html) and copies many examples verbatim. In most cases, PySpark docs should work for Spark.jl as is or with little adaptation.

Spark.jl applications usually start by creating a `SparkSession`:

```julia
using Spark

spark = SparkSession.builder.appName("Main").master("local").getOrCreate()
```

Note that here we use dot notation to chain function invocations. This makes the code more concise and also mimics Python API, making translation of examples easier. The same example could also be written as:

```julia
using Spark
import Spark: appName, master, getOrCreate

builder = SparkSession.builder
builder = appName(builder, "Main")
builder = master(builder, "local")
spark = getOrCreate(builder)
```

See [`@chainable`](@ref) for the details of the dot notation.


## DataFrame Creation


In simple cases, a Spark DataFrame can be created via `SparkSession.createDataFrame`. E.g. from a list of rows:

```julia
using Dates

df = spark.createDataFrame([
    Row(a=1, b=2.0, c="string1", d=Date(2000, 1, 1), e=DateTime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3.0, c="string2", d=Date(2000, 2, 1), e=DateTime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5.0, c="string3", d=Date(2000, 3, 1), e=DateTime(2000, 1, 3, 12, 0))
])

```

