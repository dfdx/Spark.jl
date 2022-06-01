```@meta
CurrentModule = Spark
```

# SQL / DataFrames

This is a quick introduction into the Spark.jl core functions. It closely follows the official [PySpark tutorial](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html) and copies many examples verbatim. In most cases, PySpark docs should work for Spark.jl as is or with little adaptation.

Spark.jl applications usually start by creating a `SparkSession`:

```@example
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

```@example df
using Spark                                 # hide
spark = SparkSession.builder.getOrCreate()  # hide

using Dates

df = spark.createDataFrame([
    Row(a=1, b=2.0, c="string1", d=Date(2000, 1, 1), e=DateTime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3.0, c="string2", d=Date(2000, 2, 1), e=DateTime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5.0, c="string3", d=Date(2000, 3, 1), e=DateTime(2000, 1, 3, 12, 0))
])
println(df)
```
Or using an explicit schema:

```@example df
df = spark.createDataFrame([
    [1, 2.0, "string1", Date(2000, 1, 1), DateTime(2000, 1, 1, 12, 0)],
    [2, 3.0, "string2", Date(2000, 2, 1), DateTime(2000, 1, 2, 12, 0)],
    [3, 4.0, "string3", Date(2000, 3, 1), DateTime(2000, 1, 3, 12, 0)]
], "a long, b double, c string, d date, e timestamp")
println(df)
```


## Viewing Data

The top rows of a DataFrame can be displayed using `DataFrame.show()`:

```@example df
df.show(1)
```

You can see the DataFrame’s schema and column names as follows:

```@example df
df.columns()
```

```@example df
df.printSchema()
```

Show the summary of the DataFrame

```@example df
df.select("a", "b", "c").describe().show()
```

`DataFrame.collect()` collects the distributed data to the driver side as the local data in Julia. Note that this can throw an out-of-memory error when the dataset is too large to fit in the driver side because it collects all the data from executors to the driver side.

```@example df
df.collect()
```

In order to avoid throwing an out-of-memory exception, use `take()` or `tail()`.

```@example df
df.take(1)
```

## Selecting and Accessing Data

Spark.jl `DataFrame` is lazily evaluated and simply selecting a column does not trigger the computation but it returns a `Column` instance.

```@example df
df.a
```

In fact, most of column-wise operations return `Column`s.

```@example df
typeof(df.c) == typeof(df.c.upper()) == typeof(df.c.isNull())
```

These `Column`s can be used to select the columns from a `DataFrame`. For example, `select()` takes the `Column` instances that returns another `DataFrame`.

```@example df
df.select(df.c).show()
```

Assign new Column instance.

```@example df
df.withColumn("upper_c", df.c.upper()).show()
```

To select a subset of rows, use `filter()` (a.k.a. `where()`).

```@example df
df.filter(df.a == 1).show()
```

## Grouping Data

Spark.jl `DataFrame` also provides a way of handling grouped data by using the common approach, split-apply-combine strategy. It groups the data by a certain condition applies a function to each group and then combines them back to the `DataFrame`.

```@example gdf
using Spark   # hide
spark = SparkSession.builder.appName("Main").master("local").getOrCreate()  # hide

df = spark.createDataFrame([
    ["red", "banana", 1, 10], ["blue", "banana", 2, 20], ["red", "carrot", 3, 30],
    ["blue", "grape", 4, 40], ["red", "carrot", 5, 50], ["black", "carrot", 6, 60],
    ["red", "banana", 7, 70], ["red", "grape", 8, 80]], ["color string", "fruit string", "v1 long", "v2 long"])
df.show()
```

Grouping and then applying the `avg()` function to the resulting groups.

```@example gdf
df.groupby("color").avg().show()
```

## Getting Data in/out

Spark.jl can read and write a variety of data formats. Here's a few examples.

### CSV

```@example gdf
df.write.option("header", true).csv("data/fruits.csv")
spark.read.option("header", true).csv("data/fruits.csv")
```

### Parquet

```@example gdf
df.write.parquet("data/fruits.parquet")
spark.read.parquet("data/fruits.parquet")
```

### ORC

```@example gdf
df.write.orc("data/fruits.orc")
spark.read.orc("data/fruits.orc")
```

## Working with SQL

`DataFrame` and Spark SQL share the same execution engine so they can be interchangeably used seamlessly. For example, you can register the `DataFrame` as a table and run a SQL easily as below:

```@example gdf
df.createOrReplaceTempView("tableA")
spark.sql("SELECT count(*) from tableA").show()
```

```@example gdf
spark.sql("SELECT fruit, sum(v1) as s FROM tableA GROUP BY fruit ORDER BY s").show()
```