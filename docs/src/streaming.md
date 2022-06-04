# Structured Streaming

Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. In this tutorial, we explore basic API of the Structured Streaming in Spark.jl. For a general introduction into the topic and more advanced examples follow the [official guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) and adapt Python snippets.

Let’s say you want to maintain a running word count of text data received from a data server listening on a TCP socket. We will use Netcat to send this data:

```
nc -lk 9999
```

As usually, we start by creating a SparkSession:

```@example basic
using Spark

spark = SparkSession.
    builder.
    master("local").
    appName("StructuredNetworkWordCount").
    getOrCreate()
```

Next, let’s create a streaming DataFrame that represents text data received from a server listening on localhost:9999, and transform the DataFrame to calculate word counts.

```@example basic
# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark.
    readStream.
    format("socket").
    option("host", "localhost").
    option("port", 9999).
    load()

# Split the lines into words
words = lines.select(
    lines.value.split(" ").explode().alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()
```

This `lines` DataFrame represents an unbounded table containing the streaming text data. This table contains one column of strings named “value”, and each line in the streaming text data becomes a row in the table. Note, that this is not currently receiving any data as we are just setting up the transformation, and have not yet started it. Next, we have used two built-in SQL functions - `split` and `explode`, to split each line into multiple rows with a word each. In addition, we use the function `alias` to name the new column as "word". Finally, we have defined the `wordCounts` DataFrame by grouping by the unique values in the Dataset and counting them. Note that this is a streaming DataFrame which represents the running word counts of the stream.

We have now set up the query on the streaming data. All that is left is to actually start receiving data and computing the counts. To do this, we set it up to print the complete set of counts (specified by `outputMode("complete"))` to the console every time they are updated. And then start the streaming computation using `start()`.

```julia
query = wordCounts.
    writeStream.
    outputMode("complete").
    format("console").
    start()

query.awaitTermination()
```

Now type a few lines in the Netcat terminal window and you should see output similar to this:

```julia
julia> query.awaitTermination()
-------------------------------------------
Batch: 0
-------------------------------------------
+----+-----+
|word|count|
+----+-----+
+----+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+------------+-----+
|        word|count|
+------------+-----+
|         was|    1|
|         for|    1|
|   beginning|    1|
|       Julia|    1|
|    designed|    1|
|         the|    1|
|        high|    1|
|        from|    1|
|performance.|    1|
+------------+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+------------+-----+
|        word|count|
+------------+-----+
|         was|    1|
|         for|    1|
|   beginning|    1|
|       Julia|    2|
|          is|    1|
|    designed|    1|
|         the|    1|
|        high|    1|
|        from|    1|
|       typed|    1|
|performance.|    1|
| dynamically|    1|
+------------+-----+
```