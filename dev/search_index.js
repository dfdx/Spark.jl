var documenterSearchIndex = {"docs":
[{"location":"api/","page":"API Reference","title":"API Reference","text":"CurrentModule = Spark","category":"page"},{"location":"api/","page":"API Reference","title":"API Reference","text":"SparkSessionBuilder\nSparkSession\nRuntimeConfig\nDataFrame\nGroupedData\nColumn\nRow\nStructType\nStructField\nWindow\nWindowSpec\nDataFrameReader\nDataFrameWriter\nDataStreamReader\nDataStreamWriter\nStreamingQuery\n@chainable\nDotChainer","category":"page"},{"location":"api/#Spark.SparkSessionBuilder","page":"API Reference","title":"Spark.SparkSessionBuilder","text":"Builder for SparkSession\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.SparkSession","page":"API Reference","title":"Spark.SparkSession","text":"The entry point to programming Spark with the Dataset and DataFrame API\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.RuntimeConfig","page":"API Reference","title":"Spark.RuntimeConfig","text":"User-facing configuration API, accessible through SparkSession.conf\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.DataFrame","page":"API Reference","title":"Spark.DataFrame","text":"A distributed collection of data grouped into named columns\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.GroupedData","page":"API Reference","title":"Spark.GroupedData","text":"A set of methods for aggregations on a DataFrame, created by DataFrame.groupBy()\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.Column","page":"API Reference","title":"Spark.Column","text":"A column in a DataFrame\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.Row","page":"API Reference","title":"Spark.Row","text":"A row in DataFrame\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.StructType","page":"API Reference","title":"Spark.StructType","text":"Struct type, consisting of a list of StructField\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.StructField","page":"API Reference","title":"Spark.StructField","text":"A field in StructType\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.Window","page":"API Reference","title":"Spark.Window","text":"Utility functions for defining window in DataFrames\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.WindowSpec","page":"API Reference","title":"Spark.WindowSpec","text":"A window specification that defines the partitioning, ordering, and frame boundaries\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.DataFrameReader","page":"API Reference","title":"Spark.DataFrameReader","text":"Interface used to load a DataFrame from external storage systems\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.DataFrameWriter","page":"API Reference","title":"Spark.DataFrameWriter","text":"Interface used to write a DataFrame to external storage systems\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.DataStreamReader","page":"API Reference","title":"Spark.DataStreamReader","text":"Interface used to load a streaming DataFrame from external storage systems\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.DataStreamWriter","page":"API Reference","title":"Spark.DataStreamWriter","text":"Interface used to write a streaming DataFrame to external\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.StreamingQuery","page":"API Reference","title":"Spark.StreamingQuery","text":"A handle to a query that is executing continuously in the background as new data arrives\n\n\n\n\n\n","category":"type"},{"location":"api/#Spark.@chainable","page":"API Reference","title":"Spark.@chainable","text":"@chainable T\n\nAdds dot chaining syntax to the type, i.e. automatically translate:\n\nfoo.bar(a)\n\ninto\n\nbar(foo, a)\n\nFor single-argument functions also support implicit calls, e.g:\n\nfoo.bar.baz(a, b)\n\nis treated the same as:\n\nfoo.bar().baz(a, b)\n\nNote that @chainable works by overloading Base.getproperty(), making it impossible to customize it for T. To have more control, one may use the underlying wrapper type - DotCaller.\n\n\n\n\n\n","category":"macro"},{"location":"api/#Spark.DotChainer","page":"API Reference","title":"Spark.DotChainer","text":"DotChainer{O, Fn}\n\nSee @chainable for details.\n\n\n\n\n\n","category":"type"},{"location":"api/","page":"API Reference","title":"API Reference","text":"","category":"page"},{"location":"streaming/#Structured-Streaming","page":"Structured Streaming","title":"Structured Streaming","text":"","category":"section"},{"location":"streaming/","page":"Structured Streaming","title":"Structured Streaming","text":"Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. In this tutorial, we explore basic API of the Structured Streaming in Spark.jl. For a general introduction into the topic and more advanced examples follow the official guide and adapt Python snippets.","category":"page"},{"location":"streaming/","page":"Structured Streaming","title":"Structured Streaming","text":"Let’s say you want to maintain a running word count of text data received from a data server listening on a TCP socket. We will use Netcat to send this data:","category":"page"},{"location":"streaming/","page":"Structured Streaming","title":"Structured Streaming","text":"nc -lk 9999","category":"page"},{"location":"streaming/","page":"Structured Streaming","title":"Structured Streaming","text":"As usually, we start by creating a SparkSession:","category":"page"},{"location":"streaming/","page":"Structured Streaming","title":"Structured Streaming","text":"using Spark\n\nspark = SparkSession.\n    builder.\n    master(\"local\").\n    appName(\"StructuredNetworkWordCount\").\n    getOrCreate()","category":"page"},{"location":"streaming/","page":"Structured Streaming","title":"Structured Streaming","text":"Next, let’s create a streaming DataFrame that represents text data received from a server listening on localhost:9999, and transform the DataFrame to calculate word counts.","category":"page"},{"location":"streaming/","page":"Structured Streaming","title":"Structured Streaming","text":"# Create DataFrame representing the stream of input lines from connection to localhost:9999\nlines = spark.\n    readStream.\n    format(\"socket\").\n    option(\"host\", \"localhost\").\n    option(\"port\", 9999).\n    load()\n\n# Split the lines into words\nwords = lines.select(\n    lines.value.split(\" \").explode().alias(\"word\")\n)\n\n# Generate running word count\nwordCounts = words.groupBy(\"word\").count()","category":"page"},{"location":"streaming/","page":"Structured Streaming","title":"Structured Streaming","text":"This lines DataFrame represents an unbounded table containing the streaming text data. This table contains one column of strings named “value”, and each line in the streaming text data becomes a row in the table. Note, that this is not currently receiving any data as we are just setting up the transformation, and have not yet started it. Next, we have used two built-in SQL functions - split and explode, to split each line into multiple rows with a word each. In addition, we use the function alias to name the new column as \"word\". Finally, we have defined the wordCounts DataFrame by grouping by the unique values in the Dataset and counting them. Note that this is a streaming DataFrame which represents the running word counts of the stream.","category":"page"},{"location":"streaming/","page":"Structured Streaming","title":"Structured Streaming","text":"We have now set up the query on the streaming data. All that is left is to actually start receiving data and computing the counts. To do this, we set it up to print the complete set of counts (specified by outputMode(\"complete\")) to the console every time they are updated. And then start the streaming computation using start().","category":"page"},{"location":"streaming/","page":"Structured Streaming","title":"Structured Streaming","text":"query = wordCounts.\n    writeStream.\n    outputMode(\"complete\").\n    format(\"console\").\n    start()\n\nquery.awaitTermination()","category":"page"},{"location":"streaming/","page":"Structured Streaming","title":"Structured Streaming","text":"Now type a few lines in the Netcat terminal window and you should see output similar to this:","category":"page"},{"location":"streaming/","page":"Structured Streaming","title":"Structured Streaming","text":"julia> query.awaitTermination()\n-------------------------------------------\nBatch: 0\n-------------------------------------------\n+----+-----+\n|word|count|\n+----+-----+\n+----+-----+\n\n-------------------------------------------\nBatch: 1\n-------------------------------------------\n+------------+-----+\n|        word|count|\n+------------+-----+\n|         was|    1|\n|         for|    1|\n|   beginning|    1|\n|       Julia|    1|\n|    designed|    1|\n|         the|    1|\n|        high|    1|\n|        from|    1|\n|performance.|    1|\n+------------+-----+\n\n-------------------------------------------\nBatch: 2\n-------------------------------------------\n+------------+-----+\n|        word|count|\n+------------+-----+\n|         was|    1|\n|         for|    1|\n|   beginning|    1|\n|       Julia|    2|\n|          is|    1|\n|    designed|    1|\n|         the|    1|\n|        high|    1|\n|        from|    1|\n|       typed|    1|\n|performance.|    1|\n| dynamically|    1|\n+------------+-----+","category":"page"},{"location":"#Introduction","page":"Introduction","title":"Introduction","text":"","category":"section"},{"location":"#Overview","page":"Introduction","title":"Overview","text":"","category":"section"},{"location":"","page":"Introduction","title":"Introduction","text":"Spark.jl provides an interface to Apache Spark™ platform, including SQL / DataFrame and Structured Streaming. It closely follows the PySpark API, making it easy to translate existing Python code to Julia.","category":"page"},{"location":"","page":"Introduction","title":"Introduction","text":"Spark.jl supports multiple cluster types (in client mode), and can be considered as an analogue to PySpark or RSpark within the Julia ecosystem. It supports running within on-premise installations, as well as hosted instance such as Amazon EMR and Azure HDInsight.","category":"page"},{"location":"#Installation","page":"Introduction","title":"Installation","text":"","category":"section"},{"location":"","page":"Introduction","title":"Introduction","text":"Spark.jl requires at least JDK 8/11 and Maven to be installed and available in PATH.","category":"page"},{"location":"","page":"Introduction","title":"Introduction","text":"] add Spark","category":"page"},{"location":"","page":"Introduction","title":"Introduction","text":"To link against a specific version of Spark, also run:","category":"page"},{"location":"","page":"Introduction","title":"Introduction","text":"ENV[\"BUILD_SPARK_VERSION\"] = \"3.2.1\"   # version you need\n] build Spark","category":"page"},{"location":"#Quick-Example","page":"Introduction","title":"Quick Example","text":"","category":"section"},{"location":"","page":"Introduction","title":"Introduction","text":"Note that most types in Spark.jl support dot notation for calling functions, e.g. x.foo(y) is expanded into foo(x, y).","category":"page"},{"location":"","page":"Introduction","title":"Introduction","text":"using Spark.SQL\n\nspark = SparkSession.builder.appName(\"Main\").master(\"local\").getOrCreate()\ndf = spark.createDataFrame([[\"Alice\", 19], [\"Bob\", 23]], \"name string, age long\")\nrows = df.select(Column(\"age\") + 1).collect()\nfor row in rows\n    println(row[1])\nend","category":"page"},{"location":"#Cluster-Types","page":"Introduction","title":"Cluster Types","text":"","category":"section"},{"location":"","page":"Introduction","title":"Introduction","text":"This package supports multiple cluster types (in client mode): local, standalone, mesos and yarn. The location of the cluster (in case of mesos or standalone) or the cluster type (in case of local or yarn) must be passed as a parameter master when creating a Spark context. For YARN based clusters, the cluster parameters are picked up from spark-defaults.conf, which must be accessible via a SPARK_HOME environment variable.","category":"page"},{"location":"#Current-Limitations","page":"Introduction","title":"Current Limitations","text":"","category":"section"},{"location":"","page":"Introduction","title":"Introduction","text":"Jobs can be submitted from Julia process attached to the cluster in client deploy mode. Cluster mode is not fully supported, and it is uncertain if it is useful in the Julia context.\nSince records are serialised between Java and Julia at the edges, the maximum size of a single row in an RDD is 2GB, due to Java array indices being limited to 32 bits.","category":"page"},{"location":"#Trademarks","page":"Introduction","title":"Trademarks","text":"","category":"section"},{"location":"","page":"Introduction","title":"Introduction","text":"Apache®, Apache Spark and Spark are registered trademarks, or trademarks of the Apache Software Foundation in the United States and/or other countries.","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"CurrentModule = Spark","category":"page"},{"location":"sql/#SQL-/-DataFrames","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"","category":"section"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"This is a quick introduction into the Spark.jl core functions. It closely follows the official PySpark tutorial and copies many examples verbatim. In most cases, PySpark docs should work for Spark.jl as is or with little adaptation.","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"Spark.jl applications usually start by creating a SparkSession:","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"using Spark\n\nspark = SparkSession.builder.appName(\"Main\").master(\"local\").getOrCreate()","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"Note that here we use dot notation to chain function invocations. This makes the code more concise and also mimics Python API, making translation of examples easier. The same example could also be written as:","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"using Spark\nimport Spark: appName, master, getOrCreate\n\nbuilder = SparkSession.builder\nbuilder = appName(builder, \"Main\")\nbuilder = master(builder, \"local\")\nspark = getOrCreate(builder)","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"See @chainable for the details of the dot notation.","category":"page"},{"location":"sql/#DataFrame-Creation","page":"SQL / DataFrames","title":"DataFrame Creation","text":"","category":"section"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"In simple cases, a Spark DataFrame can be created via SparkSession.createDataFrame. E.g. from a list of rows:","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"using Spark                                 # hide\nspark = SparkSession.builder.getOrCreate()  # hide\n\nusing Dates\n\ndf = spark.createDataFrame([\n    Row(a=1, b=2.0, c=\"string1\", d=Date(2000, 1, 1), e=DateTime(2000, 1, 1, 12, 0)),\n    Row(a=2, b=3.0, c=\"string2\", d=Date(2000, 2, 1), e=DateTime(2000, 1, 2, 12, 0)),\n    Row(a=4, b=5.0, c=\"string3\", d=Date(2000, 3, 1), e=DateTime(2000, 1, 3, 12, 0))\n])\nprintln(df)","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"Or using an explicit schema:","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df = spark.createDataFrame([\n    [1, 2.0, \"string1\", Date(2000, 1, 1), DateTime(2000, 1, 1, 12, 0)],\n    [2, 3.0, \"string2\", Date(2000, 2, 1), DateTime(2000, 1, 2, 12, 0)],\n    [3, 4.0, \"string3\", Date(2000, 3, 1), DateTime(2000, 1, 3, 12, 0)]\n], \"a long, b double, c string, d date, e timestamp\")\nprintln(df)","category":"page"},{"location":"sql/#Viewing-Data","page":"SQL / DataFrames","title":"Viewing Data","text":"","category":"section"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"The top rows of a DataFrame can be displayed using DataFrame.show():","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.show(1)","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"You can see the DataFrame’s schema and column names as follows:","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.columns()","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.printSchema()","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"Show the summary of the DataFrame","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.select(\"a\", \"b\", \"c\").describe().show()","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"DataFrame.collect() collects the distributed data to the driver side as the local data in Julia. Note that this can throw an out-of-memory error when the dataset is too large to fit in the driver side because it collects all the data from executors to the driver side.","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.collect()","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"In order to avoid throwing an out-of-memory exception, use take() or tail().","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.take(1)","category":"page"},{"location":"sql/#Selecting-and-Accessing-Data","page":"SQL / DataFrames","title":"Selecting and Accessing Data","text":"","category":"section"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"Spark.jl DataFrame is lazily evaluated and simply selecting a column does not trigger the computation but it returns a Column instance.","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.a","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"In fact, most of column-wise operations return Columns.","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"typeof(df.c) == typeof(df.c.upper()) == typeof(df.c.isNull())","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"These Columns can be used to select the columns from a DataFrame. For example, select() takes the Column instances that returns another DataFrame.","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.select(df.c).show()","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"Assign new Column instance.","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.withColumn(\"upper_c\", df.c.upper()).show()","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"To select a subset of rows, use filter() (a.k.a. where()).","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.filter(df.a == 1).show()","category":"page"},{"location":"sql/#Grouping-Data","page":"SQL / DataFrames","title":"Grouping Data","text":"","category":"section"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"Spark.jl DataFrame also provides a way of handling grouped data by using the common approach, split-apply-combine strategy. It groups the data by a certain condition applies a function to each group and then combines them back to the DataFrame.","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"using Spark   # hide\nspark = SparkSession.builder.appName(\"Main\").master(\"local\").getOrCreate()  # hide\n\ndf = spark.createDataFrame([\n    [\"red\", \"banana\", 1, 10], [\"blue\", \"banana\", 2, 20], [\"red\", \"carrot\", 3, 30],\n    [\"blue\", \"grape\", 4, 40], [\"red\", \"carrot\", 5, 50], [\"black\", \"carrot\", 6, 60],\n    [\"red\", \"banana\", 7, 70], [\"red\", \"grape\", 8, 80]], [\"color string\", \"fruit string\", \"v1 long\", \"v2 long\"])\ndf.show()","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"Grouping and then applying the avg() function to the resulting groups.","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.groupby(\"color\").avg().show()","category":"page"},{"location":"sql/#Getting-Data-in/out","page":"SQL / DataFrames","title":"Getting Data in/out","text":"","category":"section"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"Spark.jl can read and write a variety of data formats. Here's a few examples.","category":"page"},{"location":"sql/#CSV","page":"SQL / DataFrames","title":"CSV","text":"","category":"section"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.write.option(\"header\", true).csv(\"data/fruits.csv\")\nspark.read.option(\"header\", true).csv(\"data/fruits.csv\")","category":"page"},{"location":"sql/#Parquet","page":"SQL / DataFrames","title":"Parquet","text":"","category":"section"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.write.parquet(\"data/fruits.parquet\")\nspark.read.parquet(\"data/fruits.parquet\")","category":"page"},{"location":"sql/#ORC","page":"SQL / DataFrames","title":"ORC","text":"","category":"section"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.write.orc(\"data/fruits.orc\")\nspark.read.orc(\"data/fruits.orc\")","category":"page"},{"location":"sql/#Working-with-SQL","page":"SQL / DataFrames","title":"Working with SQL","text":"","category":"section"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"DataFrame and Spark SQL share the same execution engine so they can be interchangeably used seamlessly. For example, you can register the DataFrame as a table and run a SQL easily as below:","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"df.createOrReplaceTempView(\"tableA\")\nspark.sql(\"SELECT count(*) from tableA\").show()","category":"page"},{"location":"sql/","page":"SQL / DataFrames","title":"SQL / DataFrames","text":"spark.sql(\"SELECT fruit, sum(v1) as s FROM tableA GROUP BY fruit ORDER BY s\").show()","category":"page"}]
}
