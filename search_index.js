var documenterSearchIndex = {"docs": [

{
    "location": "index.html#",
    "page": "Introduction",
    "title": "Introduction",
    "category": "page",
    "text": ""
},

{
    "location": "index.html#Introduction-1",
    "page": "Introduction",
    "title": "Introduction",
    "category": "section",
    "text": ""
},

{
    "location": "index.html#Overview-1",
    "page": "Introduction",
    "title": "Overview",
    "category": "section",
    "text": "Spark.jl is the package to allow the execution of Julia programs on the Apache Spark platform. It supports running pure Julia scripts on Julia data structures, while utilising the data and code distribution capabalities of Apache Spark. It supports multiple cluster types (in client mode), and can be consider as an analogue to PySpark or RSpark within the Julia ecosystem. It supports running within on-premise installations, as well as hosted instance such as Amazon EMR and Azure HDInsight. "
},

{
    "location": "index.html#Installation-1",
    "page": "Introduction",
    "title": "Installation",
    "category": "section",
    "text": "Spark.jl requires at least Java 7 and Maven to be installed and available in PATH. Pkg.add(\"Spark.jl\")"
},

{
    "location": "index.html#Basic-Usage-1",
    "page": "Introduction",
    "title": "Basic Usage",
    "category": "section",
    "text": "The Spark.init() method must to called at the beginning of a session to initialie the JVM. Subsequently a SparkContext is created to serve as the primary reference to a Spark instance.  using Spark\nSpark.init()\nsc = SparkContext(master=\"local\")"
},

{
    "location": "index.html#Cluster-Tyes-1",
    "page": "Introduction",
    "title": "Cluster Tyes",
    "category": "section",
    "text": "This package supports multiple cluster types: local, standalone, mesos and yarn. The location of the cluster (in case of mesos or standalone) or the cluster type (in case of local or yarn) must be passed as a parameter master when creating a Spark context. For YARN based clusters, the cluster parameters are picked up from spark-defaults.conf, which must be accessible via a SPARK_HOME environment variable. "
},

{
    "location": "index.html#RDD-Interface-1",
    "page": "Introduction",
    "title": "RDD Interface",
    "category": "section",
    "text": "The primary interface exposed vis this package is the Spark RDD object. RDD's may be created from any Julia iterator via the parallelize method. Alternatively, the text_file method may be used to read data from any Spark supported filesystem, such as HDFSJulia functions are passed as parameters to the various Spark operations. These functions must either be anonymous functions defined inline within the spark call, or they must be available on all nodes. Functions may be made available by installing Julia packages on all nodes, or via the @attach macro that will make any julia script file available on all the workder nodes. "
},

{
    "location": "index.html#Example:-Count-lines-in-file-1",
    "page": "Introduction",
    "title": "Example: Count lines in file",
    "category": "section",
    "text": "sc = SparkContext(master=\"local\")\npath = \"file:///var/log/syslog\"\ntxt = text_file(sc, path)\ncount(txt)\nclose(sc)"
},

{
    "location": "index.html#Example:-Map-/-Reduce-on-Standalone-master-1",
    "page": "Introduction",
    "title": "Example: Map / Reduce on Standalone master",
    "category": "section",
    "text": "sc = SparkContext(master=\"spark://spark-standalone:7077\", appname=\"Say 'Hello!'\")\npath = \"file:///var/log/syslog\"\ntxt = text_file(sc, path)\nrdd = map(txt, line -> length(split(line)))\nreduce(rdd, +)\nclose(sc)"
},

{
    "location": "index.html#Example:-Map-partitions-on-Mesos-and-HDFS-1",
    "page": "Introduction",
    "title": "Example: Map partitions on Mesos and HDFS",
    "category": "section",
    "text": "sc = SparkContext(master=\"mesos://mesos-master:5050\")\npath = \"hdfs://namenode:8020/user/hdfs/test.log\"\ntxt = text_file(sc, path)\nrdd = map_partitions(txt, it -> filter(line -> contains(line, \"a\"), it))\ncollect(rdd)\nclose(sc)"
},

{
    "location": "index.html#SQL-Interface-1",
    "page": "Introduction",
    "title": "SQL Interface",
    "category": "section",
    "text": "A recent addition to this package is a DataFrame+SQL interface to Spark. In the examples below, it is assumed that you have a file people.json with content like this:{\"name\": \"Alice\", \"age\": 27}\n{\"name\": \"Bob\", \"age\": 32}"
},

{
    "location": "index.html#Example:-Read-dataframe-from-JSON-and-collect-to-a-driver-1",
    "page": "Introduction",
    "title": "Example: Read dataframe from JSON and collect to a driver",
    "category": "section",
    "text": "spark = SparkSession()\ndf = read_json(spark, \"/path/to/people.json\")\ncollect(df)"
},

{
    "location": "index.html#Example:-Read-JSON-and-write-Parquet-1",
    "page": "Introduction",
    "title": "Example: Read JSON and write Parquet",
    "category": "section",
    "text": "spark = SparkSession()\ndf = read_json(spark, \"/path/to/people.json\")\nwrite_parquet(df, \"/path/to/people.parquet\")"
},

{
    "location": "api.html#Spark.JavaRDD",
    "page": "API Reference",
    "title": "Spark.JavaRDD",
    "category": "Type",
    "text": "Pure wrapper around JavaRDD\n\n\n\n"
},

{
    "location": "api.html#Spark.SparkContext",
    "page": "API Reference",
    "title": "Spark.SparkContext",
    "category": "Type",
    "text": "Wrapper around JavaSparkContext\n\n\n\n"
},

{
    "location": "api.html#Spark.SparkContext-Tuple{}",
    "page": "API Reference",
    "title": "Spark.SparkContext",
    "category": "Method",
    "text": "Params:\n\nmaster - address of application master. Currently only local and standalone modes          are supported. Default is 'local'\nappname - name of application\n\n\n\n"
},

{
    "location": "api.html#Base.close-Tuple{Spark.SparkContext}",
    "page": "API Reference",
    "title": "Base.close",
    "category": "Method",
    "text": "Close SparkContext\n\n\n\n"
},

{
    "location": "api.html#Base.collect-Tuple{Spark.PairRDD}",
    "page": "API Reference",
    "title": "Base.collect",
    "category": "Method",
    "text": "Collect all elements of rdd on a driver machine\n\n\n\n"
},

{
    "location": "api.html#Base.collect-Tuple{Spark.SingleRDD}",
    "page": "API Reference",
    "title": "Base.collect",
    "category": "Method",
    "text": "Collect all elements of rdd on a driver machine\n\n\n\n"
},

{
    "location": "api.html#Base.count-Tuple{Spark.RDD}",
    "page": "API Reference",
    "title": "Base.count",
    "category": "Method",
    "text": "Count number of elements in this RDD\n\n\n\n"
},

{
    "location": "api.html#Base.map-Tuple{Spark.RDD,Function}",
    "page": "API Reference",
    "title": "Base.map",
    "category": "Method",
    "text": "Apply function f to each element of rdd\n\n\n\n"
},

{
    "location": "api.html#Base.reduce-Tuple{Spark.RDD,Function}",
    "page": "API Reference",
    "title": "Base.reduce",
    "category": "Method",
    "text": "Reduce elements of rdd using specified function f\n\n\n\n"
},

{
    "location": "api.html#Spark.cache-Tuple{Spark.PairRDD}",
    "page": "API Reference",
    "title": "Spark.cache",
    "category": "Method",
    "text": "Persist this RDD with the default storage level (MEMORY_ONLY)\n\n\n\n"
},

{
    "location": "api.html#Spark.cache-Tuple{Spark.SingleRDD}",
    "page": "API Reference",
    "title": "Spark.cache",
    "category": "Method",
    "text": "Persist this RDD with the default storage level (MEMORY_ONLY)\n\n\n\n"
},

{
    "location": "api.html#Spark.cartesian-Tuple{Spark.SingleRDD,Spark.SingleRDD}",
    "page": "API Reference",
    "title": "Spark.cartesian",
    "category": "Method",
    "text": "Create a pair RDD with every combination of the values of rdd1 and rdd2\n\n\n\n"
},

{
    "location": "api.html#Spark.coalesce-Tuple{T<:Spark.RDD,Integer}",
    "page": "API Reference",
    "title": "Spark.coalesce",
    "category": "Method",
    "text": "Return a new RDD that is reduced into num_partitions partitions.\n\n\n\n"
},

{
    "location": "api.html#Spark.flat_map-Tuple{Spark.RDD,Function}",
    "page": "API Reference",
    "title": "Spark.flat_map",
    "category": "Method",
    "text": "Similar to map, but each input item can be mapped to 0 or more output items (so f should return an iterator rather than a single item)\n\n\n\n"
},

{
    "location": "api.html#Spark.flat_map_pair-Tuple{Spark.RDD,Function}",
    "page": "API Reference",
    "title": "Spark.flat_map_pair",
    "category": "Method",
    "text": "Similar to map, but each input item can be mapped to 0 or more output items (so f should return an iterator of pairs rather than a single item)\n\n\n\n"
},

{
    "location": "api.html#Spark.group_by_key-Tuple{Spark.PairRDD}",
    "page": "API Reference",
    "title": "Spark.group_by_key",
    "category": "Method",
    "text": "When called on a dataset of (K, V) pairs, returns a dataset of (K, [V]) pairs.\n\n\n\n"
},

{
    "location": "api.html#Spark.id-Tuple{Spark.RDD}",
    "page": "API Reference",
    "title": "Spark.id",
    "category": "Method",
    "text": "Return the id of the rdd\n\n\n\n"
},

{
    "location": "api.html#Spark.map_pair-Tuple{Spark.RDD,Function}",
    "page": "API Reference",
    "title": "Spark.map_pair",
    "category": "Method",
    "text": "Apply function f to each element of rdd\n\n\n\n"
},

{
    "location": "api.html#Spark.map_partitions-Tuple{Spark.RDD,Function}",
    "page": "API Reference",
    "title": "Spark.map_partitions",
    "category": "Method",
    "text": "Apply function f to each partition of rdd. f should be of type (iterator) -> iterator\n\n\n\n"
},

{
    "location": "api.html#Spark.map_partitions_pair-Tuple{Spark.RDD,Function}",
    "page": "API Reference",
    "title": "Spark.map_partitions_pair",
    "category": "Method",
    "text": "Apply function f to each partition of rdd. f should be of type (iterator) -> iterator\n\n\n\n"
},

{
    "location": "api.html#Spark.map_partitions_with_index-Tuple{Spark.RDD,Function}",
    "page": "API Reference",
    "title": "Spark.map_partitions_with_index",
    "category": "Method",
    "text": "Apply function f to each partition of rdd. f should be of type (index, iterator) -> iterator\n\n\n\n"
},

{
    "location": "api.html#Spark.num_partitions-Tuple{Union{Spark.PipelinedPairRDD,Spark.PipelinedRDD}}",
    "page": "API Reference",
    "title": "Spark.num_partitions",
    "category": "Method",
    "text": "Returns the number of partitions of this RDD.\n\n\n\n"
},

{
    "location": "api.html#Spark.pipe-Tuple{Spark.RDD,String}",
    "page": "API Reference",
    "title": "Spark.pipe",
    "category": "Method",
    "text": "Return an RDD created by piping elements to a forked external process.\n\n\n\n"
},

{
    "location": "api.html#Spark.reduce_by_key-Tuple{Spark.PairRDD,Function}",
    "page": "API Reference",
    "title": "Spark.reduce_by_key",
    "category": "Method",
    "text": "When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V.\n\n\n\n"
},

{
    "location": "api.html#Spark.repartition-Tuple{T<:Spark.RDD,Integer}",
    "page": "API Reference",
    "title": "Spark.repartition",
    "category": "Method",
    "text": "Return a new RDD that has exactly num_partitions partitions.\n\n\n\n"
},

{
    "location": "api.html#Spark.share_variable-Tuple{Spark.SparkContext,Symbol,Any}",
    "page": "API Reference",
    "title": "Spark.share_variable",
    "category": "Method",
    "text": "Makes the value of data available on workers as symbol name\n\n\n\n"
},

{
    "location": "api.html#Spark.text_file-Tuple{Spark.SparkContext,AbstractString}",
    "page": "API Reference",
    "title": "Spark.text_file",
    "category": "Method",
    "text": "Create RDD from a text file\n\n\n\n"
},

{
    "location": "api.html#Spark.FlatMapIterator",
    "page": "API Reference",
    "title": "Spark.FlatMapIterator",
    "category": "Type",
    "text": "Iterates over the iterators within an iterator\n\n\n\n"
},

{
    "location": "api.html#Spark.JavaPairRDD",
    "page": "API Reference",
    "title": "Spark.JavaPairRDD",
    "category": "Type",
    "text": "Pure wrapper around JavaPairRDD\n\n\n\n"
},

{
    "location": "api.html#Spark.PipelinedPairRDD",
    "page": "API Reference",
    "title": "Spark.PipelinedPairRDD",
    "category": "Type",
    "text": "Julia type to handle Pair RDDs. Can handle pipelining of operations to reduce interprocess IO.\n\n\n\n"
},

{
    "location": "api.html#Spark.PipelinedPairRDD-Tuple{Spark.RDD,Function}",
    "page": "API Reference",
    "title": "Spark.PipelinedPairRDD",
    "category": "Method",
    "text": "Params:\n\nparentrdd - parent RDD\nfunc - function of type (index, iterator) -> iterator to apply to each partition\n\n\n\n"
},

{
    "location": "api.html#Spark.PipelinedRDD",
    "page": "API Reference",
    "title": "Spark.PipelinedRDD",
    "category": "Type",
    "text": "Julia type to handle RDDs. Can handle pipelining of operations to reduce interprocess IO.\n\n\n\n"
},

{
    "location": "api.html#Spark.PipelinedRDD-Tuple{Spark.RDD,Function}",
    "page": "API Reference",
    "title": "Spark.PipelinedRDD",
    "category": "Method",
    "text": "Params:\n\nparentrdd - parent RDD\nfunc - function of type (index, iterator) -> iterator to apply to each partition\n\n\n\n"
},

{
    "location": "api.html#Spark.add_file-Tuple{Spark.SparkContext,AbstractString}",
    "page": "API Reference",
    "title": "Spark.add_file",
    "category": "Method",
    "text": "Add file to SparkContext. This file will be downloaded to each executor's work directory\n\n\n\n"
},

{
    "location": "api.html#Spark.add_jar-Tuple{Spark.SparkContext,AbstractString}",
    "page": "API Reference",
    "title": "Spark.add_jar",
    "category": "Method",
    "text": "Add JAR file to SparkContext. Classes from this JAR will then be available to all tasks\n\n\n\n"
},

{
    "location": "api.html#Spark.chain_function-Tuple{Any,Any}",
    "page": "API Reference",
    "title": "Spark.chain_function",
    "category": "Method",
    "text": "chain 2 partion functions together \n\n\n\n"
},

{
    "location": "api.html#Spark.collect_internal-Tuple{Spark.RDD,Any,Any}",
    "page": "API Reference",
    "title": "Spark.collect_internal",
    "category": "Method",
    "text": "Collects the RDD to the Julia process, by serialising all values via a byte array\n\n\n\n"
},

{
    "location": "api.html#Spark.collect_internal_itr-Tuple{Spark.RDD,Any,Any}",
    "page": "API Reference",
    "title": "Spark.collect_internal_itr",
    "category": "Method",
    "text": "Collects the RDD to the Julia process, via an Julia iterator that fetches each row at a time. This prevents creation of a byte array containing all rows at a time.\n\n\n\n"
},

{
    "location": "api.html#Spark.collect_itr-Tuple{Spark.PairRDD}",
    "page": "API Reference",
    "title": "Spark.collect_itr",
    "category": "Method",
    "text": "Collect all elements of rdd on a driver machine\n\n\n\n"
},

{
    "location": "api.html#Spark.collect_itr-Tuple{Spark.SingleRDD}",
    "page": "API Reference",
    "title": "Spark.collect_itr",
    "category": "Method",
    "text": "Collect all elements of rdd on a driver machine\n\n\n\n"
},

{
    "location": "api.html#Spark.context-Tuple{Spark.RDD}",
    "page": "API Reference",
    "title": "Spark.context",
    "category": "Method",
    "text": "Get SparkContext of this RDD\n\n\n\n"
},

{
    "location": "api.html#Spark.create_flat_map_function-Tuple{Function}",
    "page": "API Reference",
    "title": "Spark.create_flat_map_function",
    "category": "Method",
    "text": "creates a function that operates on a partition from an element by element flat_map function\n\n\n\n"
},

{
    "location": "api.html#Spark.create_map_function-Tuple{Function}",
    "page": "API Reference",
    "title": "Spark.create_map_function",
    "category": "Method",
    "text": "creates a function that operates on a partition from an element by element map function\n\n\n\n"
},

{
    "location": "api.html#Spark.deserialized-Tuple{Array{UInt8,1}}",
    "page": "API Reference",
    "title": "Spark.deserialized",
    "category": "Method",
    "text": "Return object deserialized from array of bytes\n\n\n\n"
},

{
    "location": "api.html#Spark.readobj-Tuple{IO}",
    "page": "API Reference",
    "title": "Spark.readobj",
    "category": "Method",
    "text": "Read data object from a ioet. Returns code and byte array:\n\nif code is negative, it's considered as a special command code\nif code is positive, it's considered as array length\n\n\n\n"
},

{
    "location": "api.html#Spark.serialized-Tuple{Any}",
    "page": "API Reference",
    "title": "Spark.serialized",
    "category": "Method",
    "text": "Return serialized object as an array of bytes\n\n\n\n"
},

{
    "location": "api.html#Spark.writeobj-Tuple{IO,Any}",
    "page": "API Reference",
    "title": "Spark.writeobj",
    "category": "Method",
    "text": "Write object to stream\n\n\n\n"
},

{
    "location": "api.html#",
    "page": "API Reference",
    "title": "API Reference",
    "category": "page",
    "text": "Modules = [Spark]\nOrder   = [:type, :function]"
},

]}
