
abstract RDD
abstract SingleRDD <: RDD
abstract PairRDD <: RDD

"Pure wrapper around JavaRDD"
type JavaRDD <: SingleRDD
    jrdd::JJavaRDD
end

"Pure wrapper around JavaPairRDD"
type JavaPairRDD <: PairRDD
    jrdd::JJavaPairRDD
end

"""
Julia type to handle RDDs. Can handle pipelining of operations to reduce interprocess IO.
"""
type PipelinedRDD <: SingleRDD
    parentrdd::RDD
    func::Function
    jrdd::JJuliaRDD
end

"""
Julia type to handle Pair RDDs. Can handle pipelining of operations to reduce interprocess IO.
"""
type PipelinedPairRDD <: PairRDD
    parentrdd::RDD
    func::Function
    jrdd::JJuliaPairRDD
end

"""
Params:
 * parentrdd - parent RDD
 * func - function of type `(index, iterator) -> iterator` to apply to each partition
"""
function PipelinedRDD(parentrdd::RDD, func::Function)
    command_ser = create_pipeline_command(parentrdd, func)
    jrdd = jcall(JJuliaRDD, "fromRDD", JJuliaRDD,
                 (JRDD, Vector{jbyte}),
                 get_root_rdd(parentrdd), command_ser)
    PipelinedRDD(parentrdd, func, jrdd)
end

"""
Params:
 * parentrdd - parent RDD
 * func - function of type `(index, iterator) -> iterator` to apply to each partition
"""
function PipelinedPairRDD(parentrdd::RDD, func::Function)
    command_ser = create_pipeline_command(parentrdd, func)
    jrdd = jcall(JJuliaPairRDD, "fromRDD", JJuliaPairRDD,
                 (JRDD, Vector{jbyte}),
                 get_root_rdd(parentrdd), command_ser)
    PipelinedPairRDD(parentrdd, func, jrdd)
end

# as_java_rdd returns a JJavaRDD or JJavaPairRDD class
as_java_rdd(rdd::RDD) = rdd.jrdd
as_java_rdd(rdd::PipelinedRDD) = jcall(rdd.jrdd, "asJavaRDD", JJavaRDD, ())
as_java_rdd(rdd::PipelinedPairRDD) = jcall(rdd.jrdd, "asJavaPairRDD", JJavaPairRDD, ())

# as_java_rdd returns a JRDD class
as_rdd(rdd::RDD) = jcall(rdd.jrdd, "rdd", JRDD, ())
as_rdd(rdd::PipelinedRDD) = rdd.jrdd
as_rdd(rdd::PipelinedPairRDD) = rdd.jrdd

# as_root_rdd returns the root RDD suitable for contructing JJuliaRDD or JJuliaPairRDD objects
get_root_rdd(rdd::RDD) = as_rdd(rdd)
get_root_rdd(rdd::PipelinedRDD) = get_root_rdd(rdd.parentrdd)
get_root_rdd(rdd::PipelinedPairRDD) = get_root_rdd(rdd.parentrdd)

Base.show(io::IO, rdd::JavaRDD) = print(io, "JavaRDD()")
Base.show(io::IO, rdd::JavaPairRDD) = print(io, "JavaPairRDD()")
Base.show(io::IO, rdd::PipelinedRDD) =  print(io, "PipelinedRDD($(rdd.parentrdd))")
Base.show(io::IO, rdd::PipelinedPairRDD) = print(io, "PipelinedPairRDD($(rdd.parentrdd))")

create_pipeline_command(rdd::RDD, func) = reinterpret(Vector{jbyte}, serialized(func))

function create_pipeline_command(rdd::PipelinedRDD, func)
    parent_func = rdd.func
    function pipelined_func(split, iterator)
        return func(split, parent_func(split, iterator))
    end
    command_ser = reinterpret(Vector{jbyte}, serialized(pipelined_func))
end

function create_pipeline_command(rdd::PipelinedPairRDD, func)
    parent_func = rdd.func
    function pipelined_func(split, iterator)
        return func(split, parent_func(split, iterator))
    end
    command_ser = reinterpret(Vector{jbyte}, serialized(pipelined_func))
end

Base.reinterpret(::Type{Array{jbyte,1}}, bytes::Array{UInt8,1}) =
    jbyte[reinterpret(jbyte, b) for b in bytes]

Base.reinterpret(::Type{Array{UInt8,1}}, bytes::Array{jbyte,1}) =
    UInt8[reinterpret(UInt8, b) for b in bytes]

"""
Apply function `f` to each partition of `rdd`. `f` should be of type
`(index, iterator) -> iterator`
"""
function map_partitions_with_index(rdd::RDD, f::Function)
    return PipelinedRDD(rdd, f)
end

"""
Apply function `f` to each partition of `rdd`. `f` should be of type
`(iterator) -> iterator`
"""
function map_partitions(rdd::RDD, f::Function)
    function func(idx, it)
        f(it)
    end
    return PipelinedRDD(rdd, func)
end

"""
Apply function `f` to each partition of `rdd`. `f` should be of type
`(iterator) -> iterator`
"""
function map_partitions_pair(rdd::RDD, f::Function)
    function func(idx, it)
        f(it)
    end
    return create_pair_pipeline_rdd(rdd, func)
end

"Apply function `f` to each element of `rdd`"
function map(rdd::RDD, f::Function)
    function func(idx, it)
        imap(f, it)
    end
    return PipelinedRDD(rdd, func)
end

"Apply function `f` to each element of `rdd`"
function map_pair(rdd::RDD, f::Function)
    function func(idx, it)
        imap(f, it)
    end
    return create_pair_pipeline_rdd(rdd, func)
end

"""
Similar to `map`, but each input item can be mapped to 0 or more 
output items (so `f` should return an iterator rather than a single item)
"""
function flat_map(rdd::RDD, f::Function)
    function func(idx, it)
        FlatMapIterator(imap(f, it))
    end
    return PipelinedRDD(rdd, func)
end

"""
Similar to `map`, but each input item can be mapped to 0 or more 
output items (so `f` should return an iterator rather than a single item)
"""
function flat_map_pair(rdd::RDD, f::Function)
    function func(idx, it)
        FlatMapIterator(imap(f, it))
    end
    return create_single_pipeline_rdd(rdd, func)
end

"Reduce elements of `rdd` using specified function `f`"
function reduce(rdd::RDD, f::Function)
    process_attachments(context(rdd))
    locally_reduced = map_partitions(rdd, it -> [reduce(f, it)])
    subresults = collect(locally_reduced)
    return reduce(f, subresults)
end

"Get SparkContext of this RDD"
function context(rdd::RDD)
    ssc = jcall(rdd.jrdd, "context", JSparkContext, ())
    jsc = jcall(JJavaSparkContext, "fromSparkContext",
                JJavaSparkContext, (JSparkContext,), ssc)
    return SparkContext(jsc)
end

"""
Collect all elements of `rdd` on a driver machine
"""
function collect(rdd::SingleRDD)
    process_attachments(context(rdd))
    jobj = jcall(rdd.jrdd, "collect", JObject, ())
    jbyte_arrs = convert(Vector{Vector{jbyte}}, jobj)
    byte_arrs = Vector{UInt8}[reinterpret(Vector{UInt8}, arr)
                              for arr in jbyte_arrs]
    vals = [deserialized(arr) for arr in byte_arrs]
    return vals
end

"Count number of elements in this RDD"
function count(rdd::RDD)
    process_attachments(context(rdd))
    return jcall(rdd.jrdd, "count", jlong, ())
end

function cache(rdd::RDD)
    @assert(typeof(parent(rdd)) == JavaRDD,
            "Non-pipelineable RDDs are not supported yet")    
    parent_jrdd = parent(rdd).jrdd
    jcall(parent_jrdd, "cache", JJavaRDD, ())
    return rdd
end

function cartesian(rdd1::SingleRDD, rdd2::SingleRDD)
    jprdd = jcall(JJuliaRDD, "cartesianSS", JJavaPairRDD,
                 (JJavaRDD, JJavaRDD),
                 as_java_rdd(rdd1),  as_java_rdd(rdd2))

    JavaPairRDD(jprdd)
end

function group_by_key(rdd::PairRDD)
    jprdd = jcall(as_java_rdd(rdd), "groupByKey", JJavaPairRDD, ())
    JavaPairRDD(jprdd)
end
