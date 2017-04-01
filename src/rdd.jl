
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


function Base.show(io::IO, rdd::JavaRDD)
    print(io, "JavaRDD()")
end

function Base.show(io::IO, rdd::JavaPairRDD)
    print(io, "JavaPairRDD()")
end

function Base.show(io::IO, rdd::PipelinedRDD)
    print(io, "PipelinedRDD($(rdd.parentrdd))")
end
function Base.show(io::IO, rdd::PipelinedPairRDD)
    print(io, "PipelinedPairRDD($(rdd.parentrdd))")
end

function create_pipeline_command(rdd::JavaRDD, func)
    command_ser = reinterpret(Vector{jbyte}, serialized(func))
end

function create_pipeline_command(rdd::JavaPairRDD, func)
    command_ser = reinterpret(Vector{jbyte}, serialized(func))
end

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

"""
Params:
 * parentrdd - parent RDD
 * func - function of type `(index, iterator) -> iterator` to apply to each partition
"""
function create_single_pipeline_rdd(parentrdd::RDD, func::Function)
    command_ser = create_pipeline_command(parentrdd, func)
    jrdd = jcall(JJuliaRDD, "fromRDD", JJuliaRDD,
                 (JRDD, Vector{jbyte}),
                 as_rdd(parent(parentrdd)), command_ser)
    PipelinedRDD(parentrdd, func, jrdd)
end

"""
Params:
 * parentrdd - parent RDD
 * func - function of type `(index, iterator) -> iterator` to apply to each partition
"""
function create_pair_pipeline_rdd(parentrdd::RDD, func::Function)
    command_ser = create_pipeline_command(parentrdd, func)
    jrdd = jcall(JJuliaPairRDD, "fromRDD", JJuliaPairRDD,
                 (JRDD, Vector{jbyte}),
                 as_rdd(parent(parentrdd)), command_ser)
    PipelinedPairRDD(parentrdd, func, jrdd)
end

function as_java_rdd(rdd::JavaRDD)
    rdd.jrdd
end

function as_java_rdd(rdd::JavaPairRDD)
    rdd.jrdd
end

function as_java_rdd(rdd::PipelinedRDD)
    jcall(rdd.jrdd, "asJavaRDD", JJavaRDD, ())
end

function as_java_rdd(rdd::PipelinedPairRDD)
    jcall(rdd.jrdd, "asJavaPairRDD", JJavaPairRDD, ())
end

function as_rdd(rdd::JavaRDD)
    jcall(rdd.jrdd, "rdd", JRDD, ())
end

function as_rdd(rdd::JavaPairRDD)
    jcall(rdd.jrdd, "rdd", JRDD, ())
end

function as_rdd(rdd::PipelinedRDD)
    rdd.jrdd
end

function as_rdd(rdd::PipelinedPairRDD)
    rdd.jrdd
end

Base.parent(rdd::RDD) = rdd
Base.parent(rdd::PipelinedRDD) = rdd.parentrdd
Base.parent(rdd::PipelinedPairRDD) = rdd.parentrdd

Base.reinterpret(::Type{Array{jbyte,1}}, bytes::Array{UInt8,1}) =
    jbyte[reinterpret(jbyte, b) for b in bytes]

Base.reinterpret(::Type{Array{UInt8,1}}, bytes::Array{jbyte,1}) =
    UInt8[reinterpret(UInt8, b) for b in bytes]

"""
Apply function `f` to each partition of `rdd`. `f` should be of type
`(index, iterator) -> iterator`
"""
function map_partitions_with_index(rdd::RDD, f::Function)
    return create_single_pipeline_rdd(rdd, f)
end

"""
Apply function `f` to each partition of `rdd`. `f` should be of type
`(iterator) -> iterator`
"""
function map_partitions(rdd::RDD, f::Function)
    function func(idx, it)
        f(it)
    end
    return create_single_pipeline_rdd(rdd, func)
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
    return create_single_pipeline_rdd(rdd, func)
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
    return create_single_pipeline_rdd(rdd, func)
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
function reduce(rdd::SingleRDD, f::Function)
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
