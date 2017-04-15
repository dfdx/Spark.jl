
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
    pipelined_func = create_pipeline_command(parentrdd, func)
    command_ser = reinterpret(Vector{jbyte}, serialized(pipelined_func))
    jrdd = jcall(JJuliaRDD, "fromRDD", JJuliaRDD,
                 (JRDD, Vector{jbyte}),
                 get_root_rdd(parentrdd), command_ser)
    PipelinedRDD(parentrdd, pipelined_func, jrdd)
end

"""
Params:
 * parentrdd - parent RDD
 * func - function of type `(index, iterator) -> iterator` to apply to each partition
"""
function PipelinedPairRDD(parentrdd::RDD, func::Function)
    pipelined_func = create_pipeline_command(parentrdd, func)
    command_ser = reinterpret(Vector{jbyte}, serialized(pipelined_func))
    jrdd = jcall(JJuliaPairRDD, "fromRDD", JJuliaPairRDD,
                 (JRDD, Vector{jbyte}),
                 get_root_rdd(parentrdd), command_ser)
    PipelinedPairRDD(parentrdd, pipelined_func, jrdd)
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

" chain 2 partion functions together " 
function chain_function(parent_func, child_func)
    function pipelined_func(split, iterator)
        return child_func(split, parent_func(split, iterator))
    end
    pipelined_func
end

create_pipeline_command(rdd::RDD, func) = func
create_pipeline_command(rdd::PipelinedRDD, func) = chain_function(rdd.func, func)
create_pipeline_command(rdd::PipelinedPairRDD, func) = chain_function(rdd.func, func)

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

function add_index_param(f::Function)
    function func(idx, it)
        f(it)
    end
    func
end
    
"""
Apply function `f` to each partition of `rdd`. `f` should be of type
`(iterator) -> iterator`
"""
function map_partitions(rdd::RDD, f::Function)
    return PipelinedRDD(rdd, add_index_param(f))
end

"""
Apply function `f` to each partition of `rdd`. `f` should be of type
`(iterator) -> iterator`
"""
function map_partitions_pair(rdd::RDD, f::Function)
    return PipelinedPairRDD(rdd, add_index_param(f))
end

"""
creates a function that operates on a partition from an
element by element map function
"""
function create_map_function(f::Function)
    function func(idx, it)
        imap(f, it)
    end
    return func
end


"Apply function `f` to each element of `rdd`"
function map(rdd::RDD, f::Function)
    return PipelinedRDD(rdd, create_map_function(f))
end

"Apply function `f` to each element of `rdd`"
function map_pair(rdd::RDD, f::Function)
    return PipelinedPairRDD(rdd, create_map_function(f))
end

"""
creates a function that operates on a partition from an
element by element flat_map function
"""
function create_flat_map_function(f::Function)
    function func(idx, it)
        FlatMapIterator(imap(f, it))
    end
    return func
end

"""
Similar to `map`, but each input item can be mapped to 0 or more 
output items (so `f` should return an iterator rather than a single item)
"""
function flat_map(rdd::RDD, f::Function)
    return PipelinedRDD(rdd, create_flat_map_function(f))
end

"""
Similar to `map`, but each input item can be mapped to 0 or more 
output items (so `f` should return an iterator of pairs rather than a single item)
"""
function flat_map_pair(rdd::RDD, f::Function)
    return PipelinedPairRDD(rdd, create_flat_map_function(f))
end

function create_filter_function(f::Function)
    function func(idx, it)
        filter(f, it)
    end
    return func
end

filter(rdd::SingleRDD, f::Function) = PipelinedRDD(rdd, create_filter_function(f))
filter(rdd::PairRDD, f::Function) = PipelinedPairRDD(rdd, create_filter_function(f))

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

function collect_internal(rdd::RDD, static_java_class, result_class)
    process_attachments(context(rdd))
    jbyte_arr = jcall(static_java_class, "collectToJulia", Vector{jbyte},
                 (result_class,),
                 as_java_rdd(rdd))

    byte_arrs = reinterpret(Vector{UInt8}, jbyte_arr)
    val = readobj(IOBuffer(byte_arrs))[2]
    return val
end

"""
Collect all elements of `rdd` on a driver machine
"""
function collect(rdd::SingleRDD)
    collect_internal(rdd, JJuliaRDD, JJavaRDD)
end

"""
Collect all elements of `rdd` on a driver machine
"""
function collect(rdd::PairRDD)
    collect_internal(rdd, JJuliaPairRDD, JJavaPairRDD)
end


"Count number of elements in this RDD"
function count(rdd::RDD)
    process_attachments(context(rdd))
    return jcall(rdd.jrdd, "count", jlong, ())
end

"Persist this RDD with the default storage level (MEMORY_ONLY)"
function cache(rdd::SingleRDD)
    JavaRDD(jcall(as_java_rdd(rdd), "cache", JJavaRDD, ()))
end

"Persist this RDD with the default storage level (MEMORY_ONLY)"
function cache(rdd::PairRDD)
    JavaPairRDD(jcall(as_java_rdd(rdd), "cache", JJavaPairRDD, ()))
end

"Create a pair RDD with every combination of the values of rdd1 and rdd2"
function cartesian(rdd1::SingleRDD, rdd2::SingleRDD)
    jprdd = jcall(JJuliaRDD, "cartesianSS", JJavaPairRDD,
                 (JJavaRDD, JJavaRDD),
                 as_java_rdd(rdd1),  as_java_rdd(rdd2))

    return JavaPairRDD(jprdd)
end

"When called on a dataset of (K, V) pairs, returns a dataset of (K, [V]) pairs."
function group_by_key(rdd::PairRDD)
    jprdd = jcall(as_java_rdd(rdd), "groupByKey", JJavaPairRDD, ())
    return JavaPairRDD(jprdd)
end

"""Return a new RDD that has exactly num_partitions partitions."""
function repartition{T<:RDD}(rdd::T, num_partitions::Integer)
    (Tjr, Tr) = (T <: PairRDD) ? (JJavaPairRDD, JavaPairRDD) : (JJavaRDD, JavaRDD)
    jrdd = jcall(as_java_rdd(rdd), "repartition", Tjr, (jint,), num_partitions)
    return Tr(jrdd)
end

"""Return a new RDD that is reduced into num_partitions partitions."""
function coalesce{T<:RDD}(rdd::T, num_partitions::Integer; shuffle::Union{Void,Bool}=nothing)
    (Tjr, Tr) = (T <: PairRDD) ? (JJavaPairRDD, JavaPairRDD) : (JJavaRDD, JavaRDD)
    if shuffle === nothing
        jrdd = jcall(as_java_rdd(rdd), "coalesce", Tjr, (jint,), num_partitions)
    else
        jrdd = jcall(as_java_rdd(rdd), "coalesce", Tjr, (jint,jboolean), num_partitions, shuffle)
    end
    return Tr(jrdd)
end

"""
When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the 
values for each key are aggregated using the given reduce function func, 
which must be of type (V,V) => V.
"""
function reduce_by_key(rdd::PairRDD, f::Function)
    grouped = group_by_key(rdd)
    function func(it)
        (it[1], reduce(f, it[2]))
    end
    return map_pair(grouped, func)
end

"""Returns the number of partitions of this RDD."""
num_partitions(rdd::Union{PipelinedRDD,PipelinedPairRDD}) = jcall(rdd.jrdd, "getNumPartitions", jint, ())
num_partitions(rdd::JavaRDD) = jcall(JRDDUtils, "getNumPartitions", jint, (JJavaRDD,), as_java_rdd(rdd))
num_partitions(rdd::JavaPairRDD) = jcall(JRDDUtils, "getNumPartitions", jint, (JJavaPairRDD,), as_java_rdd(rdd))

"Return the id of the rdd"
function id(rdd::RDD)
    jcall(rdd.jrdd, "id", jint, ())
end

"""Return an RDD created by piping elements to a forked external process."""
function pipe(rdd::RDD, command::String)
    jrdd = jcall(as_java_rdd(rdd), "pipe", JJavaRDD, (JString,), command)
    return JavaRDD(jrdd)
end

function pipe(rdd::RDD, command::Vector{String})
    jrdd = jcall(as_java_rdd(rdd), "pipe", JJavaRDD, (JList,), convert(JArrayList, command, JString))
    return JavaRDD(jrdd)
end

function pipe(rdd::RDD, command::Vector{String}, env::Dict{String,String})
    jrdd = jcall(as_java_rdd(rdd), "pipe", JJavaRDD, (JList,JMap), convert(JArrayList, command, JString), convert(JHashMap, JString, JString, env))
    return JavaRDD(jrdd)
end
