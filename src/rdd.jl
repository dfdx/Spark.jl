
abstract RDD

"Pure wrapper around JavaRDD"
type JavaRDD <: RDD
    jrdd::JJavaRDD
    meta::Dict{Symbol,Any}
end

"""
Julia type to handle RDDs. Can handle pipelining of operations to reduce interprocess IO.
"""
type PipelinedRDD <: RDD
    parentrdd::RDD
    func::Function
    jrdd::JJuliaRDD
    meta::Dict{Symbol,Any}
end

function Base.show(io::IO, rdd::JavaRDD)
    typ_str = haskey(rdd.meta, :typ) ? "{$(rdd.meta[:typ])}" : ""
    print(io, "JavaRDD$typ_str()")
end
function Base.show(io::IO, rdd::PipelinedRDD)
    typ_str = haskey(rdd.meta, :typ) ? "{$(rdd.meta[:typ])}" : ""
    print(io, "PipelinedRDD$typ_str($(rdd.parentrdd))")
end

"Compute a little portiton of RDD to get determine type of return elements"
function probe_type(rdd::RDD)
    type_rdd = map(rdd, typeof)
    jobj = jcall(type_rdd.jrdd, "first", JObject, ())
    jbytes = convert(Vector{jbyte}, jobj)
    bytes = reinterpret(Vector{UInt8}, jbytes)
    typ = deserialized(bytes)
    return typ
end

function Base.eltype(rdd::RDD)
    if haskey(rdd.meta, :typ)
        return rdd.meta[:typ] # get from cache
    else
        typ = probe_type(rdd)
        rdd.meta[:typ] = typ
        return typ
    end

end

Base.parent(rdd::PipelinedRDD) = rdd.parentrdd
Base.parent(rdd::RDD) = nothing

"""
Explicitly set element type for this RDD. This may be used on final RDD to prevent
automatic type probation.
"""
typehint!{T}(rdd::RDD, ::Type{T}) = (rdd.meta[:typ] = T)

"""
Extract or calculate type of elements in the source RDD. Source RDD is normally
the Java RDD that precedes JuliaRDD. Type returned by this method is used
to determine how to decode byte arrays passed from JVM to Julia.
"""
function source_eltype(nextrdd::Union{RDD, Void})
    if nextrdd == nothing
        return nothing
    elseif haskey(nextrdd.meta, :styp)
        return nextrdd.meta[:styp]
    else
        return source_eltype(parent(nextrdd))
    end
end


Base.reinterpret(::Type{Array{jbyte,1}}, bytes::Array{UInt8,1}) =
    jbyte[reinterpret(jbyte, b) for b in bytes]

Base.reinterpret(::Type{Array{UInt8,1}}, bytes::Array{jbyte,1}) =
    UInt8[reinterpret(UInt8, b) for b in bytes]


"""
Params:
 * parentrdd - parent RDD
 * func - function of type `(index, iterator) -> iterator` to apply to each partition
 * stype - type of source elements for this RDD, optional
"""
function PipelinedRDD(parentrdd::RDD, func::Function,
                      styp::Union{DataType, Void}=nothing)
    meta = Dict{Symbol,Any}()
    if styp != nothing
        meta[:styp] = styp
    else
        styp = source_eltype(parentrdd)
    end
    styp_ser = reinterpret(Vector{jbyte}, serialized(styp))
    if !isa(parentrdd, PipelinedRDD)
        command_ser = reinterpret(Vector{jbyte}, serialized(func))
        jrdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                     (JJavaRDD, Vector{jbyte}, Vector{jbyte}),
                     parentrdd.jrdd, command_ser, styp_ser)
        PipelinedRDD(parentrdd, func, jrdd, meta)
    else
        parent_func = parentrdd.func
        function pipelined_func(split, iterator)
            return func(split, parent_func(split, iterator))
        end
        command_ser = reinterpret(Vector{jbyte}, serialized(pipelined_func))
        jrdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                     (JJavaRDD, Vector{jbyte}, Vector{jbyte}),
                     parent(parentrdd).jrdd, command_ser, styp_ser)
        PipelinedRDD(parent(parentrdd), pipelined_func, jrdd, meta)
    end
end

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

"Apply function `f` to each element of `rdd`"
function map(rdd::RDD, f::Function)
    function func(idx, it)
        imap(f, it)
    end
    return PipelinedRDD(rdd, func)
end

"Reduce elements of `rdd` using specified function `f`"
function reduce(rdd::RDD, f::Function)
    process_attachments(context(rdd))
    locally_reduced = map_partitions(rdd, it -> [reduce(f, it)])
    subresults = collect(eltype(rdd), locally_reduced)
    return reduce(f, subresults)
end

"Get SparkContext of this RDD"
function context(rdd::RDD)
    ssc = jcall(rdd.jrdd, "context", JSparkContext, ())
    jsc = jcall(JJavaSparkContext, "fromSparkContext",
                JJavaSparkContext, (JSparkContext,), ssc)
    return SparkContext(jsc, "")  # TODO: get name
end

"""
Collect all elements of `rdd` on a driver machine. This method may take optional
parameter of element type to convert after collecting
"""
function collect{T}(::Type{T}, rdd::RDD)
    process_attachments(context(rdd))
    jobj = jcall(rdd.jrdd, "collect", JObject, ())
    jbyte_arrs = convert(Vector{Vector{jbyte}}, jobj)
    byte_arrs = Vector{UInt8}[reinterpret(Vector{UInt8}, arr)
                              for arr in jbyte_arrs]
    vals = [from_bytes(T, arr) for arr in byte_arrs]
    return vals
end

"Collect elements of this rdd to a driver process"
function collect(rdd::RDD)
    process_attachments(context(rdd))
    T = eltype(rdd)
    ET = T != nothing ? T : Vector{UInt8}
    return collect(ET, rdd)
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
