
abstract RDD

"Pure wrapper around JavaRDD"
type JavaRDD <: RDD
    jrdd::JJavaRDD
    meta::Dict{Symbol,Any}
end

"""
Julia type to handle RDDs. Can handle pipelining of operations to reduce Java IO.
"""
type PipelinedRDD <: RDD
    parentrdd::RDD
    func::Function
    jrdd::JJuliaRDD
    meta::Dict{Symbol,Any}
end


Base.eltype(rdd::RDD) = get(rdd.meta, :eltyp, nothing)
Base.parent(rdd::PipelinedRDD) = rdd.parentrdd
Base.parent(rdd::RDD) = nothing


# TODO: assigntype! -> typehint!
# TODO: infer type from the 1st element of RDD (in worker.jl? first(rdd)?)
assigntype!{T}(rdd::RDD, ::Type{T}) = (rdd.meta[:eltyp] = T)


function source_eltype(nextrdd::Union{RDD, Void})
    if nextrdd == nothing
        return nothing
    elseif haskey(nextrdd.meta, :source_eltyp)
        return nextrdd.meta[:source_eltyp]
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
 * func - function of type `(split, iterator) -> iterator` to apply to each partition
 * source_eltype - type of RDD elements, optional
"""
function PipelinedRDD(parentrdd::RDD, func::Function,
                      source_eltyp::Union{DataType, Void}=nothing)
    meta = Dict{Symbol,Any}()
    if source_eltyp != nothing
        meta[:source_eltyp] = source_eltyp
    else
        source_eltyp = source_eltype(parentrdd)
    end
    source_eltyp_ser = reinterpret(Vector{jbyte}, serialized(source_eltyp))
    if !isa(parentrdd, PipelinedRDD)
        command_ser = reinterpret(Vector{jbyte}, serialized(func))
        jrdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                     (JJavaRDD, Vector{jbyte}, Vector{jbyte}),
                     parentrdd.jrdd, command_ser, source_eltyp_ser)
        PipelinedRDD(parentrdd, func, jrdd, meta)
    else
        parent_func = parentrdd.func
        function pipelined_func(split, iterator)
            return func(split, parent_func(split, iterator))
        end
        command_ser = reinterpret(Vector{jbyte}, serialized(pipelined_func))
        jrdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                     (JJavaRDD, Vector{jbyte}, Vector{jbyte}),
                     parent(parentrdd).jrdd, command_ser, source_eltyp_ser)
        PipelinedRDD(parent(parentrdd), pipelined_func, jrdd, meta)
    end
end


function map_partitions_with_index(rdd::RDD, f::Function)
    return PipelinedRDD(rdd, f)
end

function map_partitions(rdd::RDD, f::Function)
    function func(idx, it)
        f(it)
    end
    return PipelinedRDD(rdd, func)
end

function map(rdd::RDD, f::Function)
    function func(idx, it)
        imap(f, it)
    end
    return PipelinedRDD(rdd, func)
end

function reduce(rdd::RDD, f::Function)
    locally_reduced = map_partitions(rdd, it -> reduce(f, it))
    subresults = collect(locally_reduced, eltype(rdd))
    return reduce(f, subresults)
end


function collect{T}(rdd::RDD, ::Type{T})
    jobj = jcall(rdd.jrdd, "collect", JObject, ())
    jbyte_arrs = convert(Vector{Vector{jbyte}}, jobj)
    byte_arrs = Vector{UInt8}[reinterpret(Vector{UInt8}, arr) for arr in jbyte_arrs]
    vals = [from_bytes(T, arr) for arr in byte_arrs]
    return vals
end

function collect(rdd::RDD)
    T = eltype(rdd)
    ET = T != nothing ? T : Vector{UInt8}
    return collect(rdd, ET)
end


function count(rdd::RDD)
    return jcall(rdd.jrdd, "count", jlong, ())
end
