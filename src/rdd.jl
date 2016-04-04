
abstract RDD

"Pure wrapper around JavaRDD"
type JavaRDD <: RDD
    jrdd::JJavaRDD
    eltyp::DataType
end

"""
Julia type to handle RDDs. Can handle pipelining of operations to reduce Java IO.
"""
type PipelinedRDD <: RDD
    parentrdd::RDD
    func::Function
    jrdd::JJuliaRDD
    eltyp::DataType
end

"Pseudo-RDD, think of `nothing` inherited from RDD"
immutable NotRDD <: RDD
end

NOT_RDD = NotRDD()

"Special type to indicate that type of RDD elements is not known"
immutable Unknown
end


Base.eltype(rdd::RDD) = rdd.eltyp
Base.parent(rdd::PipelinedRDD) = rdd.parentrdd
Base.parent(rdd::RDD) = NOT_RDD

assigntype!{T}(rdd::RDD, ::Type{T}) = (rdd.eltyp = T)


Base.reinterpret(::Type{Array{jbyte,1}}, bytes::Array{UInt8,1}) =
    jbyte[reinterpret(jbyte, b) for b in bytes]

Base.reinterpret(::Type{Array{UInt8,1}}, bytes::Array{jbyte,1}) =
    UInt8[reinterpret(UInt8, b) for b in bytes]


function latest_known_eltype(eltyp::DataType, nextrdd::RDD)
    if eltyp != Unknown
        return eltyp
    elseif nextrdd == NOT_RDD
        return Unknown
    else
        println("!!!eltyp = $eltyp, parent(nextrdd) = $(parent(nextrdd))")
        return latest_known_eltype(eltype(nextrdd), parent(nextrdd))
    end
end

"""
Params:
 * parentrdd - parent RDD
 * func - function of type `(split, iterator) -> iterator` to apply to each partition
 * eltyp - type of RDD elements, optional
"""
function PipelinedRDD(parentrdd::RDD, func::Function, eltyp::DataType=Unknown)
    latest_known_eltyp = latest_known_eltype(eltyp, parentrdd)
    latest_known_eltyp_ser = reinterpret(Vector{jbyte}, serialized(latest_known_eltyp))
    if !isa(parentrdd, PipelinedRDD)
        command_ser = reinterpret(Vector{jbyte}, serialized(func))
        jrdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                     (JJavaRDD, Vector{jbyte}, Vector{jbyte}),
                     parentrdd.jrdd, command_ser, latest_known_eltyp_ser)
        PipelinedRDD(parentrdd, func, jrdd, Unknown)
    else
        parent_func = parentrdd.func
        function pipelined_func(split, iterator)
            return func(split, parent_func(split, iterator))
        end
        command_ser = reinterpret(Vector{jbyte}, serialized(pipelined_func))
        jrdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                     (JJavaRDD, Vector{jbyte}, Vector{jbyte}),
                     parent(parentrdd).jrdd, command_ser, latest_known_eltyp_ser)
        PipelinedRDD(parent(parentrdd), pipelined_func, jrdd, Unknown)
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
    # TODO: not tested, not verified for correctness
    # should probably wait until at least strings are supported
    subresults = collect(map_partitions(rdd, f))
    return reduce(f, subresults)
end


function collect{T}(rdd::RDD, ::Type{T}=eltype(rdd))
    ET = T != Unknown ? T : Vector{UInt8}
    jobj = jcall(rdd.jrdd, "collect", JObject, ())
    jbyte_arrs = convert(Vector{Vector{jbyte}}, jobj)
    byte_arrs = Vector{UInt8}[reinterpret(Vector{UInt8}, arr) for arr in jbyte_arrs]
    vals = [from_bytes(ET, arr) for arr in byte_arrs]
    return vals
end


function count(rdd::RDD)
    return jcall(rdd.jrdd, "count", jlong, ())
end
