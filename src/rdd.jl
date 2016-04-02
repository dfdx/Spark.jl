
abstract RDD

type JavaRDD <: RDD
    jrdd::JJavaRDD
end


type PipelinedRDD <: RDD
    parent::RDD
    func::Function
    jrdd::JJuliaRDD
end


Base.reinterpret(::Type{Array{jbyte,1}}, bytes::Array{UInt8,1}) =
    jbyte[reinterpret(jbyte, b) for b in bytes]

Base.reinterpret(::Type{Array{UInt8,1}}, bytes::Array{jbyte,1}) =
    UInt8[reinterpret(UInt8, b) for b in bytes]


function PipelinedRDD(parent::RDD, func::Function)
    if !isa(parent, PipelinedRDD)
        command = serialized(func)
        jrdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                     (JJavaRDD, Array{jbyte, 1}),
                     parent.jrdd, reinterpret(Vector{jbyte}, command))
                     # parent.jrdd, convert(Array{jbyte, 1}, command))
        PipelinedRDD(parent, func, jrdd)
    else
        parent_func = parent.func
        function pipelined_func(split, iterator)
            return func(split, parent_func(split, iterator))
        end
        # pipelined_func = (split, it) -> func(split, parent.func(split, it))
        command = serialized(pipelined_func)
        jrdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                     (JJavaRDD, Array{jbyte, 1}),
                     parent.parent.jrdd, reinterpret(Vector{jbyte}, command))
        PipelinedRDD(parent.parent, pipelined_func, jrdd)
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


# TODO: make RDD{T}?
function collect{T}(rdd::RDD, ::Type{T}=Vector{UInt8})
    jobj = jcall(rdd.jrdd, "collect", JObject, ())
    jbyte_arrs = convert(Vector{Vector{jbyte}}, jobj)
    byte_arrs = Vector{UInt8}[reinterpret(Vector{UInt8}, arr) for arr in jbyte_arrs]
    vals = [decode(T, arr) for arr in byte_arrs]
    return vals
end


function count(rdd::RDD)
    return jcall(rdd.jrdd, "count", jlong, ())    
end
