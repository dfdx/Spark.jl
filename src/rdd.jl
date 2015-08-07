
abstract RDD

type JavaRDD <: RDD
    jrdd::JJavaRDD
end


type PipelinedRDD <: RDD
    parent::RDD
    func::Function
    jrdd::JJuliaRDD
end


function PipelinedRDD(parent::RDD, func::Function)
    if !isa(parent, PipelinedRDD)
        command = serialized(func)
        jrdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                     (JJavaRDD, Array{jbyte, 1}),
                     parent.jrdd, convert(Array{jbyte, 1}, command))
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
                     parent.parent.jrdd, convert(Array{jbyte, 1}, command))
        PipelinedRDD(parent.parent, pipelined_func, jrdd)
    end
end


function map_partitions_with_index(rdd::RDD, func::Function)
    return PipelinedRDD(rdd, func)
end


function collect{T}(rdd::RDD, typ::Type{T}=Array{Array{jbyte,1},1})
    res = jcall(rdd.jrdd, "collect", JObject, ())
    return convert(T, res)
end

