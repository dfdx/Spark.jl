
abstract RDD

type JavaRDD <: RDD
    jrdd::JJavaRDD
end


type PipelinedRDD <: RDD
    parent::RDD
    jrdd::JJuliaRDD
end


function PipelinedRDD(parent::RDD, func::Function)
    jrdd = if !isa(parent, PipelinedRDD)
        command = serialized(func)
        jrdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                     (JJavaRDD, Array{jbyte, 1}),
                     parent.jrdd, convert(Array{jbyte, 1}, command))
        PipelinedRDD(parent, jrdd)
    else
        pipeline_func = (split, it) -> func(split, parent.func(split, it))
        error("Not implemented yet")
    end
end


function jrdd(rdd)

end


function collect{T}(rdd::RDD, typ::Type{T}=Array{Array{jbyte,1},1})
    res = jcall(rdd.jrdd, "collect", JObject, ())
    return convert(T, res)
end
