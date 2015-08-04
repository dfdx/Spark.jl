
abstract RDD

type JavaRDD <: RDD
    jrdd::JJavaRDD
end

type PipelinedRDD <: RDD
    jrdd::JJuliaRDD
end

function PipelinedRDD(parent::RDD, func::Array{Uint8, 1})        
    jrdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                 (JJavaRDD, Array{jbyte, 1}),
                 parent.jrdd, convert(Array{jbyte, 1}, func))
    PipelinedRDD(jrdd)
end
    

