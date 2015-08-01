
abstract RDD

type JavaRDD <: RDD
    jrdd::JJavaRDD
end

type JuliaRDD <: RDD
    jrdd::JJuliaRDD
end

function JuliaRDD(parent::RDD, func::Array{Uint8, 1})        
    jjulia_rdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                       (JJavaRDD, Array{jbyte, 1}),
                       parent.jrdd, convert(Array{jbyte, 1}, func))
    JuliaRDD(jjulia_rdd)
end
    

type PipelinedRDD <: RDD
    # placeholder for now
end


