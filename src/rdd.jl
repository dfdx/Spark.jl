
abstract RDD

type JavaRDD <: RDD
    jrdd::JJavaRDD
end

type JuliaRDD <: RDD
    jrdd::JJuliaRDD
end

function JuliaRDD(parent::RDD, command::Array{jbyte, 1})        
    jjulia_rdd = jcall(JJuliaRDD, "fromJavaRDD", JJuliaRDD,
                       (JJavaRDD, Array{jbyte, 1}),
                       parent.jrdd, command)
    JuliaRDD(jjulia_rdd)
end
    

type PipelinedRDD <: RDD
    # placeholder for now
end


