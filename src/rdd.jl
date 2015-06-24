
abstract RDD

type JavaRDD <: RDD
    jrdd::JJavaRDD
end

type JuliaRDD <: RDD
    jrdd::JJuliaRDD
end

type PipelinedRDD <: RDD
    # placeholder for now
end


