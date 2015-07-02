
abstract RDD

type JavaRDD <: RDD
    jrdd::JJavaRDD
end

type JuliaRDD <: RDD
    jrdd::JJuliaRDD
end

function JuliaRDD(parent::RDD)        
    julia_rdd = JJuliaRDD((JJavaRDD, JArray), master, appname)
end
    

type PipelinedRDD <: RDD
    # placeholder for now
end


