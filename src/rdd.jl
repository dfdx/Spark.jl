
abstract RDD

type JavaRDD <: RDD
    jrdd::JJavaRDD
end

type JuliaRDD <: RDD
    jrdd::JJuliaRDD
end

function JuliaRDD(parent::RDD)
    # for now assuming parent is JavaRDD (though probably it won't matter)
    JJuliaRDD((JJavaRDD, JArray), master, appname)
end
    

type PipelinedRDD <: RDD
    # placeholder for now
end


