
struct SparkSession
    jsess::JSparkSession
end

function SparkSession()
    builder = jcall(JSparkSession, "builder", JSparkSession, ())
end
