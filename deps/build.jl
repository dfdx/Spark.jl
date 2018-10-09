mvn = Sys.iswindows() ? "mvn.cmd" : "mvn"
which = Sys.iswindows() ? "where" : "which"

try
    run(`$which $mvn`)
catch
    error("Cannot find maven. Is it installed?")
end

SPARK_VERSION = get(ENV, "BUILD_SPARK_VERSION", "2.1.0")

cd(joinpath(dirname(@__DIR__), "jvm/sparkjl")) do
    run(`$mvn clean package -Dspark.version=$SPARK_VERSION`)
end
