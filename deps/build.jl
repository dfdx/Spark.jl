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

if Sys.iswindows()   
    # on Windows Spark requires HADOOP_HOME variable to be defined and contain file `bin/winutils.exe`
    # here we create such a file in deps/hadoop/bin
    if !haskey(ENV, "HADOOP_HOME")
        HADOOP_COMMONS_URL = ("https://github.com/steveloughran/winutils/blob/master/" *
                              "hadoop-2.7.1/bin/winutils.exe")
        hadoop_bin_path = joinpath(@__DIR__, "hadoop", "bin")
        
        if !isdir(hadoop_bin_path)
            mkpath(hadoop_bin_path)
        end
        download(HADOOP_COMMONS_URL, joinpath(hadoop_bin_path, "winutils.exe"))
        ENV["HADOOP_HOME"] = dirname(hadoop_bin_path)
        
    end
end
