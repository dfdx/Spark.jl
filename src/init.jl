global const SPARK_DEFAULT_PROPS = Dict()

function init()
    JavaCall.addClassPath(get(ENV, "CLASSPATH", ""))
    defaults = load_spark_defaults(SPARK_DEFAULT_PROPS)
    shome =  get(ENV, "SPARK_HOME", "")
    if !isempty(shome)
        for x in readdir(joinpath(shome, "jars"))
            JavaCall.addClassPath(joinpath(shome, "jars", x))
        end
        JavaCall.addClassPath(joinpath(dirname(@__FILE__), "..", "jvm", "sparkjl", "target", "sparkjl-0.1.jar"))
    else
        JavaCall.addClassPath(joinpath(dirname(@__FILE__), "..", "jvm", "sparkjl", "target", "sparkjl-0.1-assembly.jar"))
    end
    for y in split(get(ENV, "SPARK_DIST_CLASSPATH", ""), [':',';'], keep=false)
        JavaCall.addClassPath(String(y))
    end
    for z in split(get(defaults, "spark.driver.extraClassPath", ""), [':',';'], keep=false)
        JavaCall.addClassPath(String(z))
    end
    JavaCall.addClassPath(get(ENV, "HADOOP_CONF_DIR", ""))
    JavaCall.addClassPath(get(ENV, "YARN_CONF_DIR", ""))
    if get(ENV, "HDP_VERSION", "") == ""
       try
           ENV["HDP_VERSION"] = pipeline(`hdp-select status` , `grep spark2-client` , `awk -F " " '{print $3}'`) |> readstring |> strip
       catch
       end
    end

    for y in split(get(defaults, "spark.driver.extraJavaOptions", ""), " ", keep=false)
        JavaCall.addOpts(String(y))
    end
    s = get(defaults, "spark.driver.extraLibraryPath", "")
    try
        JavaCall.addOpts("-Djava.library.path=$(defaults["spark.driver.extraLibraryPath"])")
    catch; end
    JavaCall.addOpts("-ea")
    JavaCall.addOpts("-Xmx1024M")
    try
        JavaCall.init()
    end
end

function load_spark_defaults(d::Dict)
    sconf = get(ENV, "SPARK_CONF", "")
    if sconf == ""
        shome =  get(ENV, "SPARK_HOME", "")
        if shome == "" ; return d; end
        sconf = joinpath(shome, "conf")
    end
    p = split(readstring(joinpath(sconf, "spark-defaults.conf")), '\n', keep=false)
    for x in p
         if !startswith(x, "#") && !isempty(strip(x))
             y=split(x, " ", limit=2)
             d[y[1]]=strip(y[2])
         end
    end
    return d
end
