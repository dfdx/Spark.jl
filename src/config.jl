
type SparkConf
    jconf::JSparkConf
end

function SparkConf(;opts...)
    jconf = JSparkConf(())
    opts = Dict(opts)
    for (k, v) in opts
        jcall(jconf, "set", JSparkConf, (JString, JString), string(k), v)
    end
    return SparkConf(jconf)
end
    

function Base.show(io::IO, conf::SparkConf)    
    print(io, "SparkConf()")
end


function Base.setindex!(conf::SparkConf, key::AbstractString, val::AbstractString)
    jcall(conf.jconf, "set", JSparkConf, (JString, JString), key, val)
end


function Base.getindex(conf::SparkConf, key::AbstractString)
    jcall(conf.jconf, "get", JString, (JString,), key)
end


function Base.get(conf::SparkConf, key::AbstractString, default::AbstractString)
    jcall(conf.jconf, "get", JString, (JString, JString), key, default)
end


function setmaster(conf::SparkConf, master::AbstractString)
    jcall(conf.jconf, "setMaster", JSparkConf, (JString,), master)
end


function setappname(conf::SparkConf, appname::AbstractString)
    jcall(conf.jconf, "setAppName", JSparkConf, (JString,), appname)
end
