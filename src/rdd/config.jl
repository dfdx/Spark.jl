
mutable struct SparkConf
    jconf::JSparkConf
end

function SparkConf(;opts...)
    opts = Dict(opts)
    return SparkConf(opts)
end

function SparkConf(opts::Dict)
    jconf = JSparkConf(())
    for (k, v) in opts
        jcall(jconf, "set", JSparkConf, (JString, JString), string(k), v)
    end
    return SparkConf(jconf)
end


function Base.show(io::IO, conf::SparkConf)
    print(io, "SparkConf()")
end


function Base.setindex!(conf::SparkConf, val::AbstractString, key::AbstractString)
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

function setdeploy(conf::SparkConf, deploymode::AbstractString)
    jcall(conf.jconf, "set", JSparkConf, (JString, JString), "deploy-mode", deploymode)
end
