
function init(;extra_cp="", extra_opts=[])
    if JavaCall.isloaded()
        warn("JVM already initialized, not reinitializing")
        return
    end
    envcp = get(ENV, "CLASSPATH", "")
    sparkjlassembly = joinpath(dirname(@__FILE__), "..", "jvm", "sparkjl", "target", "sparkjl-0.1-assembly.jar")
    delim = @static is_windows() ? ";" : ":"
    classpath = join([envcp, sparkjlassembly, extra_cp], delim)
    # try
        # prevent exceptions in REPL on code reloading
        JavaCall.init(vcat(["-ea", "-Xmx1024M", "-Djava.class.path=$classpath"], extra_opts))
    # end
end

