
function init()
    envcp = get(ENV, "CLASSPATH", "")
    sparkjlassembly = joinpath(dirname(@__FILE__), "..", "jvm", "sparkjl", "target", "sparkjl-0.1-assembly.jar")
    classpath = @static is_windows() ? "$envcp;$sparkjlassembly" : "$envcp:$sparkjlassembly"
    try
        # prevent exceptions in REPL on code reloading
        JavaCall.init(["-ea", "-Xmx1024M", "-Djava.class.path=$classpath"])
    end
end
