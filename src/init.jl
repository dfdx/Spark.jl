
function init()
    envcp = get(ENV, "CLASSPATH", "")
    spartaassembly = Pkg.dir("Sparta", "jvm", "sparta", "target", "sparta-0.1-assembly.jar")        
    classpath = "$envcp:$spartaassembly"
    try
        # prevent exceptions in REPL on code reloading
        JavaCall.init(["-ea", "-Xmx1024M", "-Djava.class.path=$classpath"])
    end
end

init()
