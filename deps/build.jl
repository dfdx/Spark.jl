
try
    run(`which mvn`)
catch
    error("Cannot find maven. Is it installed")
end

cd("../jvm/sparkjl")
run(`mvn clean package`)
