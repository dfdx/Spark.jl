
# playground and demo code

include("core.jl")

# all *_demo functions suppose SparkContext is initialized and available as `sc`

function count_demo()
    rdd = text_file(sc, "file:///var/log/syslog")
    println(count(rdd))
end


function main()
    sc = SparkContext()
    path = "file:///var/log/syslog"
    # path = "/home/azhabinski/data/dt=20151011/20151010234634882-worker-006-9.gz"
    txt = text_file(sc, path)
    rdd = map_partitions(txt, it -> map(s -> length(split(s)), it))       
    count(rdd)
    reduce(rdd, +)
    close(sc)
end

