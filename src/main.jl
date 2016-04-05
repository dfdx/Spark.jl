
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
    txt = text_file(sc, path)
    assigntype!(txt, UTF8String)
    rdd = map_partitions_with_index(txt, (prt, it) -> map(s -> length(split(s)), it))
    assigntype!(rdd, Int)
    collect(rdd)
    count(rdd)
    close(sc)
end
