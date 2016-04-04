
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
    rdd = text_file(sc, path)
    assigntype!(rdd, UTF8String)  # TODO: add separate source type (for decoding from Java)
                                  # TODO: move both to a `meta::Dict{Symbol,Any}` field
    collect(rdd)
    
    rdd = map_partitions_with_index(rdd, (split, it) -> map(length, it))
    count(rdd)
    close(sc)
end


