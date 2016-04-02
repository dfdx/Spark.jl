
# playground and demo code

include("core.jl")

# all *_demo functions suppose SparkContext is initialized and available as `sc`

function count_demo()
    rdd = text_file(sc, "file:///var/log/syslog")
    println(count(rdd))
end


function main()
    sc = SparkContext()
    rdd = text_file(sc, "file:///var/log/syslog")
    rdd = map_partitions_with_index(rdd, (split, it) -> (println("SPLIT #$split"); it))
    count(rdd)
    close(sc)
end


