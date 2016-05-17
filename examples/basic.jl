
using Spark

sc = SparkContext(master="local")
path = "file:///var/log/syslog"
txt = text_file(sc, path)
rdd = map_partitions(txt, it -> map(s -> length(split(s)), it))
count(rdd)
reduce(rdd, +)
close(sc)
