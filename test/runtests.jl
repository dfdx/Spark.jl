using Sparta
using Base.Test

# write your own tests here
sc = SparkContext()
path = "file:///var/log/syslog"
# path = "/home/azhabinski/data/dt=20151011/20151010234634882-worker-006-9.gz"
txt = text_file(sc, path)
rdd = map_partitions(txt, it -> map(s -> length(split(s)), it))       
count(rdd)
reduce(rdd, +)
close(sc)
