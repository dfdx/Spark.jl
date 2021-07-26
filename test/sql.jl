@testset "sql" begin

using DataFrames

sess = SparkSession()

Spark.set_log_level(Spark.context(sess), "ERROR")

# testing IO
ds = read_json(sess, joinpath(@__DIR__, "people.json"))
@test count(ds) == 2

mktempdir() do dir
    parquet_file = joinpath(dir, "people.parquet")
    
    write_parquet(ds, parquet_file)    
    ds2 = read_parquet(sess, parquet_file)
    write_json(ds2, joinpath(dir, "people2.json"))

    @test count(ds2) == 2
end

# testing sql
Spark.create_temp_view(ds, "people")
ds_all = sql(sess, "SELECT * from people")

@test Spark.explain_string(local_checkpoint(ds_all)) == "== Physical Plan ==\n*(1) Scan ExistingRDD[age#7L,name#8]\n\n"

@test Spark.as_named_tuple(Spark.head(ds_all)) == (age = 32, name = "Peter")

# test conversion to DataFrames
df = DataFrame(ds_all)
@test nrow(df) == 2
@test ncol(df) == 2

close(sess)

end
