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

@test Spark.head(ds_all) == (age = 32, name = "Peter")

# test conversion to DataFrames
df = DataFrame(ds_all)
@test nrow(df) == 2
@test ncol(df) == 2

# test Arrow conversions to/from DataFrames

singlerowtable = collect_to_dataframe(sql(sess, "select 1 as a,'my_string' as b"))
@test singlerowtable == DataFrame(a=[1], b=["my_string"])

numbers = Spark.range(sess, 1, 10000) |> collect_to_dataframe
@test length(numbers.id) == 9999

create_or_replace_temp_view(create_df(sess, numbers), "numbers")

@test count(Spark.table(sess, "numbers")) == 9999

complexdata = sql(sess, """
    select id as a,
           id * 2 as b,
           array(id + 1) as arr,
           map('xxxxx', id, 'x', id) as m,
           struct(string(id) as nn, id as n) as nested,
           double(id/2.0) as dec
    from numbers""") |> collect_to_tuples
@test complexdata[1].a == 1
@test complexdata[1].b == 2
@test complexdata[1].arr == [2]
@test complexdata[1].m == Dict("xxxxx" => 1, "x" => 1)
@test complexdata[1].nested.nn == "1"
@test complexdata[1].dec == 0.5

describe_df = DataFrame(complexdata) |> create_df(sess) |> Spark.describe |> collect_to_dataframe

@test describe_df.summary == ["count", "mean", "stddev", "min", "max"]
@test describe_df.a[1] == "9999"
@test describe_df.a[2] == "5000.0"

@test Spark.range(sess, 0, 888) |> Spark.limit(20) |> count == 20
@test sql(sess, "select 1 as a, array(1) as b") |> Spark.head == (a=1, b=[2])

close(sess)

end
