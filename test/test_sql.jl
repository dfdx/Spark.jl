
@testset "builder" begin
    spark = SparkSession.builder.
        appName("Hello").
        master("local").
        config("some.key", "some-value").
        getOrCreate()

    conf = config(spark)
    @test conf["spark.app.name"] == "Hello"
    @test conf["spark.master"] == "local"
    @test conf["some.key"] == "some-value"

    spark.stop()
end

@testset "column" begin

    col = Column("amount")
    for func in (+, -, *, /)
        @test func(col, 1) isa Column
        @test func(col, 1.0) isa Column
    end

    @test col + "!" isa Column

end


@testset "reader/writer" begin
    spark = SparkSession.builder.master("local").getOrCreate()

    # for REPL:
    # data_dir = joinpath(@__DIR__, "test", "data")
    data_dir = joinpath(@__DIR__, "data")
    df = spark.read.json(joinpath(data_dir, "people.json"))

    spark.stop()
end

# @testset "sql" begin

# using DataFrames

# sess = SparkSession()

# Spark.set_log_level(Spark.context(sess), "ERROR")

# # testing IO
# ds = read_json(sess, joinpath(@__DIR__, "people.json"))
# @test count(ds) == 2

# mktempdir() do dir
#     parquet_file = joinpath(dir, "people.parquet")

#     write_parquet(ds, parquet_file)
#     ds2 = read_parquet(sess, parquet_file)
#     write_json(ds2, joinpath(dir, "people2.json"))

#     @test count(ds2) == 2
# end

# # testing sql
# Spark.create_temp_view(ds, "people")
# ds_all = sql(sess, "SELECT * from people")

# @test Spark.as_named_tuple(Spark.head(ds_all)) == (age = 32, name = "Peter")

# # test conversion to DataFrames
# df = DataFrame(ds_all)
# @test nrow(df) == 2
# @test ncol(df) == 2

# close(sess)

# end
