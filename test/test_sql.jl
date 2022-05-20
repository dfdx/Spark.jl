using Spark.SQL


@testset "Builder" begin
    cnf = spark.conf.getAll()
    @test cnf["spark.app.name"] == "Hello"
    @test cnf["spark.master"] == "local"
    @test cnf["some.key"] == "some-value"
end

@testset "RuntimeConfig" begin
    @test spark.conf.get("spark.app.name") == "Hello"
    spark.conf.set("another.key", "another-value")
    @test spark.conf.get("another.key") == "another-value"
    @test spark.conf.get("non.existing", "default-value") == "default-value"

end

@testset "DataFrame" begin
    rows = [Row(name="Alice", age=12), Row(name="Bob", age=32)]
    @test spark.createDataFrame(rows) isa DataFrame
    @test spark.createDataFrame(rows, StructType("name string, age long")) isa DataFrame

    df = spark.createDataFrame(rows)
    @test df.columns() == ["name", "age"]
    @test df.first() == rows[1]
    @test df.head() == rows[1]
    @test df.head(2) == rows
    @test df.take(1) == rows[1:1]
    @test df.collect() == rows
    @test df.count() == 2

    @test df.select("age", "name").columns() == ["age", "name"]
    rows = df.select(Column("age") + 1).collect()
    @test [row[1] for row in rows] == [13, 33]

    rows = df.withColumn("inc_age", df.age + 1).collect()
    @test [row[3] for row in rows] == [13, 33]

    @test df.filter(df.name == "Alice").first().age == 12

end

@testset "GroupedData" begin
    data = [
        ["red", "banana", 1, 10], ["blue", "banana", 2, 20], ["red", "carrot", 3, 30],
        ["blue", "grape", 4, 40], ["red", "carrot", 5, 50], ["black", "carrot", 6, 60],
        ["red", "banana", 7, 70], ["red", "grape", 8, 80]
    ]
    rows = [Row(color=d[1], fruit=d[2], v1=d[3], v2=d[4]) for d in data]
    df = spark.createDataFrame(rows)

    group = df.groupby("fruit")
    @test group isa GroupedData

end

@testset "Column" begin

    col = Column("amount")
    for func in (+, -, *, /)
        @test func(col, 1) isa Column
        @test func(col, 1.0) isa Column
    end

    @test col.alias("money") isa Column
    @test col.asc() isa Column
    @test col.asc_nulls_first() isa Column
    @test col.asc_nulls_last() isa Column

    @test col.between(1, 2) isa Column
    @test col.bitwiseAND(1) isa Column
    @test col & 1 isa Column
    @test col.bitwiseOR(1) isa Column
    @test col | 1 isa Column
    @test col.bitwiseXOR(1) isa Column
    @test col ⊻ 1 isa Column

    @test col.contains("a") isa Column

    @test col.desc() isa Column
    @test col.desc_nulls_first() isa Column
    @test col.desc_nulls_last() isa Column

    # prints 'Exception in thread "main" java.lang.NoSuchMethodError: endsWith'
    # but seems to work
    @test col.endswith("a") isa Column
    @test col.endswith(Column("other")) isa Column

    @test col.eqNullSafe("other") isa Column
    @test (col == Column("other")) isa Column
    @test (col == "abc") isa Column
    @test (col != Column("other")) isa Column
    @test (col != "abc") isa Column

    col.explain()   # smoke test

    @test col.isNull() isa Column
    @test col.isNotNull() isa Column

    @test col.like("abc") isa Column
    @test col.rlike("abc") isa Column

    @test_broken col.when(Column("flag"), 1).otherwise("abc") isa Colon

    @test col.over() isa Column

    # also complains about NoSuchMethodError, but seems to work
    @test col.startswith("a") isa Column
    @test col.startswith(Column("other")) isa Column

    @test col.substr(Column("start"), Column("len")) isa Column
    @test col.substr(0, 3) isa Column
end


@testset "Reader/Writer" begin
    spark = SparkSession.builder.master("local").getOrCreate()

    # for REPL:
    # data_dir = joinpath(@__DIR__, "test", "data")
    data_dir = joinpath(@__DIR__, "data")
    df = spark.read.json(joinpath(data_dir, "people.json"))

    spark.stop()
end


@testset "StructType" begin
    st = StructType()
    @test length(st.fieldNames()) == 0

    st = StructType(
        StructField("name", "string", false),
        StructField("age", "int", true)
    )
    @test st[1] == StructField("name", "string", false)
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
