using Spark.SQL


@testset "Builder" begin
    cnf = spark.conf.getAll()
    @test cnf["spark.app.name"] == "Hello"
    @test cnf["spark.master"] == "local"
    @test cnf["some.key"] == "some-value"
end

@testset "SparkSession" begin
    df = spark.sql("select 1 as num")
    @test df.collect("num") == [1]
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
    @test df.where(df.name == "Alice").first().age == 12

    df2 = spark.createDataFrame(
        [Any["Alice", "Smith"], ["Emily", "Clark"]],
        "first_name string, last_name string"
    )
    joined_df = df.join(df2, df.name == df2.first_name)
    @test joined_df.columns() == ["name", "age", "first_name", "last_name"]
    @test joined_df.count() == 1

    joined_df = df.join(df2, df.name == df2.first_name, "outer")
    @test joined_df.count() == 3

    df.createOrReplaceTempView("people")
    @test spark.sql("select count(*) from people").first()[1] == 2

end

@testset "GroupedData" begin
    data = [
        ["red", "banana", 1, 10], ["blue", "banana", 2, 20], ["red", "carrot", 3, 30],
        ["blue", "grape", 4, 40], ["red", "carrot", 5, 50], ["black", "carrot", 6, 60],
        ["red", "banana", 7, 70], ["red", "grape", 8, 80]
    ]
    sch = ["color string", "fruit string", "v1 long", "v2 long"]
    df = spark.createDataFrame(data, sch)

    gdf = df.groupby("fruit")
    @test gdf isa GroupedData

    df_agg = gdf.agg(min(df.v1), max(df.v2))
    @test df_agg.collect("min(v1)") == [4, 1, 3]
    @test df_agg.collect("max(v2)") == [80, 70, 60]

    df_agg = gdf.agg(Dict("v1" => "min", "v2" => "max"))
    @test df_agg.collect("min(v1)") == [4, 1, 3]
    @test df_agg.collect("max(v2)") == [80, 70, 60]

    @test gdf.sum("v1").select(mean(Column("sum(v1)"))).collect(1)[1] == 12.0

end

@testset "Column" begin

    col = Column("x")
    for func in (+, -, *, /)
        @test func(col, 1) isa Column
        @test func(col, 1.0) isa Column
    end

    @test col.alias("y") isa Column
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

    @test col.explode() |> string == """col("explode(x)")"""

    @test (col.window("10 minutes", "5 minutes", "15 minutes") |> string ==
            """col("timewindow(x, 600000000, 300000000, 900000000) AS `window`")""")
    @test (col.window("10 minutes", "5 minutes") |> string ==
            """col("timewindow(x, 600000000, 300000000, 0) AS `window`")""")
    @test (col.window("10 minutes") |> string ==
            """col("timewindow(x, 600000000, 600000000, 0) AS `window`")""")

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


@testset "Window" begin
    # how can we do these tests more robust?
    @test Window.partitionBy(Column("x")).orderBy(Column("y")) isa WindowSpec
    @test Window.partitionBy("x").orderBy("y") isa WindowSpec
    @test Window.partitionBy("x").orderBy("y").rowsBetween(-3, 3) isa WindowSpec
    @test Window.partitionBy("x").orderBy("y").rangeBetween(-3, 3) isa WindowSpec
    @test Window.partitionBy("x").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing) isa WindowSpec
    @test Window.partitionBy("x").rangeBetween(Window.unboundedPreceding, Window.currentRow) isa WindowSpec
end

@testset "Reader/Writer" begin
    # for REPL:
    # data_dir = joinpath(@__DIR__, "test", "data")
    data_dir = joinpath(@__DIR__, "data")
    mktempdir(; prefix="spark-jl-") do tmp_dir
        df = spark.read.json(joinpath(data_dir, "people.json"))
        df.write.mode("overwrite").parquet(joinpath(tmp_dir, "people.parquet"))
        df = spark.read.parquet(joinpath(tmp_dir, "people.parquet"))
        df.write.mode("overwrite").orc(joinpath(tmp_dir, "people.orc"))
        df = spark.read.orc(joinpath(tmp_dir, "people.orc"))
        @test df.collect("name") |> Set == Set(["Peter", "Belle"])
    end
end