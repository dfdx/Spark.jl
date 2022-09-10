using Spark
using Spark.Compiler
import DataFrames

data_dir = joinpath(@__DIR__, "data")

@testset "collect_df" begin
    numbers = spark.sql("SELECT * FROM range(-3, 0)").collect_df()
    @test numbers == DataFrames.DataFrame(id = [-3, -2, -1])

    people = spark.read.json(joinpath(data_dir, "people.json")).collect_df()

    @test isequal(people.name, ["Peter", "Belle"])

    nested = spark.read.json(joinpath(data_dir, "nestedStructures.json")).collect_df()
    pet_names = [ p.name for p in nested.pets |> Iterators.flatten |> collect ]
    @test isequal(pet_names, [ "Albert", missing, missing, missing ])
end

@testset "createDataFrame" begin
    nested = spark.read.json(joinpath(data_dir, "nestedStructures.json")).collect_df()
    spark_df = spark.createDataFrame(nested)
    @test isequal(spark_df.collect_df(), nested)
end
