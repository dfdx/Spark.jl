@testset "sql" begin

sess = SparkSession()

# testing IO
mktempdir() do dir
    parquet_file = joinpath(dir, "people.parquet")
    ds = read_json(sess, joinpath(@__DIR__, "people.json"))
    write_parquet(ds, parquet_file)    
    ds2 = read_parquet(sess, parquet_file)
    write_json(ds2, joinpath(dir, "people2.json"))
end    

close(sess)

end
