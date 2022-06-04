using Dates

@testset "Convert" begin
    # create DateTime without fractional part
    t = now(Dates.UTC) |> datetime2unix |> floor |> unix2datetime
    d = Date(t)

    @test convert(DateTime, convert(Spark.JTimestamp, t)) == t
    @test convert(Date, convert(Spark.JDate, d)) == d
end