using Documenter
using Spark

makedocs(
    sitename = "Spark",
    format = Documenter.HTML(),
    modules = [Spark],
    pages = Any[
        "Introduction" => "index.md",
        "SQL / DataFrames" => "sql.md",
        "Structured Streaming" => "streaming.md",
        "API Reference" => "api.md"
    ],
)

deploydocs(
    repo = "github.com/dfdx/Spark.jl.git",
    devbranch = "main",
)