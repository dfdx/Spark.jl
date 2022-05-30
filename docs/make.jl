using Documenter
using Spark

makedocs(
    sitename = "Spark",
    format = Documenter.HTML(),
    modules = [Spark],
    pages = [
        "Main" => "index.md",
        "Cookbook" => "cookbook.md",
        "Build your own AD" => "design.md",
        "Reference" => "reference.md",
    ],
)

deploydocs(
    repo = "github.com/dfdx/Spark.jl.git",
    devbranch = "main",
)