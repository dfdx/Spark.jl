using Documenter, Spark

#load_dir(x) = map(file -> joinpath("lib", x, file), readdir(joinpath(Base.source_dir(), "src", "lib", x)))

makedocs(
    modules = [Spark],
    clean = false,
    format = [:html],
    sitename = "Spark",
    pages = Any[
        "Introduction" => "index.md",
        "API Reference" => "api.md"
    ]
)

deploydocs(
    repo   = "github.com/dfdx/Spark.jl.git",
    julia  = "0.6",
    osname = "linux",
    deps   = nothing,
    make   = nothing,
    target = "build",
)