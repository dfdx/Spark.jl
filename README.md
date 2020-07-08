# Spark.jl

A Julia interface to Apache Spark™

| **Latest Version** | **Documentation** | **PackageEvaluator** | **Build Status** |
|:------------------:|:-----------------:|:--------------------:|:----------------:|
| [![][version-img]][version-url] | [![][docs-latest-img]][docs-latest-url] | [![PkgEval][pkgeval-img]][pkgeval-url]  | [![][travis-img]][travis-url] [![][appveyor-img]][appveyor-url]  |



Spark.jl is a package that allows the execution of Julia programs on the Apache Spark platform. It supports running pure Julia scripts on Julia data structures, while utilising the data and code distribution capabalities of Apache Spark. It supports multiple cluster types (in client mode), and can be consider as an analogue to PySpark or RSpark within the Julia ecosystem. 

## Installation

Spark.jl requires at least Java 7 and [Maven](https://maven.apache.org/) to be installed and available in `PATH`.

```julia
Pkg.add("Spark.jl")
```

This will download and build all Julia and Java dependencies. To use Spark.jl type:

```julia
using Spark
Spark.init()
sc = SparkContext(master="local")
```

## Documentation

- [**LATEST**][docs-latest-url] &mdash; *in-development version of the documentation.*

## Project Status

The package is tested against Julia `1.0`, `1.4` and Java 8 and 11. It's also been tested on Amazon EMR and Azure HDInsight. While large cluster modes have been primarily tested on Linux, OS X and Windows do work for local development. See the [roadmap][roadmap-url] for current status.

Contributions are very welcome, as are feature requests and suggestions. Please open an [issue][issues-url] if you encounter any problems. 

## Trademarks

Apache®, [Apache Spark and Spark](http://spark.apache.org) are registered trademarks, or trademarks of the [Apache Software Foundation](http://www.apache.org/) in the United States and/or other countries.

[docs-latest-img]: https://img.shields.io/badge/docs-latest-blue.svg
[docs-latest-url]: http://dfdx.github.io/Spark.jl/

[docs-stable-img]: https://img.shields.io/badge/docs-stable-blue.svg
[docs-stable-url]: http://dfdx.github.io/Spark.jl/

[travis-img]: https://travis-ci.org/dfdx/Spark.jl.svg?branch=master
[travis-url]: https://travis-ci.org/dfdx/Spark.jl

[appveyor-img]: https://ci.appveyor.com/api/projects/status/vf5w4l37icc8m35q?svg=true
[appveyor-url]: https://ci.appveyor.com/project/dfdx/spark-jl

[codecov-img]: https://codecov.io/gh/dfdx/Spark.jl/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/dfdx/Spark.jl

[issues-url]: https://github.com/dfdx/Spark.jl/issues

[pkgeval-img]: https://juliahub.com/docs/Spark/pkgeval.svg
[pkgeval-url]: https://juliahub.com/ui/Packages/Spark/zpJEw

[version-img]: https://juliahub.com/docs/Spark/version.svg
[version-url]: https://juliahub.com/ui/Packages/Spark/zpJEw

[roadmap-url]: https://github.com/dfdx/Spark.jl/issues/1
