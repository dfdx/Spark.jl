# Spark.jl

A Julia interface to Apache Spark™

| **Latest Version** | **Documentation** | **PackageEvaluator** | **Build Status** |
|:------------------:|:-----------------:|:--------------------:|:----------------:|
| [![][version-img]][version-url] | [![][docs-latest-img]][docs-latest-url] | [![PkgEval][pkgeval-img]][pkgeval-url]  | [![][gh-test-img]][gh-test-url]  |



Spark.jl provides an interface to Apache Spark™ platform, including SQL / DataFrame and Structured Streaming. It closely follows the PySpark API, making it easy to translate existing Python code to Julia.

Spark.jl supports multiple cluster types (in client mode), and can be considered as an analogue to PySpark or RSpark within the Julia ecosystem. It supports running within on-premise installations, as well as hosted instance such as Amazon EMR and Azure HDInsight.

**[Documentation][docs-latest-url]**


## Trademarks

Apache®, [Apache Spark and Spark](http://spark.apache.org) are registered trademarks, or trademarks of the [Apache Software Foundation](http://www.apache.org/) in the United States and/or other countries.

[docs-latest-img]: https://img.shields.io/badge/docs-latest-blue.svg
[docs-latest-url]: http://dfdx.github.io/Spark.jl/

[docs-stable-img]: https://img.shields.io/badge/docs-stable-blue.svg
[docs-stable-url]: http://dfdx.github.io/Spark.jl/

[gh-test-img]: https://github.com/dfdx/Spark.jl/actions/workflows/test.yml/badge.svg
[gh-test-url]: https://github.com/dfdx/Spark.jl/actions/workflows/test.yml

[codecov-img]: https://codecov.io/gh/dfdx/Spark.jl/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/dfdx/Spark.jl

[issues-url]: https://github.com/dfdx/Spark.jl/issues

[pkgeval-img]: https://juliahub.com/docs/Spark/pkgeval.svg
[pkgeval-url]: https://juliahub.com/ui/Packages/Spark/zpJEw

[version-img]: https://juliahub.com/docs/Spark/version.svg
[version-url]: https://juliahub.com/ui/Packages/Spark/zpJEw
