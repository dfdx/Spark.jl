module Spark

export
    SparkConfig,
    SparkContext,
    RDD,
    JuliaRDD,
    JavaRDD,
    text_file,
    parallelize,
    map,
    map_pair,
    map_partitions,
    map_partitions_pair,
    map_partitions_with_index,
    reduce,
    filter,
    collect,
    count,
    id,
    num_partitions,
    close,
    @attach,
    share_variable,
    @share,
    flat_map,
    flat_map_pair,
    cartesian,
    group_by_key,
    reduce_by_key,
    cache,
    repartition,
    coalesce,
    pipe,
    # SQL
    SparkSession,
    Dataset,
    sql,
    read_json,
    write_json,
    read_parquet,
    write_parquet,
    read_df,
    write_df
    
    

include("core.jl")

end 
