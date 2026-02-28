def pyspark_transform(spark, df, param_dict):

    raw_path = f"s3a://{param_dict['bucket_raw']}/{param_dict['raw_prefix']}"
    checkpoint_path = f"s3a://{param_dict['bucket_curated']}/{param_dict['checkpoint_prefix']}"

    files_df = spark.read.format("binaryFile").load(raw_path).select("path")

    try:
        processed = spark.read.parquet(checkpoint_path).select("path")
        pending = files_df.join(processed, "path", "left_anti")
    except:
        pending = files_df

    return pending
