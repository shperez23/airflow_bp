from pyspark.sql.functions import col, lit, current_timestamp, coalesce, trim
from pyspark.sql.types import StructType, StructField, StringType


def pyspark_transform(spark, df, param_dict):

    # =====================================
    # Parameter Guard Pattern
    # Normaliza parámetros opcionales y evita errores por null/vacío
    # =====================================
    def is_missing(value):
        return value is None or (isinstance(value, str) and value.strip() == "")

    def get_bool_param(name, default):
        raw_value = param_dict.get(name, default)
        if isinstance(raw_value, bool):
            return raw_value
        if raw_value is None:
            return default

        normalized = str(raw_value).strip().lower()
        if normalized in {"true", "1", "yes", "y", "si", "sí"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
        return default

    include_skipped = get_bool_param("include_skipped", False)
    deduplicate_errors = get_bool_param("deduplicate_errors", True)

    # =====================================
    # Multi-Input Resolver Pattern
    # Resuelve entradas cuando el orquestador envía múltiples dataframes
    # =====================================
    flow_inputs = df if hasattr(df, "get") else {}

    upload_df = flow_inputs.get("pys_upload") if hasattr(flow_inputs, "get") else None
    discovery_df = flow_inputs.get("pys_discovery_node") if hasattr(flow_inputs, "get") else None
    read_df = flow_inputs.get("pys_read_normalize") if hasattr(flow_inputs, "get") else None
    summary_df = flow_inputs.get("pys_ingestion_summary") if hasattr(flow_inputs, "get") else None

    if not hasattr(df, "get"):
        input_cols = set(df.columns)
        if {"full_path", "status", "error_stage"}.issubset(input_cols):
            upload_df = df
        elif {"discovery_status", "source_file"}.issubset(input_cols):
            discovery_df = df
        elif {"record_status", "error_stage"}.issubset(input_cols):
            read_df = df
        elif {"estado_ingesta", "error"}.issubset(input_cols):
            summary_df = df

    # =====================================
    # Error Contract Pattern
    # Define esquema canónico para consolidar errores multi-etapa
    # =====================================
    consolidated_schema = StructType([
        StructField("error_consolidation_ts", StringType(), True),
        StructField("flow_stage", StringType(), True),
        StructField("error_code", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("path", StringType(), True),
        StructField("dataset", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("error_origin", StringType(), True),
    ])

    # =====================================
    # Error Adapter Pattern
    # Adapta cada contrato de salida al esquema canónico de error
    # =====================================
    def map_upload_errors(stage_df):
        if stage_df is None:
            return None

        valid_status_expr = col("status").rlike("^ERROR")
        if include_skipped:
            valid_status_expr = col("status").rlike("^(ERROR|SKIPPED)")

        return (
            stage_df
            .where(valid_status_expr)
            .select(
                lit(None).cast("string").alias("error_consolidation_ts"),
                lit("UPLOAD").alias("flow_stage"),
                col("status").cast("string").alias("error_code"),
                coalesce(col("error_message").cast("string"), col("status").cast("string")).alias("error_message"),
                col("full_path").cast("string").alias("source_file"),
                lit(None).cast("string").alias("path"),
                lit(None).cast("string").alias("dataset"),
                lit(None).cast("string").alias("batch_id"),
                lit("pys_upload").alias("error_origin"),
            )
        )

    def map_discovery_errors(stage_df):
        if stage_df is None:
            return None

        valid_status_expr = col("discovery_status").rlike("^ERROR")
        if include_skipped:
            valid_status_expr = col("discovery_status").rlike("^(ERROR|SKIPPED)")

        return (
            stage_df
            .where(valid_status_expr)
            .select(
                lit(None).cast("string").alias("error_consolidation_ts"),
                lit("DISCOVERY").alias("flow_stage"),
                col("discovery_status").cast("string").alias("error_code"),
                coalesce(col("error_message").cast("string"), col("discovery_status").cast("string")).alias("error_message"),
                col("source_file").cast("string").alias("source_file"),
                col("path").cast("string").alias("path"),
                lit(None).cast("string").alias("dataset"),
                lit(None).cast("string").alias("batch_id"),
                lit("pys_discovery_node").alias("error_origin"),
            )
        )

    def map_read_errors(stage_df):
        if stage_df is None:
            return None

        return (
            stage_df
            .where(col("record_status").rlike("^ERROR"))
            .select(
                lit(None).cast("string").alias("error_consolidation_ts"),
                coalesce(col("error_stage").cast("string"), lit("READ_NORMALIZE")).alias("flow_stage"),
                col("record_status").cast("string").alias("error_code"),
                coalesce(col("error_message").cast("string"), col("record_status").cast("string")).alias("error_message"),
                col("source_file").cast("string").alias("source_file"),
                col("path").cast("string").alias("path"),
                col("dataset").cast("string").alias("dataset"),
                col("batch_id").cast("string").alias("batch_id"),
                lit("pys_read_normalize").alias("error_origin"),
            )
        )

    def map_summary_errors(stage_df):
        if stage_df is None:
            return None

        return (
            stage_df
            .where((col("estado_ingesta") == "COMPLETADO_CON_ERRORES") | (col("error").isNotNull() & (trim(col("error")) != "")))
            .select(
                lit(None).cast("string").alias("error_consolidation_ts"),
                lit("INGESTION_SUMMARY").alias("flow_stage"),
                lit("ERROR_SUMMARY").alias("error_code"),
                coalesce(col("error").cast("string"), col("estado_ingesta").cast("string")).alias("error_message"),
                lit(None).cast("string").alias("source_file"),
                lit(None).cast("string").alias("path"),
                col("dataset").cast("string").alias("dataset"),
                col("batch_id").cast("string").alias("batch_id"),
                lit("pys_ingestion_summary").alias("error_origin"),
            )
        )

    staged_errors = [
        map_upload_errors(upload_df),
        map_discovery_errors(discovery_df),
        map_read_errors(read_df),
        map_summary_errors(summary_df),
    ]

    # =====================================
    # Consolidated Observer Pattern
    # Unifica eventos de error de todo el flujo en un solo dataframe
    # =====================================
    consolidated = None
    for stage_error_df in staged_errors:
        if stage_error_df is None:
            continue
        consolidated = stage_error_df if consolidated is None else consolidated.unionByName(stage_error_df, allowMissingColumns=True)

    if consolidated is None:
        return spark.createDataFrame([], consolidated_schema)

    consolidated = consolidated.withColumn("error_consolidation_ts", current_timestamp().cast("string"))

    if deduplicate_errors:
        consolidated = consolidated.dropDuplicates([
            "flow_stage",
            "error_code",
            "error_message",
            "source_file",
            "path",
            "dataset",
            "batch_id",
            "error_origin",
        ])

    return consolidated.select([field.name for field in consolidated_schema.fields])
