from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    count,
    countDistinct,
    when,
    coalesce,
    sha2,
    concat_ws,
    sort_array,
    collect_list,
    first,
)
from pyspark.sql.types import StringType


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

    include_global_summary = get_bool_param("include_global_summary", True)
    use_source_checksum_as_destination = get_bool_param("use_source_checksum_as_destination", True)

    # =====================================
    # Input Contract Resolver Pattern
    # Acepta salida de pys_read_normalize con contrato flexible
    # =====================================
    input_cols = set(df.columns)

    if "dataset" not in input_cols:
        df = df.withColumn("dataset", lit("default_dataset"))
    if "batch_id" not in input_cols:
        df = df.withColumn("batch_id", lit("batch_not_provided"))
    if "source_file" not in input_cols and "path" in input_cols:
        df = df.withColumn("source_file", col("path"))
    if "source_file" not in set(df.columns):
        df = df.withColumn("source_file", lit(None).cast(StringType()))
    if "record_status" not in set(df.columns):
        df = df.withColumn("record_status", lit("PROCESADO"))
    if "error_message" not in set(df.columns):
        df = df.withColumn("error_message", lit(None).cast(StringType()))

    # =====================================
    # Domain Column Resolver Pattern
    # Define columnas de negocio para checksums de contenido
    # =====================================
    technical_cols = {
        "ingestion_ts",
        "source_file",
        "path",
        "batch_id",
        "dataset",
        "record_status",
        "error_stage",
        "error_message",
    }
    business_cols = [c for c in df.columns if c not in technical_cols]

    if not business_cols:
        business_cols = ["source_file"]

    row_fingerprint_cols = [coalesce(col(c).cast("string"), lit("")) for c in business_cols]
    with_checksums = df.withColumn("row_checksum", sha2(concat_ws("||", *row_fingerprint_cols), 256))

    # =====================================
    # Audit-Write-Audit Pattern
    # Construye métricas de ingesta por dataset y batch
    # =====================================
    summary = (
        with_checksums
        .groupBy("dataset", "batch_id")
        .agg(
            count(lit(1)).alias("cantidad_registros_totales"),
            count(when(~col("record_status").rlike("^ERROR"), True)).alias("cantidad_registros_leidos"),
            count(when(col("record_status") == "PROCESADO", True)).alias("cantidad_registros_insertados"),
            count(when(col("record_status").rlike("^ERROR"), True)).alias("cantidad_registros_error"),
            countDistinct("source_file").alias("cantidad_archivos"),
            first(when(col("record_status").rlike("^ERROR"), col("error_message")), ignorenulls=True).alias("error"),
            sha2(concat_ws("||", sort_array(collect_list("row_checksum"))), 256).alias("checksum_source"),
        )
        .withColumn("ingestion_summary_ts", current_timestamp())
    )

    if use_source_checksum_as_destination:
        summary = summary.withColumn("checksum_destination", col("checksum_source"))
    else:
        summary = summary.withColumn("checksum_destination", lit(None).cast(StringType()))

    summary = summary.withColumn(
        "estado_ingesta",
        when(col("cantidad_registros_error") > 0, lit("COMPLETADO_CON_ERRORES")).otherwise(lit("COMPLETADO"))
    )

    # =====================================
    # Aggregate Snapshot Pattern
    # Agrega una fila global opcional para reporting del lote
    # =====================================
    if include_global_summary:
        global_summary = (
            summary
            .groupBy()
            .agg(
                lit("GLOBAL").alias("dataset"),
                lit("GLOBAL").alias("batch_id"),
                count(lit(1)).alias("datasets_resumidos"),
                first("ingestion_summary_ts").alias("ingestion_summary_ts"),
                first("estado_ingesta").alias("estado_ingesta"),
            )
        )

        global_summary = (
            global_summary
            .withColumn("cantidad_registros_totales", lit(None).cast("long"))
            .withColumn("cantidad_registros_leidos", lit(None).cast("long"))
            .withColumn("cantidad_registros_insertados", lit(None).cast("long"))
            .withColumn("cantidad_registros_error", lit(None).cast("long"))
            .withColumn("cantidad_archivos", lit(None).cast("long"))
            .withColumn("error", lit(None).cast(StringType()))
            .withColumn("checksum_source", lit(None).cast(StringType()))
            .withColumn("checksum_destination", lit(None).cast(StringType()))
            .select(summary.columns)
        )

        summary = summary.unionByName(global_summary, allowMissingColumns=True)

    return summary
