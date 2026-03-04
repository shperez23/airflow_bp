from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, StringType


def pyspark_transform(spark, df, param_dict):
    # =====================================
    # Parameter Guard Pattern
    # Normaliza parámetros para soportar contratos multi-nodo
    # =====================================
    def is_missing(value):
        return value is None or (isinstance(value, str) and value.strip() == "")

    def get_bool_param(name, default=False):
        raw_value = param_dict.get(name, default)
        if isinstance(raw_value, bool):
            return raw_value
        if is_missing(raw_value):
            return default
        normalized = str(raw_value).strip().lower()
        if normalized in {"1", "true", "yes", "y", "si", "sí"}:
            return True
        if normalized in {"0", "false", "no", "n"}:
            return False
        return default


    # =====================================
    # Multi-Input Resolver Pattern
    # Resuelve entradas cuando el orquestador envía varios dataframes
    # =====================================
    def resolve_input_frame(flow_inputs, preferred_keys):
        if not hasattr(flow_inputs, "get"):
            return flow_inputs

        for key in preferred_keys:
            candidate = flow_inputs.get(key)
            if candidate is not None:
                return candidate

        try:
            values = list(flow_inputs.values())
        except Exception:
            return None

        for candidate in values:
            if candidate is not None and hasattr(candidate, "columns"):
                return candidate

        return None

    input_df = resolve_input_frame(df, ["pys_read_normalize_node"])
    param_df = resolve_input_frame(df, ["tri_parametros_write"])

    if input_df is None or not hasattr(input_df, "columns"):
        raise ValueError(
            "No se encontró DataFrame de entrada para escritura. "
            "Verifique la salida de pys_read_normalize_node."
        )

    # =====================================
    # Error Interceptor Pattern
    # Solo escribe cuando no hay errores en read_normalize
    # =====================================
    normalized_status = col("record_status") if "record_status" in input_df.columns else lit("PROCESADO")
    errors_df = input_df.filter(normalized_status.startswith("ERROR"))
    if errors_df.limit(1).count() > 0:
        return input_df

    # =====================================
    # Empty Payload Guard Pattern
    # Evita escrituras innecesarias cuando read_normalize no produce registros
    # =====================================
    if input_df.limit(1).count() == 0:
        output_schema = StructType([
            StructField("nombre_tabla", StringType(), True),
            StructField("ruta_current", StringType(), True),
            StructField("ruta_history_delta", StringType(), True),
            StructField("enable_reprocess", StringType(), False),
            StructField("write_status", StringType(), False),
            StructField("write_message", StringType(), True),
        ])

        output_rows = [
            (
                None,
                None,
                None,
                str(get_bool_param("ENABLE_REPROCESS", False)).lower(),
                "OMITIDO_SIN_DATOS",
                "pys_read_normalize_node no retornó registros; se omite escritura current/history",
            )
        ]

        return spark.createDataFrame(output_rows, output_schema)

    # =====================================
    # External Trigger Pattern
    # Permite recibir parámetros por df de parametría o por param_dict
    # =====================================
    def get_value_from_param_df(param_source, field_name):
        if param_source is None:
            return None

        if hasattr(param_source, "first"):
            param_source = param_source.first()

        try:
            return param_source[field_name]
        except Exception:
            return None


    ruta_salida = (
        get_value_from_param_df(param_df, "RUTA_SALIDA")
    )
    nombre_tabla = (
        get_value_from_param_df(param_df, "NOMBRE_TABLA")
    )
    bucket_raw = (
        get_value_from_param_df(param_df, "BUCKET_RAW")
    )

    if is_missing(ruta_salida):
        raise ValueError("Falta parámetro requerido 'RUTA_SALIDA'")
    if is_missing(nombre_tabla):
        raise ValueError("Falta parámetro requerido 'NOMBRE_TABLA'")

    enable_reprocess = get_bool_param("ENABLE_REPROCESS", False)

    # =====================================
    # Dataset Materializer Pattern
    # Escribe snapshot actual en parquet para consumo operativo
    # =====================================
    sanitized_table_name = str(nombre_tabla).strip("/")
    output_root = str(ruta_salida).rstrip("/")
    current_path = f"{output_root}/{sanitized_table_name}"
    history_path = f"{output_root}/{sanitized_table_name}_shist"

    df_with_audit = (
        input_df
        .withColumn("write_ts", current_timestamp())
        .withColumn("table_name", lit(nombre_tabla))
    )

    if enable_reprocess:
        df_with_audit.write.mode("overwrite").parquet(current_path)
    else:
        df_with_audit.write.mode("errorifexists").parquet(current_path)

    # =====================================
    # Immutable Dataset Pattern
    # Conserva histórico por ejecución en Delta Lake
    # =====================================
    history_df = df_with_audit

    history_exists = False
    try:
        spark.read.format("delta").load(history_path).limit(1).count()
        history_exists = True
    except Exception:
        history_exists = False

    if enable_reprocess and history_exists:
        try:
            from delta.tables import DeltaTable

            delta_table = DeltaTable.forPath(spark, history_path)
            delta_table.delete(lit(True))
        except Exception:
            history_df = history_df.filter(lit(True))

    history_df.write.format("delta").mode("append").save(history_path)

    # =====================================
    # Readiness Marker + Checkpointer Pattern
    # Registra paths exitosos para que discovery no reprocese archivos ya escritos
    # =====================================
    checkpoint_prefix = param_dict.get("checkpoint_prefix")

    if (not is_missing(bucket_raw)) and (not is_missing(checkpoint_prefix)):
        checkpoint_path = f"s3a://{str(bucket_raw).strip('/')}/{str(checkpoint_prefix).strip('/')}"

        checkpoint_base = (
            input_df
            .where((~normalized_status.startswith("ERROR")) & col("path").isNotNull() & (col("path") != ""))
            .select(
                col("path").cast("string").alias("path"),
                col("source_file").cast("string").alias("source_file"),
                col("dataset").cast("string").alias("dataset"),
                col("batch_id").cast("string").alias("batch_id"),
            )
            .dropDuplicates(["path"])
            .withColumn("checkpoint_ts", current_timestamp().cast("string"))
            .withColumn("writer_status", lit("ESCRITO"))
        )

        if checkpoint_base.limit(1).count() > 0:
            checkpoint_base.write.mode("append").parquet(checkpoint_path)

    # =====================================
    # Output Contract Pattern
    # Retorna un dataframe de control de escritura
    # =====================================
    output_schema = StructType([
        StructField("nombre_tabla", StringType(), False),
        StructField("ruta_current", StringType(), False),
        StructField("ruta_history_delta", StringType(), False),
        StructField("enable_reprocess", StringType(), False),
        StructField("write_status", StringType(), False),
        StructField("write_message", StringType(), True),
    ])

    output_rows = [
        (
            str(nombre_tabla),
            current_path,
            history_path,
            str(enable_reprocess).lower(),
            "ESCRITO",
            None,
        )
    ]

    return spark.createDataFrame(output_rows, output_schema)
