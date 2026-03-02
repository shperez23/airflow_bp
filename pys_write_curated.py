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

    input_df = resolve_input_frame(df, ["pys_read_normalize", "pys_read_normalize_node"])
    param_df = resolve_input_frame(df, ["tri_parametros_write", "tri_parametros_read", "Tri_parametros_write"])

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
        or param_dict.get("RUTA_SALIDA")
        or param_dict.get("ruta_salida")
    )
    nombre_tabla = (
        get_value_from_param_df(param_df, "NOMBRE_TABLA")
        or param_dict.get("NOMBRE_TABLA")
        or param_dict.get("nombre_tabla")
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
    base_path = f"{str(ruta_salida).rstrip('/')}/{str(nombre_tabla).strip('/')}"
    current_path = f"{base_path}/current"
    history_path = f"{base_path}/history_delta"

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
    execution_id = param_dict.get("execution_id") or param_dict.get("run_id")
    history_df = df_with_audit.withColumn("execution_id", lit(execution_id).cast("string"))

    history_exists = False
    try:
        spark.read.format("delta").load(history_path).limit(1).count()
        history_exists = True
    except Exception:
        history_exists = False

    if enable_reprocess and history_exists and execution_id is not None:
        try:
            from delta.tables import DeltaTable

            delta_table = DeltaTable.forPath(spark, history_path)
            delta_table.delete(col("execution_id") == lit(str(execution_id)))
        except Exception:
            history_df = history_df.filter(lit(True))

    history_df.write.format("delta").mode("append").save(history_path)

    # =====================================
    # Output Contract Pattern
    # Retorna un dataframe de control de escritura
    # =====================================
    output_schema = StructType([
        StructField("nombre_tabla", StringType(), False),
        StructField("ruta_current", StringType(), False),
        StructField("ruta_history_delta", StringType(), False),
        StructField("execution_id", StringType(), True),
        StructField("enable_reprocess", StringType(), False),
    ])

    output_rows = [
        (
            str(nombre_tabla),
            current_path,
            history_path,
            None if execution_id is None else str(execution_id),
            str(enable_reprocess).lower(),
        )
    ]

    return spark.createDataFrame(output_rows, output_schema)
