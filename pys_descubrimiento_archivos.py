from pyspark.sql.functions import col


def pyspark_transform(spark, df, param_dict):
    # =====================================
    # Unified Status Contract Pattern
    # Estandariza la salida a un único registro de control del proceso
    # =====================================
    process_log = []

    def append_log(message):
        process_log.append(message)

    def build_status_df(estado, error=""):
        detail = str(process_log)
        log_control = process_log[-1] if process_log else ""
        return spark.createDataFrame(
            [(estado, error, log_control, detail)],
            ["estado", "error", "log_control", "log_detail"],
        )

    # =====================================
    # Parameter Guard Pattern
    # Normaliza parámetros opcionales y evita errores por null/vacío
    # =====================================
    def is_missing(value):
        return value is None or (isinstance(value, str) and value.strip() == "")

    append_log("Inicio pys_descubrimiento_archivos")

    # =====================================
    # Multi-Input Resolver Pattern
    # Resuelve entradas cuando el orquestador envía múltiples dataframes
    # =====================================
    upload_df = df.get("pys_subida_archivos") if hasattr(df, "get") else df
    param_discovery_row = df.get("tri_parametros_discovery") if hasattr(df, "get") else None

    def get_param_discovery_value(param_source, field_name):
        if param_source is None:
            return None

        if hasattr(param_source, "first"):
            param_source = param_source.first()

        try:
            return param_source[field_name]
        except Exception:
            return None

    bucket_blob = get_param_discovery_value(param_discovery_row, "BUCKET_BLOB")
    bucket_raw = get_param_discovery_value(param_discovery_row, "BUCKET_RAW")

    # =====================================
    # Upload Result Contract Pattern
    # Detecta si el input proviene del nodo pys_subida_archivos (éxito:path / error:estado)
    # =====================================
    input_cols = set(upload_df.columns)
    has_upload_path_contract = {"path"}.issubset(input_cols)
    has_standardized_contract = {"estado", "error", "log_control", "log_detail"}.issubset(input_cols)

    # =====================================
    # Dataset Discovery Pattern
    # Construye el set de archivos candidatos según contrato detectado
    # =====================================
    if has_standardized_contract:
        upload_status_row = upload_df.selectExpr("estado", "error", "log_control", "log_detail").first()
        if upload_status_row is not None and str(upload_status_row.estado).upper() == "FALLIDO":
            append_log("Contrato estandarizado FALLIDO recibido desde pys_subida_archivos")
            return upload_df.select("estado", "error", "log_control", "log_detail")

        return build_status_df("FALLIDO", "Contrato estandarizado recibido sin paths para descubrimiento")

    elif has_upload_path_contract:
        pending = (
            upload_df
            .where(col("path").isNotNull() & (col("path") != ""))
            .selectExpr(
                "path",
                "'PENDIENTE' as discovery_status",
                "cast(null as string) as error_message",
                "path as source_file"
            )
            .distinct()
        )
        append_log("Contrato de éxito de upload detectado (lista de paths)")
    else:
        # =====================================
        # Standalone Source Pattern
        # Permite discovery directo usando path relativo de upload
        # =====================================
        relative_upload_file_path = param_dict.get("relative_upload_file_path")

        if is_missing(bucket_blob):
            return build_status_df("FALLIDO", "Falta parámetro requerido 'BUCKET_BLOB' en tri_parametros_discovery")
        if is_missing(relative_upload_file_path):
            return build_status_df("FALLIDO", "Falta parámetro requerido 'relative_upload_file_path'")

        # =====================================
        # Raw Prefix Alignment Pattern
        # Descubre desde /original/ para alinearse con la escritura de upload
        # =====================================
        raw_path = f"s3a://{bucket_blob}/{relative_upload_file_path}/original/"
        pending = (
            spark.read
            .format("binaryFile")
            .option("recursiveFileLookup", "true")
            .load(raw_path)
            .selectExpr("path", "'PENDIENTE' as discovery_status", "cast(null as string) as error_message", "path as source_file")
            .distinct()
        )
        append_log("Descubrimiento ejecutado en modo standalone")
    # =====================================
    # Checkpointer Pattern
    # Excluye archivos ya procesados por pys_read_normalize (left_anti)
    # =====================================
    checkpoint_prefix = param_dict.get("checkpoint_prefix")

    if is_missing(bucket_raw):
        return build_status_df("FALLIDO", "Falta parámetro requerido 'BUCKET_RAW' en tri_parametros_discovery")
    if is_missing(checkpoint_prefix):
        return build_status_df("FALLIDO", "Falta parámetro requerido 'checkpoint_prefix'")

    checkpoint_path = f"s3a://{bucket_raw}/{checkpoint_prefix}"

    try:
        processed_raw = spark.read.parquet(checkpoint_path)
        processed_cols = set(processed_raw.columns)

        # =====================================
        # Checkpoint Contract Adapter Pattern
        # Acepta checkpoint antiguo (solo path) y nuevo (path + source_file)
        # =====================================
        processed_candidates = []

        if "path" in processed_cols:
            processed_candidates.append(processed_raw.select(col("path").cast("string").alias("path")))

        if "source_file" in processed_cols:
            processed_candidates.append(processed_raw.select(col("source_file").cast("string").alias("path")))

        if not processed_candidates:
            processed = spark.createDataFrame([], "path string")
        else:
            processed = processed_candidates[0]
            for candidate in processed_candidates[1:]:
                processed = processed.unionByName(candidate, allowMissingColumns=True)

            processed = processed.where(col("path").isNotNull() & (col("path") != "")).distinct()

        pending = pending.join(processed, "path", "left_anti")
        append_log("Checkpoint aplicado para excluir archivos procesados")
    except Exception:
        pending = pending.distinct()
        append_log("Checkpoint no disponible; se continúa sin exclusión incremental")

    # =====================================
    # Process Result Dataset Pattern
    # Retorna listado final de paths pendientes para pys_read_normalize
    # =====================================
    total_pending = pending.where(col("path").isNotNull() & (col("path") != "")).count()
    append_log(f"Archivos pendientes detectados: {total_pending}")

    append_log("Descubrimiento finalizado")
    return pending
