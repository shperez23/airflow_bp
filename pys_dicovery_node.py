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

    include_upload_errors = get_bool_param("include_upload_errors", True)
    include_upload_skipped = get_bool_param("include_upload_skipped", False)

    # =====================================
    # Multi-Input Resolver Pattern
    # Resuelve entradas cuando el orquestador envía múltiples dataframes
    # =====================================
    upload_df = df.get("pys_upload") if hasattr(df, "get") else df
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
    # Detecta si el input proviene del nodo pys_upload (columnas de resultado)
    # =====================================
    input_cols = set(upload_df.columns)
    has_upload_contract = {"full_path", "s3_key", "status"}.issubset(input_cols)

    # =====================================
    # Dataset Discovery Pattern
    # Construye el set de archivos candidatos según contrato detectado
    # =====================================
    if has_upload_contract:
        # =====================================
        # Storage URI Resolver Pattern
        # Convierte s3_key de upload a path s3a:// consumible por Spark
        # =====================================
        if is_missing(bucket_blob):
            raise ValueError("Falta parámetro requerido 'BUCKET_BLOB' en tri_parametros_discovery para resolver paths desde s3_key")

        pending = (
            upload_df
            .where((upload_df.status == "PROCESADO") & (upload_df.s3_key.isNotNull()) & (upload_df.s3_key != ""))
            .selectExpr(
                f"concat('s3a://{bucket_blob}/', s3_key) as path",
                "'PENDING' as discovery_status",
                "cast(null as string) as error_message",
                f"concat('s3a://{bucket_blob}/', s3_key) as source_file"
            )
            .distinct()
        )

        if include_upload_errors:
            upload_errors_df = upload_df.where((upload_df.status != "PROCESADO") & upload_df.status.rlike("^ERROR"))
            if include_upload_skipped:
                upload_errors_df = upload_df.where((upload_df.status != "PROCESADO") & (upload_df.status.rlike("^ERROR") | upload_df.status.rlike("^SKIPPED")))

            upload_errors = (
                upload_errors_df
                .selectExpr(
                    "cast(null as string) as path",
                    "status as discovery_status",
                    "coalesce(error_message, status) as error_message",
                    "full_path as source_file"
                )
                .distinct()
            )
        else:
            upload_errors = None

    else:
        # =====================================
        # Standalone Source Pattern
        # Permite discovery directo usando path relativo de upload
        # =====================================
        relative_upload_file_path = param_dict.get("relative_upload_file_path")

        if is_missing(bucket_blob):
            raise ValueError("Falta parámetro requerido 'BUCKET_BLOB' en tri_parametros_discovery")
        if is_missing(relative_upload_file_path):
            raise ValueError("Falta parámetro requerido 'relative_upload_file_path'")

        raw_path = f"s3a://{bucket_blob}/{relative_upload_file_path}"
        pending = (
            spark.read
            .format("binaryFile")
            .load(raw_path)
            .selectExpr("path", "'PENDING' as discovery_status", "cast(null as string) as error_message", "path as source_file")
            .distinct()
        )
        upload_errors = None

    # =====================================
    # Checkpointer Pattern
    # Excluye archivos ya procesados por pys_read_normalize (left_anti)
    # =====================================
    checkpoint_prefix = param_dict.get("checkpoint_prefix")

    if is_missing(bucket_raw):
        raise ValueError("Falta parámetro requerido 'BUCKET_RAW' en tri_parametros_discovery")
    if is_missing(checkpoint_prefix):
        raise ValueError("Falta parámetro requerido 'checkpoint_prefix'")

    checkpoint_path = f"s3a://{bucket_raw}/{checkpoint_prefix}"

    try:
        processed = spark.read.parquet(checkpoint_path).select("path").distinct()
        pending = pending.join(processed, "path", "left_anti")
    except Exception:
        pending = pending.distinct()

    if upload_errors is not None:
        pending = pending.unionByName(upload_errors, allowMissingColumns=True)

    # =====================================
    # Process Result Dataset Pattern
    # Retorna listado final de paths pendientes para pys_read_normalize
    # =====================================
    return pending
