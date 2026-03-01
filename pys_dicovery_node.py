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
    # Upload Result Contract Pattern
    # Detecta si el input proviene del nodo pys_upload (columnas de resultado)
    # =====================================
    input_cols = set(df.columns)
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
        bucket_raw = param_dict.get("bucket_raw") or param_dict.get("bucket")
        if is_missing(bucket_raw):
            raise ValueError("Falta parámetro requerido 'bucket_raw' (o 'bucket') para resolver paths desde s3_key")

        pending = (
            df
            .where((df.status == "PROCESADO") & (df.s3_key.isNotNull()) & (df.s3_key != ""))
            .selectExpr(
                f"concat('s3a://{bucket_raw}/', s3_key) as path",
                "'PENDING' as discovery_status",
                "cast(null as string) as error_message",
                f"concat('s3a://{bucket_raw}/', s3_key) as source_file"
            )
            .distinct()
        )

        if include_upload_errors:
            upload_errors_df = df.where((df.status != "PROCESADO") & df.status.rlike("^ERROR"))
            if include_upload_skipped:
                upload_errors_df = df.where((df.status != "PROCESADO") & (df.status.rlike("^ERROR") | df.status.rlike("^SKIPPED")))

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
        # Backward Compatibility Pattern
        # Conserva el comportamiento anterior cuando discovery se ejecuta standalone
        # =====================================
        bucket_raw = param_dict.get("bucket_raw")
        raw_prefix = param_dict.get("raw_prefix")

        if is_missing(bucket_raw):
            raise ValueError("Falta parámetro requerido 'bucket_raw'")
        if is_missing(raw_prefix):
            raise ValueError("Falta parámetro requerido 'raw_prefix'")

        raw_path = f"s3a://{bucket_raw}/{raw_prefix}"
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
    bucket_curated = param_dict.get("bucket_curated")
    checkpoint_prefix = param_dict.get("checkpoint_prefix")

    if is_missing(bucket_curated):
        raise ValueError("Falta parámetro requerido 'bucket_curated'")
    if is_missing(checkpoint_prefix):
        raise ValueError("Falta parámetro requerido 'checkpoint_prefix'")

    checkpoint_path = f"s3a://{bucket_curated}/{checkpoint_prefix}"

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
