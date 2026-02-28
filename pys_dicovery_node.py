def pyspark_transform(spark, df, param_dict):

    # =====================================
    # Parameter Guard Pattern
    # Normaliza parámetros opcionales y evita errores por null/vacío
    # =====================================
    def is_missing(value):
        return value is None or (isinstance(value, str) and value.strip() == "")

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
        # Toma únicamente archivos procesados exitosamente y con key válida
        discovered_df = (
            df
            .where((df.status == "PROCESADO") & (df.s3_key.isNotNull()) & (df.s3_key != ""))
            .select("s3_key")
            .distinct()
        )

        # =====================================
        # Storage URI Resolver Pattern
        # Convierte s3_key de upload a path s3a:// consumible por Spark
        # =====================================
        bucket_raw = param_dict.get("bucket_raw") or param_dict.get("bucket")
        if is_missing(bucket_raw):
            raise ValueError("Falta parámetro requerido 'bucket_raw' (o 'bucket') para resolver paths desde s3_key")

        pending = discovered_df.selectExpr(
            f"concat('s3a://{bucket_raw}/', s3_key) as path"
        )

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
        pending = spark.read.format("binaryFile").load(raw_path).select("path")

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
        pending = pending.distinct().join(processed, "path", "left_anti")
    except Exception:
        pending = pending.distinct()

    # =====================================
    # Process Result Dataset Pattern
    # Retorna listado final de paths pendientes para pys_read_normalize
    # =====================================
    return pending
