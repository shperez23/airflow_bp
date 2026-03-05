from pyspark.sql.functions import col
import re


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
    append_log("Inicio pys_descubrimiento_archivos")

    # =====================================
    # Upstream Status Adapter Pattern
    # Interpreta contrato estandarizado de upload para validar continuidad
    # =====================================
    def parse_processed_count(log_detail):
        if is_missing(log_detail):
            return 0

        matches = re.findall(r"procesados=(\d+)", str(log_detail))
        if not matches:
            return 0

        try:
            return int(matches[-1])
        except Exception:
            return 0

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
    # Detecta si el input proviene del nodo pys_subida_archivos (columnas de resultado)
    # =====================================
    input_cols = set(upload_df.columns)
    has_upload_contract = {"full_path", "s3_key", "status"}.issubset(input_cols)
    has_standardized_contract = {"estado", "error", "log_control", "log_detail"}.issubset(input_cols)
    upload_processed_count = 0

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
            return build_status_df("FALLIDO", "Falta parámetro requerido 'BUCKET_BLOB' en tri_parametros_discovery para resolver paths desde s3_key")

        pending = (
            upload_df
            .where((upload_df.status == "PROCESADO") & (upload_df.s3_key.isNotNull()) & (upload_df.s3_key != ""))
            .selectExpr(
                f"concat('s3a://{bucket_blob}/', s3_key) as path",
                "'PENDIENTE' as discovery_status",
                "cast(null as string) as error_message",
                f"concat('s3a://{bucket_blob}/', s3_key) as source_file"
            )
            .distinct()
        )

        append_log("Contrato de upload detectado")

        if include_upload_errors:
            upload_errors_df = upload_df.where((upload_df.status != "PROCESADO") & upload_df.status.rlike("^ERROR"))
            if include_upload_skipped:
                upload_errors_df = upload_df.where((upload_df.status != "PROCESADO") & (upload_df.status.rlike("^ERROR") | upload_df.status.rlike("^OMITIDO")))

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

    elif has_standardized_contract:
        relative_upload_file_path = param_dict.get("relative_upload_file_path")

        upload_status_row = upload_df.selectExpr("estado", "error", "log_detail").first()
        if upload_status_row is not None:
            upload_processed_count = parse_processed_count(upload_status_row.log_detail)
            append_log(f"Contrato estandarizado de upload detectado: estado={upload_status_row.estado}, procesados={upload_processed_count}")

            if str(upload_status_row.estado).upper() == "FALLIDO":
                return build_status_df("FALLIDO", f"Upload falló: {upload_status_row.error}")

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
            .selectExpr(
                "path",
                "'PENDIENTE' as discovery_status",
                "cast(null as string) as error_message",
                "path as source_file"
            )
            .distinct()
        )
        append_log("Descubrimiento ejecutado desde contrato estandarizado en prefijo /original/")
        upload_errors = None

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
        upload_errors = None

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

    if upload_errors is not None:
        pending = pending.unionByName(upload_errors, allowMissingColumns=True)

    # =====================================
    # Process Result Dataset Pattern
    # Retorna listado final de paths pendientes para pys_read_normalize
    # =====================================
    total_pending = pending.where(col("path").isNotNull() & (col("path") != "")).count()
    append_log(f"Archivos pendientes detectados: {total_pending}")

    total_errors = 0
    if upload_errors is not None:
        total_errors = upload_errors.count()
        if total_errors > 0:
            append_log(f"Errores heredados de upload: {total_errors}")

    if total_errors > 0:
        return build_status_df("FALLIDO", f"Se detectaron {total_errors} errores heredados desde la carga")

    if upload_processed_count > 0 and total_pending == 0:
        return build_status_df(
            "FALLIDO",
            (
                "No se descubrieron archivos pendientes, pero upload reportó "
                f"procesados={upload_processed_count}. Verifique 'relative_upload_file_path' "
                "y que discovery lea el prefijo /original/."
            ),
        )

    append_log("Descubrimiento finalizado")
    return pending
