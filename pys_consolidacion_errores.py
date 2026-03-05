def pyspark_transform(spark, df, param_dict):
    # =====================================
    # Unified Error Observer Pattern
    # Consolida estados estandarizados del flujo en un único registro de salida
    # =====================================
    process_log = []

    def append_log(message):
        process_log.append(str(message))

    def build_status_df(estado, error=""):
        return spark.createDataFrame(
            [(
                estado,
                str(error or ""),
                process_log[-1] if process_log else "",
                str(process_log),
            )],
            ["estado", "error", "log_control", "log_detail"],
        )

    def get_stage_df(flow_inputs, stage_name):
        if hasattr(flow_inputs, "get"):
            return flow_inputs.get(stage_name)
        return None

    append_log("Inicio consolidación de errores")

    upload_df = get_stage_df(df, "pys_subida_archivos")
    discovery_df = get_stage_df(df, "pys_descubrimiento_archivos")
    read_df = get_stage_df(df, "pys_lectura_normalizacion")

    if not hasattr(df, "get"):
        cols = set(df.columns)
        if {"estado", "error", "log_control", "log_detail"}.issubset(cols):
            upload_df = df

    standardized_errors = []

    def collect_error_from_stage(stage_name, stage_df):
        if stage_df is None or not hasattr(stage_df, "columns"):
            return

        cols = set(stage_df.columns)
        if not {"estado", "error", "log_control", "log_detail"}.issubset(cols):
            append_log(f"{stage_name}: sin contrato estandarizado")
            return

        row = stage_df.select("estado", "error", "log_control", "log_detail").first()
        if row is None:
            return

        append_log(f"{stage_name}: estado={row.estado}")
        if str(row.estado).upper() == "FALLIDO":
            msg = f"{stage_name}: {row.error or 'Error no especificado'}"
            standardized_errors.append(msg)

    collect_error_from_stage("pys_subida_archivos", upload_df)
    collect_error_from_stage("pys_descubrimiento_archivos", discovery_df)
    collect_error_from_stage("pys_lectura_normalizacion", read_df)

    if standardized_errors:
        append_log(f"Errores consolidados: {len(standardized_errors)}")
        return build_status_df("FALLIDO", " | ".join(standardized_errors))

    append_log("Consolidación finalizada sin errores")
    return build_status_df("FINALIZADO", "")
