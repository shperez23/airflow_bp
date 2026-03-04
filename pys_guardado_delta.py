from typing import Dict, List, Tuple


DEFAULT_RESULT = {
    "estado": "FALLIDO",
    "error": "SIN DATOS",
    "reproceso": "",
    "reprocesoLog": "",
    "vacuumLog": "",
    "escrituraLog": "",
    "cantidadInsertado": "0",
    "esquemaEntrada": "",
    "esquemaDestino": "",
    "casteoAplicado": "",
    "rangoIniProcesado": "",
    "rangoFinProcesado": "",
}


def _result_dataframe(spark, payload: Dict[str, str]):
    return spark.createDataFrame([payload]).selectExpr("*")


def _safe_row_from_df(dict_df):
    # Parameter Guard Pattern: protege el contrato de entrada para evitar fallos tempranos.
    params_df = dict_df.get("tri_parametros") if hasattr(dict_df, "get") else None
    if params_df is None or params_df.limit(1).count() == 0:
        return None
    return params_df.first()


def _replace_where(template: str, start_range: str, end_range: str) -> str:
    value = template or ""
    return value.replace("<RANGO_INI>", str(start_range)).replace("<RANGO_FIN>", str(end_range))


def _build_cast_projection(df_source, df_target) -> Tuple[List[str], Dict[str, str], Dict[str, str]]:
    # Schema Compatibility Enforcer: alinea tipos entre entrada y destino de forma explícita.
    target_dict = {field.name.lower(): field.dataType.simpleString() for field in df_target.schema}
    input_schema = {field.name: field.dataType.simpleString() for field in df_source.schema}
    target_schema = {field.name: field.dataType.simpleString() for field in df_target.schema}

    cast_expr = []
    for field in df_source.schema:
        src_name = field.name
        src_type = field.dataType.simpleString()
        target_type = target_dict.get(src_name.lower())

        if target_type and target_type != src_type:
            cast_expr.append(f"CAST({src_name} AS {target_type}) AS {src_name}")
        else:
            cast_expr.append(src_name)

    return cast_expr, input_schema, target_schema


def _write_delta(df, path: str, mode: str, partition_columns: List[str], optimize_columns: str, replace_where: str = None):
    writer = (
        df.write.format("delta")
        .mode(mode)
        .option("zorderBy", optimize_columns)
        .option("mergeSchema", "true")
    )

    if partition_columns:
        writer = writer.partitionBy(partition_columns)

    if replace_where:
        writer = writer.option("replaceWhere", replace_where)

    writer.save(path)


def pyspark_transform(spark, dict_df, param_dict):
    row_info = _safe_row_from_df(dict_df)
    if row_info is None:
        return _result_dataframe(spark, DEFAULT_RESULT)

    df_input = dict_df.get("tri_transformacion").persist()
    output_count = df_input.count()
    if output_count == 0:
        return _result_dataframe(spark, DEFAULT_RESULT)

    # Orchestrator Pattern: centraliza parámetros para controlar la estrategia de escritura.
    p_start_range = getattr(row_info, "rangoIni", "")
    p_end_range = getattr(row_info, "rangoFin", "")
    p_target_location = getattr(row_info, "tablaUbicacion", "")
    p_reprocess_condition = getattr(row_info, "condicionReproceso", "")
    p_partition_columns = getattr(row_info, "columnasParticion", "")
    p_optimize_columns = getattr(row_info, "columnasOptimizar", "")
    p_type_table = getattr(row_info, "tipoTabla", "history")
    p_shist = getattr(row_info, "requiereShist", 0)
    p_stock_condition = getattr(
        row_info,
        "condicionStock",
        "periodo = DATE_SUB(FROM_UTC_TIMESTAMP(current_timestamp(), 'America/Guayaquil'),1)",
    )
    p_reprocess_table = getattr(row_info, "tablaReproceso", "")

    p_target_suffix = "_shist" if p_type_table == "stock" and int(p_shist) == 1 else ""
    target_path = f"{p_target_location}{p_target_suffix}"
    replace_where = _replace_where(p_reprocess_condition, p_start_range, p_end_range)
    partition_columns = [col.strip() for col in str(p_partition_columns).split(",") if col.strip()]

    state = "FINALIZADO"
    error = ""
    reprocess_log = ""
    vacuum_log = ""
    write_log = ""
    cast_expr = ["*"]
    input_schema_dict = {}
    target_schema_dict = {}

    output_df = df_input
    table_exists = False
    reprocess_done = False

    if p_type_table == "history" or int(p_shist) == 1:
        reprocess_table_name = f"{p_reprocess_table}{p_target_suffix}"

        try:
            df_target = spark.table(reprocess_table_name)
            table_exists = True
            cast_expr, input_schema_dict, target_schema_dict = _build_cast_projection(df_input, df_target)
            output_df = df_input.selectExpr(*cast_expr).persist()
        except Exception as exc:
            error = str(exc)
            output_df = df_input

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        if table_exists and replace_where:
            try:
                reprocess_log = str(
                    spark.sql(f"DELETE FROM {reprocess_table_name} WHERE {replace_where}").collect()
                )
                reprocess_done = True
            except Exception as exc:
                reprocess_log = str(exc)

            try:
                vacuum_log = str(spark.sql(f"VACUUM {reprocess_table_name}").collect())
            except Exception as exc:
                vacuum_log = str(exc)

        try:
            if (not table_exists) or reprocess_done:
                _write_delta(output_df, target_path, "append", partition_columns, p_optimize_columns)
                write_log = "Modo Append"
            else:
                _write_delta(
                    output_df,
                    target_path,
                    "overwrite",
                    partition_columns,
                    p_optimize_columns,
                    replace_where=replace_where if replace_where else None,
                )
                write_log = "Modo Sobreescritura por partición"
        except Exception as exc:
            error = str(exc)
            try:
                _write_delta(output_df, target_path, "overwrite", partition_columns, p_optimize_columns)
                write_log = "Primera Escritura"
            except Exception as exc2:
                error = str(exc2)
                state = "FALLIDO"
                output_count = 0

    if p_type_table == "stock":
        df_stock = output_df.where(p_stock_condition)
        if df_stock.limit(1).count() > 0:
            _write_delta(df_stock, p_target_location, "overwrite", [], p_optimize_columns)
            write_log = f"{write_log}, Escritura con stock" if write_log else "Escritura con stock"

        if p_reprocess_table:
            try:
                spark.sql(f"VACUUM {p_reprocess_table}")
            except Exception as exc:
                vacuum_log = str(exc)

    start_processed = ""
    end_processed = ""
    try:
        row_summary = df_input.selectExpr(
            "MIN(periodo) as rangoIniProcesado",
            "MAX(periodo) as rangoFinProcesado",
        ).first()
        start_processed = str(row_summary.rangoIniProcesado)
        end_processed = str(row_summary.rangoFinProcesado)
    except Exception:
        start_processed = ""
        end_processed = ""

    result = {
        "estado": state,
        "error": str(error),
        "reproceso": replace_where,
        "reprocesoLog": reprocess_log,
        "vacuumLog": vacuum_log,
        "escrituraLog": write_log,
        "cantidadInsertado": str(output_count),
        "esquemaEntrada": str(input_schema_dict),
        "esquemaDestino": str(target_schema_dict),
        "casteoAplicado": str(cast_expr),
        "rangoIniProcesado": start_processed,
        "rangoFinProcesado": end_processed,
    }
    return _result_dataframe(spark, result)
