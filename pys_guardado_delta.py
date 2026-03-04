# =================================================================================
#                        EJECUTAR INGESTA CON OPCION DE COLUMNA MD5
# ---------------------------------------------------------------------------------
#                              PROPOSITO
#   Plantilla que permite la ingesta de datos con el uso del motor.
# ---------------------------------------------------------------------------------
#                            MODIFICACIONES
#   FECHA           AUTOR       OBSERVACIÓN
#   01/Ago/2023     cmedina    Emision Inicial
#   31/Ago/2024     cmedina    Correciones code style y sonnar
#   30/Ene/2025     xxxxxxx    Aumentar columnas de logs
#   25/Feb/2025     ezapatap   Agregar columna ingesta_md5 a todas las tablas en ZR
#   22/07/2025      ezapatap   Codigo optimizado para:
#                               Evitar desbordamiento de memoria
#                               Suffle excesivo (total de líneas 502)
#   04/Mar/2026     codex      Refactor de mantenibilidad y performance
# =================================================================================

from datetime import datetime

import pytz
from pyspark.sql.functions import lit


def pyspark_transform(spark, df, param_dict):
    # habilitar sobreescritura por particion
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # =====================================
    # Parameter Guard Pattern
    # =====================================
    def get_bool_param(name, default=False):
        raw_value = param_dict.get(name, default)
        if isinstance(raw_value, bool):
            return raw_value

        normalized = str(raw_value).strip().lower()
        if normalized in {"1", "true", "yes", "y", "si", "sí"}:
            return True
        if normalized in {"0", "false", "no", "n"}:
            return False
        return default

    aplica_md5 = get_bool_param("calcula_md5", False)

    # =====================================
    # Utility Functions
    # =====================================
    def get_fecha_sql():
        return spark.sql(
            "SELECT from_utc_timestamp(CURRENT_TIMESTAMP(), 'America/Guayaquil') as fecha"
        ).collect()[0][0]

    def get_fecha_str(str_format="%Y-%m-%d %H:%M:%S.%f"):
        tz = pytz.timezone("America/Guayaquil")
        now = datetime.now(tz)
        return str(now.strftime(str_format))

    def append_log(log_list, log_str):
        log_list.append(f"{get_fecha_str()} {log_str}")

    def get_row_value(row, field_name, default=None):
        if hasattr(row, "asDict"):
            return row.asDict().get(field_name, default)
        if hasattr(row, "get"):
            return row.get(field_name, default)
        return getattr(row, field_name, default)

    def get_df_period(df_base):
        return (
            df_base.selectExpr("STRING(periodo) AS periodo")
            .limit(1)
            .collect()[0][0]
        )

    def default_ingest_record():
        fecha_actual = get_fecha_sql()
        return {
            "id_ingesta": "",
            "fecha_inicio": fecha_actual,
            "estado": "INICIADO",
            "fecha_fin": fecha_actual,
            "cantidad_insertados": 0,
            "error": "",
            "rango_ini_procesado": "",
            "rango_fin_procesado": "",
            "id_control": 0,
            "punto_control": "",
            "p_columna_delta_tipo_dato": "",
            "log_control": "",
            "log1": "",
            "error1": "",
            "checksum_source": 0,
            "checksum_destination": 0,
            "cantidad_leidos": 0,
            "is_stock_delta": 0,
            "log_detail": "",
            "coleccion_salida": "",
            "nombre_tabla": "",
            "root_metada_path": "",
            "tiene_shist": "0",
            "objeto_existe": 0,
        }

    def save_delta_partition(df_write, replace_where, target_location):
        try:
            (
                df_write.write.format("delta")
                .mode("overwrite")
                .partitionBy("periodo")
                .option("mergeSchema", "true")
                .option("replaceWhere", replace_where)
                .save(target_location)
            )
        except Exception:
            # fallback para periodos no alineados con replaceWhere entrante
            row_per = df_write.selectExpr(
                "date_format(min(periodo),'yyyy-MM-dd') as rango_ini",
                "date_format(max(periodo),'yyyy-MM-dd') as rango_fin",
            ).collect()[0]
            fallback_where = f"periodo between '{row_per.rango_ini}' and '{row_per.rango_fin}'"
            (
                df_write.write.format("delta")
                .mode("overwrite")
                .partitionBy("periodo")
                .option("mergeSchema", "true")
                .option("replaceWhere", fallback_where)
                .save(target_location)
            )

    def casteo_destino(df_source, target_location, tipo_formato, log_list):
        try:
            target_schema = (
                spark.read.format(f"{tipo_formato}").load(f"{target_location}").schema
            )
            target_dict = {
                col.name.lower(): col.dataType.simpleString() for col in target_schema
            }
            cast_expr = []
            for source_col in df_source.schema:
                source_name = source_col.name
                source_type = source_col.dataType.simpleString()
                target_type = target_dict.get(source_name.lower())
                if target_type is not None and target_type != source_type:
                    cast_expr.append(f"CAST({source_name} AS {target_type}) AS {source_name}")
                else:
                    cast_expr.append(source_name)

            append_log(log_list, "Aplicar Cast Destino")
            return df_source.selectExpr(*cast_expr)
        except Exception as err_cast:
            append_log(log_list, f"Aplicar Cast Destino, {err_cast}")
            return df_source

    def build_hash_expressions(df_base):
        coalesce_expr = "(" + "".join(
            [f",COALESCE(STRING({col.name}), '')" for col in df_base.schema]
        )
        hash_expr = coalesce_expr.replace("(,", "hash(CONCAT(") + ")) as ingesta_hash"
        hash_expr = hash_expr.replace("COALESCE(STRING(PARTICION_LECTURA), ''),", "")

        md5_expr = coalesce_expr.replace("(,", "md5(CONCAT(") + ")) as ingesta_md5"
        return hash_expr, md5_expr

    def checksum_from_hash(df_input, hash_expr=None):
        work_df = df_input
        if "ingesta_hash" not in work_df.columns:
            if hash_expr is None:
                return 0
            work_df = work_df.selectExpr(hash_expr)

        checksum_row = work_df.selectExpr("SUM(ingesta_hash) AS suma_hash").collect()[0]
        return checksum_row.suma_hash if checksum_row.suma_hash is not None else 0

    def get_objeto_existe(coleccion_salida, nombre_tabla, log_list):
        try:
            return spark.sql(
                f"SHOW TABLES FROM {coleccion_salida} like '{nombre_tabla}'"
            ).count()
        except Exception as error_tb:
            append_log(log_list, f"Verificar, colección y tabla delta, {error_tb}")
            return 0

    def process_stock_delta(df_new, target_location, new_periodo, old_periodo, log_list):
        df_new.createOrReplaceTempView("new")
        df_old = spark.read.format("parquet").load(target_location)
        df_old.createOrReplaceTempView("old")

        delta_count = 0
        previous_delta = ""
        try:
            previous_delta = (
                spark.read.parquet(f"{target_location}_sdelta")
                .selectExpr("STRING(MAX(periodo)) as periodo")
                .collect()[0][0]
                or ""
            )
        except Exception as error_delta:
            append_log(log_list, f"Verificar, Delta anterior, {error_delta}")

        append_log(log_list, f"Periodo Ultimo Delta: {previous_delta}")

        if previous_delta != "":
            df_delta = (
                spark.sql(
                    f"""SELECT
                        CASE
                            WHEN N.SDELTA_ID IS NOT NULL THEN STRUCT(N.*)
                            ELSE STRUCT(O.*)
                        END datos,
                        CASE
                            WHEN N.SDELTA_ID IS NOT NULL AND O.SDELTA_ID IS NOT NULL THEN 'U'
                            WHEN N.SDELTA_ID IS NOT NULL AND O.SDELTA_ID IS NULL THEN 'I'
                            WHEN N.SDELTA_ID IS NULL AND O.SDELTA_ID IS NOT NULL THEN 'D'
                            ELSE '-'
                        END as sdelta_operacion
                    FROM new N
                    FULL OUTER JOIN old O ON (N.SDELTA_ID = O.SDELTA_ID)
                    WHERE '{new_periodo}' >= '{old_periodo}'
                    AND coalesce(N.ingesta_md5, '-') != coalesce(O.ingesta_md5, '-')
                    """
                )
                .selectExpr(
                    "datos.*",
                    "sdelta_operacion",
                    f"'{old_periodo}' as sdelta_periodo_compararacion",
                )
                .withColumn("periodo", lit(new_periodo))
            )
            delta_count = df_delta.count()
            append_log(log_list, f"Conteo delta {delta_count}")

            if delta_count > 0:
                write_mode = "append" if previous_delta == new_periodo else "overwrite"
                df_delta.write.partitionBy("periodo").mode(write_mode).parquet(
                    f"{target_location}_sdelta"
                )
                append_log(log_list, f"Escritura delta modo {write_mode}")

        if delta_count == 0 and previous_delta == "":
            df_delta0 = df_new.selectExpr(
                "*",
                "'I' as sdelta_operacion",
                "'' as sdelta_periodo_compararacion",
            )
            df_delta0.write.partitionBy("periodo").mode("overwrite").parquet(
                f"{target_location}_sdelta"
            )
            append_log(log_list, f"Escritura primera vez, {target_location}_sdelta")

    def write_parquet_history(df_input, target_location, log_list):
        df_out = casteo_destino(df_input, target_location, "parquet", log_list)
        df_out.write.partitionBy("periodo").mode("overwrite").parquet(target_location)
        append_log(log_list, "Guardar PARQUET - HISTORY")
        return df_out

    def write_parquet_stock(df_input, target_location, row, is_stock_delta, log_list):
        df_out = casteo_destino(df_input, target_location, "parquet", log_list)

        if is_stock_delta == 1:
            new_periodo = get_df_period(df_out)
            append_log(log_list, f"Periodo Actual Delta: {new_periodo}")
            try:
                df_old = spark.read.format("parquet").load(target_location)
                old_count = df_old.count()
                old_periodo = get_df_period(df_old)
                append_log(log_list, f"Conteo Ultima Carga {old_count}")
                if old_count > 0:
                    process_stock_delta(
                        df_out,
                        target_location,
                        new_periodo,
                        old_periodo,
                        log_list,
                    )
                append_log(log_list, "Guardar PARQUET - STOCK_DELTA")
            except Exception as error_sd:
                append_log(log_list, str(error_sd))
                raise

        if row.p_requiere_shist == 1:
            df_out.write.partitionBy("periodo").mode("overwrite").parquet(
                f"{target_location}_shist"
            )
            append_log(log_list, "Guardar PARQUET - STOCK_SHIST")

        df_out.write.mode("overwrite").parquet(target_location)
        append_log(log_list, "Guardar PARQUET - STOCK")
        return df_out

    def write_delta_history(df_input, target_location, row, log_list):
        df_out = casteo_destino(df_input, target_location, "delta", log_list)
        save_delta_partition(df_out, row.p_condicion_reproceso, target_location)
        append_log(log_list, "Guardar DELTA - HISTORY")
        return df_out

    def write_delta_stock(df_input, target_location, row, log_list):
        df_out = casteo_destino(df_input, target_location, "delta", log_list)
        (
            df_out.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(target_location)
        )
        append_log(log_list, "Guardar DELTA - STOCK")

        if row.p_requiere_shist == 1:
            save_delta_partition(
                df_out,
                row.p_condicion_reproceso,
                f"{target_location}_shist",
            )
            append_log(log_list, "Guardar DELTA - STOCK_SHIST")
        return df_out

    def resolve_writer(tipo_escritura, tipo_tabla):
        writers = {
            ("parquet", "history"): write_parquet_history,
            ("parquet", "stock"): write_parquet_stock,
            ("delta", "history"): write_delta_history,
            ("delta", "stock"): write_delta_stock,
        }
        return writers.get((str(tipo_escritura), str(tipo_tabla)))

    ingest_list = [default_ingest_record()]

    # =====================================
    # Streaming Iterator Pattern
    # Evita uso de collect() para reducir presión de memoria en driver
    # =====================================
    for row in df.toLocalIterator():
        row_dict = row.asDict()
        log_control_list = []
        append_log(log_control_list, "Inicio")

        coleccion_salida = row_dict.get("coleccion_salida")
        nombre_tabla = row.p_nombre_tabla
        p_target_location = f"{row.p_ruta_salida}/{row.p_nombre_tabla}"
        objeto_existe = get_objeto_existe(coleccion_salida, nombre_tabla, log_control_list)

        ingest = {
            "id_ingesta": row.p_id_ingesta,
            "fecha_inicio": get_fecha_sql(),
            "estado": "INICIADO",
            "fecha_fin": "",
            "cantidad_insertados": 0,
            "error": "",
            "rango_ini_procesado": "",
            "rango_fin_procesado": "",
            "id_control": row.id_control,
            "punto_control": row.p_last_control,
            "p_columna_delta_tipo_dato": row.p_columna_delta_tipo_dato,
            "log_control": "",
            "log1": row.log1,
            "error1": row.error1,
            "checksum_source": 0,
            "checksum_destination": 0,
            "cantidad_leidos": 0,
            "is_stock_delta": 0,
            "log_detail": "",
            "coleccion_salida": coleccion_salida,
            "nombre_tabla": nombre_tabla,
            "root_metada_path": row_dict.get("root_metada_path"),
            "tiene_shist": str(row.p_requiere_shist),
            "objeto_existe": objeto_existe,
        }

        try:
            query = f"{row.p_query_consulta} {row.p_condicion_delta}"
            df0 = spark.sql(query).persist()

            if df0.take(1):
                append_log(log_control_list, "Leído")
                ingest["cantidad_leidos"] = df0.count()

                drop_list = [
                    "periodo",
                    "Periodo",
                    "PERIODO",
                    "Fecha_ingesta",
                    "Fecha_Ingesta",
                    "fecha_ingesta",
                    "FECHA_INGESTA",
                    "FECHAINGESTA",
                    "FechaIngesta",
                    "fechaIngesta",
                    "SDELTA_ID",
                    "ingesta_hash",
                    "ingesta_md5",
                ]
                df1 = df0.drop(*drop_list)
                append_log(log_control_list, "Eliminar columnas de control")

                hash_expr, md5_expr = build_hash_expressions(df1)
                p_expr_extras = [
                    expr
                    for expr in str(row.p_expresion_campos_extras).split(";")
                    if str(expr).strip() != ""
                ]
                p_expr_extras.append(hash_expr)
                if aplica_md5:
                    p_expr_extras.append(md5_expr)

                is_stock_delta = int(
                    row.tipo_tabla == "stock"
                    and "SDELTA_ID" in str(row.p_expresion_campos_extras)
                )
                ingest["is_stock_delta"] = is_stock_delta

                df2 = df1.selectExpr(
                    "*",
                    *p_expr_extras,
                    "FROM_UTC_TIMESTAMP(current_timestamp(), 'America/Guayaquil') AS fecha_ingesta",
                )
                append_log(log_control_list, "Aplicar campos extras")

                ingest["checksum_source"] = checksum_from_hash(df2)

                writer = resolve_writer(row.tipo_escritura, row.tipo_tabla)
                if writer is None:
                    raise ValueError(
                        "Combinación no soportada para escritura: "
                        f"tipo_escritura={row.tipo_escritura}, tipo_tabla={row.tipo_tabla}"
                    )

                if row.tipo_escritura == "parquet" and row.tipo_tabla == "history":
                    df_out = writer(df2, p_target_location, log_control_list)
                elif row.tipo_escritura == "parquet" and row.tipo_tabla != "history":
                    df_out = writer(df2, p_target_location, row, is_stock_delta, log_control_list)
                elif row.tipo_escritura == "delta" and row.tipo_tabla == "history":
                    df_out = writer(df2, p_target_location, row, log_control_list)
                else:
                    df_out = writer(df2, p_target_location, row, log_control_list)

                p_columna_delta = get_row_value(row, "p_columna_delta")
                row_sum = df2.selectExpr(
                    "count(1) as cantidad_insertados",
                    f"string(min({p_columna_delta})) as rango_ini_procesado",
                    f"string(max({p_columna_delta})) as rango_fin_procesado",
                ).collect()[0]
                append_log(log_control_list, "Obtener Resumen")

                ingest["cantidad_insertados"] = row_sum.cantidad_insertados
                ingest["rango_ini_procesado"] = row_sum.rango_ini_procesado
                ingest["rango_fin_procesado"] = row_sum.rango_fin_procesado
                ingest["fecha_fin"] = get_fecha_sql()
                ingest["estado"] = "FINALIZADO"
                ingest["log_detail"] = f"{log_control_list} Ubicacion: {p_target_location}"

                if row.tipo_tabla != "history":
                    if row.tipo_escritura == "parquet":
                        df_checksum = spark.read.parquet(p_target_location)
                    else:
                        df_checksum = spark.read.format("delta").load(p_target_location)
                else:
                    if row.tipo_escritura == "parquet":
                        df_checksum = spark.read.parquet(p_target_location).filter(
                            row.p_condicion_reproceso
                        )
                    else:
                        df_checksum = spark.read.format("delta").load(
                            p_target_location
                        ).filter(row.p_condicion_reproceso)

                ingest["checksum_destination"] = checksum_from_hash(df_checksum, hash_expr)
            else:
                ingest.update(
                    {
                        "fecha_fin": get_fecha_sql(),
                        "estado": "FALLIDO",
                        "log1": p_target_location,
                        "error1": "SIN DATOS",
                        "log_detail": str(log_control_list),
                    }
                )
        except Exception as err:
            ingest["fecha_fin"] = get_fecha_sql()
            ingest["error"] = str(err)
            ingest["estado"] = "FALLIDO"
            ingest["log_detail"] = str(log_control_list)

        ingest["log_control"] = log_control_list[-1] if log_control_list else ""
        ingest_list.append(ingest)

    output_df = spark.createDataFrame(ingest_list).where("id_ingesta!=''").selectExpr("*")
    return output_df
