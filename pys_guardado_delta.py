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
# =================================================================================

from datetime import datetime
from pyspark.sql.functions import lit
import pytz


def pyspark_transform(spark, df, param_dict):
    # habilitar sobreescritura por particion
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    ingest = {}
    log_control_list = []

    def get_fecha_sql():
        return spark.sql(
            "SELECT from_utc_timestamp(CURRENT_TIMESTAMP(), 'America/Guayaquil') as fecha"
        ).collect()[0][0]

    def get_fecha():
        # now = datetime.now().astimezone(timezone("America/Guayaquil"))
        tz = pytz.timezone("America/Guayaquil")
        now = datetime.now(tz)
        return now

    def get_fecha_str(str_format="%Y-%m-%d %H:%M:%S.%f"):
        return str(get_fecha().strftime(str_format))

    def append_log(log_str):
        ingest["log_control"] = log_str
        log_control_list.append(get_fecha_str() + " " + log_str)

    def save_delta_partition(
        df_write, p_replace_where, p_target_location, partition="periodo"
    ):
        try:
            df_write.write.format("delta").mode("overwrite").partitionBy(
                "periodo"
            ).option("mergeSchema", "true").option(
                "replaceWhere", p_replace_where
            ).save(
                p_target_location
            )
        except Exception as error_write:
            append_log("Guardar delta por partición, " + str(error_write))
            # obtener rango de periodos desde la fuente
            row_per = df_write.selectExpr(
                "date_format(min(periodo),'yyyy-MM-dd') as rango_ini",
                "date_format(max(periodo),'yyyy-MM-dd') as rango_fin",
            ).collect()[0]
            p_star = row_per.rango_ini
            p_end = row_per.rango_fin
            p_replace_where = f"periodo between '{p_star}' and '{p_end}'"
            # reintentar
            df_write.write.format("delta").mode("overwrite").partitionBy(
                "periodo"
            ).option("mergeSchema", "true").option(
                "replaceWhere", p_replace_where
            ).save(
                p_target_location
            )

    def get_checksum(df, source):
        if source == 0:
            df = df.selectExpr(hash_expr).persist()
        # obtiene la suma de hash de todos los registros
        df_result = df.selectExpr("SUM(ingesta_hash) AS suma_hash")
        hash_value = df_result.collect()[0][0]
        return hash_value

    def casteo_destino(df, p_target_location, tipo_formato):
        try:
            cast_expr = []
            target_schema = (
                spark.read.format(f"{tipo_formato}").load(f"{p_target_location}").schema
            )

            # captura esquema entrasa
            input_schema = df.schema
            # convertir a diccionario
            target_dict = {}
            for col in target_schema:
                col_name = (col.name).lower()
                target_dict[col_name] = col.dataType.simpleString()
            # Crear expresion de cast
            for col in input_schema:
                source_name = col.name
                source_type = col.dataType.simpleString()
                target_type = target_dict.get(source_name.lower())
                if target_type is not None and target_type != source_type:
                    cast_expr.append(
                        f"CAST({source_name} AS {target_type}) AS {source_name}"
                    )
                else:
                    cast_expr.append(source_name)
            # aplicar casteo
            df = df.selectExpr(*cast_expr)
            append_log("Aplicar Cast Destino")

        except Exception as err_cast:
            append_log("Aplicar Cast Destino, " + str(err_cast))
            # cast_expr = ["*"]
        return df

    ingest_rows = df.collect()
    ingest_list = []
    # registro base, en caso de no tener datos
    ingest_model = {
        "id_ingesta": "",
        "fecha_inicio": get_fecha_sql(),
        "estado": "INICIADO",
        "fecha_fin": get_fecha_sql(),
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
    ingest_list.append(ingest_model)
    # recorrer grupo de ingesta
    for row in ingest_rows:
        row_dict1 = row.asDict()
        append_log("Inicio")
        coleccion_salida = row_dict1.get("coleccion_salida")
        nombre_tabla = row.p_nombre_tabla
        # verificar  objeto existe
        try:
            objeto_existe = spark.sql(
                f"SHOW TABLES FROM  {coleccion_salida}   like  '{nombre_tabla}'"
            ).count()
        except Exception as error_tb:
            append_log("Verificar, colección y tabla delta, " + str(error_tb))
            objeto_existe = 0

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
            "root_metada_path": row_dict1.get("root_metada_path"),
            "tiene_shist": str(row.p_requiere_shist),
            "objeto_existe": objeto_existe,
        }

        # leer tabla
        try:
            query = row.p_query_consulta + " " + row.p_condicion_delta
            # persistir consulta base
            df0 = spark.sql(query).persist()

            if df.take(1):
                append_log("Leído")
                p_replace_where = row.p_condicion_reproceso

                # contar registros origen
                row_sum_o = df0.selectExpr("count(1) as cantidad_leidos").collect()[0]
                ingest["cantidad_leidos"] = row_sum_o.cantidad_leidos
                # quitar columnas de control
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
                append_log("Eliminar columnas de control")

                # expresion calcular hash
                target_schema = df1.schema
                coalesce_expr = "("
                for col in target_schema:
                    col_name = col.name
                    coalesce_expr = coalesce_expr + f",COALESCE(STRING({col_name}),'')"

                hash_expr = (
                    coalesce_expr.replace("(,", "hash(CONCAT(") + ")) as ingesta_hash"
                )
                hash_expr = hash_expr.replace(
                    "COALESCE(STRING(PARTICION_LECTURA),''),", ""
                )
                # aplicar campos extras
                p_expresion_campos_extras = row.p_expresion_campos_extras
                p_expr_extras = (p_expresion_campos_extras).split(";")
                # anadir hash
                p_expr_extras.append(hash_expr)

                # calcular identificador de fila md5
                aplica_md5 = str(param_dict.get("calcula_md5"))
                if aplica_md5 == "1":
                    md5_expr = (
                        coalesce_expr.replace("(,", "md5(CONCAT(") + ")) as ingesta_md5"
                    )
                    p_expr_extras.append(md5_expr)

                # identificar carga stock delta
                is_stock_delta = 0
                if (
                    row.tipo_tabla == "stock"
                    and "SDELTA_ID" in p_expresion_campos_extras
                ):
                    is_stock_delta = 1
                    ingest["is_stock_delta"] = is_stock_delta

                df2 = df1.selectExpr(
                    "*",
                    *p_expr_extras,
                    "FROM_UTC_TIMESTAMP(current_timestamp(), 'America/Guayaquil') AS fecha_ingesta",
                )
                append_log("Aplicar campos extras")
                # calcular checksum origen
                ingest["checksum_source"] = get_checksum(df2, 1)
                # ubicacion tabla
                p_target_location = row.p_ruta_salida + "/" + row.p_nombre_tabla
                if row.tipo_escritura == "parquet":
                    # PARQUET - HISTORY
                    if row.tipo_tabla == "history":
                        # Escribir en parquet
                        df2 = casteo_destino(df2, p_target_location, "parquet")
                        df2.write.partitionBy("periodo").mode("overwrite").parquet(
                            p_target_location
                        )
                        append_log("Guardar PARQUET - HISTORY")
                    # PARQUET - STOCK
                    else:
                        # verificar stock delta
                        if is_stock_delta == 1:
                            # verificar si existe delta
                            previous_delta = ""
                            try:
                                df_aux = spark.read.parquet(
                                    f"{p_target_location}_sdelta"
                                ).selectExpr("STRING(MAX(periodo)) as periodo")
                                previous_delta = df_aux.collect()[0][0]

                            except Exception as error_delta:
                                append_log(
                                    "Verificar, Delta anterior, " + str(error_delta)
                                )

                            append_log("Periodo Ultimo Delta: {previous_delta}")
                            df2 = casteo_destino(df2, p_target_location, "parquet")
                            new_periodo = (
                                df2.selectExpr("STRING(periodo) AS periodo")
                                .limit(1)
                                .collect()[0][0]
                            )
                            append_log("Periodo Actual Delta: {new_periodo}")

                            # comparar ultima carga con nueva carga
                            try:
                                df_old = spark.read.format("parquet").load(
                                    p_target_location
                                )
                                old_count = df_old.count()
                                old_periodo = (
                                    df_old.selectExpr("STRING(periodo) AS periodo")
                                    .limit(1)
                                    .collect()[0][0]
                                )
                                append_log("Conteo Ultima Carga {old_count}")
                                if old_count > 0:
                                    # crear delta
                                    df2.createOrReplaceTempView("new")
                                    df_old.createOrReplaceTempView("old")

                                    # captura insert/ update / delete
                                    # el periodo se toma siempre del new
                                    # cuando son eliminaciones se toman los datos del OLD; cuando son nuevos o updates se toman del NEW
                                    delta_count = 0
                                    # validar si existe antes delta, sino es primera carga
                                    if previous_delta != "":
                                        df_delta = (
                                            spark.sql(
                                                f"""SELECT
                                            CASE
                                                WHEN N.SDELTA_ID IS NOT NULL THEN STRUCT(N.*)
                                                ELSE STRUCT(O.*)
                                            END datos
                                            ,CASE
                                                WHEN N.SDELTA_ID IS NOT NULL AND O.SDELTA_ID IS NOT NULL THEN 'U'  
                                                WHEN N.SDELTA_ID IS NOT NULL AND O.SDELTA_ID IS     NULL THEN 'I'
                                                WHEN N.SDELTA_ID IS     NULL AND O.SDELTA_ID IS NOT NULL THEN 'D'                                    
                                                ELSE '-'
                                            END as sdelta_operacion
                                            FROM new N
                                            FULL OUTER JOIN old O ON (N.SDELTA_ID=O.SDELTA_ID)
                                            WHERE
                                            '{new_periodo}' >= '{old_periodo}'
                                            AND coalesce(N.ingesta_md5,'-') != coalesce(O.ingesta_md5,'-')
                                            """
                                            )
                                            .selectExpr(
                                                "datos.*",
                                                "sdelta_operacion",
                                                f"'{old_periodo}' as sdelta_periodo_compararacion",
                                            )
                                            .withColumn("periodo", lit(new_periodo))
                                        )
                                        # guardar history snap-delta
                                        delta_count = df_delta.count()
                                        append_log("Conteo delta {delta_count}")

                                    if delta_count > 0:
                                        # si existe delta el mimo dia, se realiza append
                                        write_mode = "overwrite"
                                        if previous_delta == new_periodo:
                                            write_mode = "append"

                                        # escritura  delta
                                        df_delta = casteo_destino(
                                            df2, p_target_location, "parquet"
                                        )
                                        df_delta.write.partitionBy("periodo").mode(
                                            write_mode
                                        ).parquet(f"{p_target_location}_sdelta")
                                        append_log("Escritura delta modo {write_mode}")
                                    else:
                                        # verificar si existe tabla delta
                                        if previous_delta == "":
                                            # carga inicial
                                            df_delta0 = df2.selectExpr(
                                                "*",
                                                "'I' as sdelta_operacion",
                                                "'' as sdelta_periodo_compararacion",
                                            )
                                            df_delta0.write.partitionBy("periodo").mode(
                                                "overwrite"
                                            ).parquet(f"{p_target_location}_sdelta")
                                            append_log(
                                                f"Escritura primera vez, {p_target_location}_sdelta"
                                            )

                                append_log("Guardar PARQUET - STOCK_DELTA")
                            except Exception as error_sd:
                                append_log(str(error_sd))
                                ingest["error"] = str(error_sd)

                        # verificar stock snap
                        if row.p_requiere_shist == 1:
                            # guardar history snap
                            df2 = casteo_destino(df2, p_target_location, "parquet")
                            df2.write.partitionBy("periodo").mode("overwrite").parquet(
                                f"{p_target_location}_shist"
                            )
                            append_log("Guardar PARQUET - STOCK_SHIST")

                        # guardar stock
                        df2 = casteo_destino(df2, p_target_location, "parquet")
                        df2.write.mode("overwrite").parquet(p_target_location)
                        append_log("Guardar PARQUET - STOCK")
                else:
                    # DELTA - HISTORY
                    if row.tipo_tabla == "history":
                        # Escribir en delta
                        df2 = casteo_destino(df2, p_target_location, "delta")
                        save_delta_partition(df2, p_replace_where, p_target_location)
                        append_log("Guardar DELTA - HISTORY")

                    # DELTA - STOCK
                    else:
                        df2 = casteo_destino(df2, p_target_location, "delta")
                        df2.write.format("delta").mode("overwrite").option(
                            "mergeSchema", "true"
                        ).save(p_target_location)
                        append_log("Guardar DELTA - STOCK")
                        # verificar stock snap
                        if row.p_requiere_shist == 1:
                            # guardar history snap delta
                            df2 = casteo_destino(df2, p_target_location, "delta")
                            save_delta_partition(
                                df2, p_replace_where, p_target_location + "_shist"
                            )
                            append_log("Guardar DELTA - STOCK_SHIST")
                # resumen
                p_columna_delta = row.p_columna_delta
                row_sum = df2.selectExpr(
                    "count(1) as cantidad_insertados",
                    f"string(min({p_columna_delta})) as rango_ini_procesado",
                    f"string(max({p_columna_delta})) as rango_fin_procesado",
                ).collect()[0]
                append_log("Obtener Resumen")
                ingest["cantidad_insertados"] = row_sum.cantidad_insertados
                ingest["rango_ini_procesado"] = row_sum.rango_ini_procesado
                ingest["rango_fin_procesado"] = row_sum.rango_fin_procesado
                ingest["fecha_fin"] = get_fecha_sql()
                ingest["estado"] = "FINALIZADO"
                ingest["log_detail"] = (
                    str(log_control_list) + "Ubicacion: " + p_target_location
                )
                # Checksum destino
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
                        df_checksum = (
                            spark.read.format("delta")
                            .load(p_target_location)
                            .filter(row.p_condicion_reproceso)
                        )
                ingest["checksum_destination"] = get_checksum(df_checksum, 0)
            else:
                ingest_list = [
                    {
                        "id_ingesta": row.p_id_ingesta,
                        "fecha_inicio": get_fecha_sql(),
                        "estado": "FALLIDO",
                        "fecha_fin": get_fecha_sql(),
                        "cantidad_insertados": 0,
                        "error": "",
                        "rango_ini_procesado": "",
                        "rango_fin_procesado": "",
                        "id_control": "",
                        "punto_control": "",
                        "p_columna_delta_tipo_dato": "",
                        "log_control": "",
                        "log1": p_target_location,
                        "error1": "SIN DATOS",
                        "checksum_source": 0,
                        "checksum_destination": 0,
                        "cantidad_leidos": 0,
                        "is_stock_delta": 0,
                        "log_detail": "",
                        "coleccion_salida": coleccion_salida,
                        "nombre_tabla": nombre_tabla,
                        "root_metada_path": row_dict1.get("root_metada_path"),
                        "tiene_shist": str(row.p_requiere_shist),
                        "objeto_existe": objeto_existe,
                    }
                ]
        except Exception as err:
            ingest["fecha_fin"] = get_fecha_sql()
            ingest["error"] = str(err)
            ingest["estado"] = "FALLIDO"
        # logs
        ingest_list.append(ingest)
    output_df = (
        spark.createDataFrame(ingest_list).where("id_ingesta!=''").selectExpr("*")
    )
    return output_df
