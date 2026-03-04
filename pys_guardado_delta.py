def pyspark_transform(spark, dict_df, param_dict):
    # leer dataframes *****
    # parametros
    try:
        row_info = dict_df.get("tri_parametros").limit(1).collect()[0]
    except Exception as error_pars:
        print(error_pars)
        row_info = dict_df.collect()[0]
        result_list = [
            {
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
        ]
        output_df = spark.createDataFrame(result_list).selectExpr("*")
        return output_df
    # obtener parametros de flujo
    p_start_range = row_info.rangoIni
    p_end_range = row_info.rangoFin
    p_target_location = row_info.tablaUbicacion
    # obtener parametros
    p_reprocess_condition = row_info.condicionReproceso
    p_partition_columns = row_info.columnasParticion
    p_optimize_columns = row_info.columnasOptimizar

    # dataframe a insertar
    df = dict_df.get("tri_transformacion").persist()
    output_df = df

    # parametros tipoCarga, default si no se envia
    p_type_table = "history"
    p_shist = 0
    p_target = ""
    # tipo Tabla
    try:
        p_type_table = row_info.tipoTabla
        p_shist = row_info.requiereShist
    except Exception as error_pars:
        print(error_pars)
    # condicion Stock
    p_stock_condition = "periodo = DATE_SUB(FROM_UTC_TIMESTAMP(current_timestamp(), 'America/Guayaquil'),1)"
    try:
        p_stock_condition = row_info.condicionStock
    except Exception as error_pars2:
        print(error_pars2)
    # crear sufijo para tablas shist
    if p_type_table == "stock" and p_shist == 1:
        p_target = "_shist"
    # valores procesados
    p_star_process = ""
    p_end_process = ""
    p_state = "FINALIZADO"

    # condicion de reproceso
    p_replace_where = p_reprocess_condition.replace(
        "<RANGO_INI>", p_start_range
    ).replace("<RANGO_FIN>", p_end_range)

    # logs
    error = ""
    reprocess_message = ""
    vaccum_message = ""
    write_message = ""
    reproceso = 0
    table_exists = 0
    target_schema_dict = {}
    input_schema_dict = {}
    cast_expr = []

    # obtener tabla reproceso, control de try en caso de no existir
    p_reprocess_table = ""
    output_count = df.count()
    if output_count > 0:
        # opciones para escritura tipo history o snap history (shist)
        if p_type_table == "history" or p_shist == 1:
            try:
                p_reprocess_table = row_info.tablaReproceso + p_target
                # intentar leer tabla
                df_target = spark.table(p_reprocess_table)
                table_exists = 1
                target_schema = df_target.schema
                # captura esquema entrada
                input_schema = df.schema
                # convertir a diccionario
                target_dict = {}
                for col in target_schema:
                    col_name = (col.name).lower()
                    target_dict[col_name] = col.dataType.simpleString()
                    target_schema_dict[col.name] = col.dataType.simpleString()
                # Crear expresion de cast
                for col in input_schema:
                    source_name = col.name
                    source_type = col.dataType.simpleString()
                    input_schema_dict[source_name] = source_type
                    target_type = target_dict.get(source_name.lower())
                    if target_type is not None and target_type != source_type:
                        cast_expr.append(
                            f"CAST({source_name} AS {target_type}) AS {source_name}"
                        )
                    else:
                        cast_expr.append(source_name)
            except Exception as err:
                error = str(err)
                cast_expr = ["*"]
            # expresion casteo a destino
            output_df = df.selectExpr(*cast_expr).persist()
            # habilitar sobreescritura por particion
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            # reproceso
            try:
                if table_exists == 1:
                    df_del = spark.sql(
                        f"DELETE FROM {p_reprocess_table} WHERE {p_replace_where}"
                    )
                    reprocess_message = str(df_del.collect())
                    reproceso = 1
            except Exception as err2:
                reprocess_message = str(err2)
            # vacuum
            try:
                if table_exists == 1:
                    df_v = spark.sql(f"VACUUM {p_reprocess_table}")
                    vaccum_message = str(df_v.collect())
            except Exception as err3:
                vaccum_message = str(err3)
            if table_exists == 0 or (table_exists == 1 and reproceso == 1):
                # escritura en modo append, ya que ya se ejecuto el reproceso
                output_df.write.format("delta").mode("append").option(
                    "zorderBy", p_optimize_columns
                ).partitionBy(p_partition_columns.split(",")).option(
                    "mergeSchema", "true"
                ).save(
                    p_target_location + p_target
                )
                write_message = "Modo Append"
            else:
                try:
                    # sobreescritura por particion si ya existe
                    output_df.write.format("delta").mode("overwrite").option(
                        "zorderBy", p_optimize_columns
                    ).partitionBy(p_partition_columns.split(",")).option(
                        "mergeSchema", "true"
                    ).option(
                        "replaceWhere", p_replace_where
                    ).save(
                        p_target_location + p_target
                    )
                    write_message = "Modo Sobreescritura por partición"
                except Exception as err4:
                    try:
                        error = str(err4)
                        # guardar primera vez
                        output_df.write.format("delta").mode("overwrite").option(
                            "zorderBy", p_optimize_columns
                        ).partitionBy(p_partition_columns.split(",")).option(
                            "mergeSchema", "true"
                        ).save(
                            p_target_location + p_target
                        )
                        write_message = "Primera Escritura"
                    except Exception as err5:
                        error = str(err5)
                        p_state = "FALLIDO"
                        output_count = 0
        # guardar stock
        if p_type_table == "stock":
            df_stock = output_df.where(p_stock_condition)
            if df_stock.count() > 0:
                df_stock.write.format("delta").mode("overwrite").option(
                    "zorderBy", p_optimize_columns
                ).option("mergeSchema", "true").save(p_target_location)
                write_message = write_message + ", Escritura con stock"
            # vacuum
            try:
                df_v = spark.sql(f"VACUUM {p_reprocess_table}")
            except Exception as error_vac:
                print(error_vac)
        # obtener resumen de carga
        row_summary = df.selectExpr(
            "MIN(periodo) as rangoIniProcesado", "MAX(periodo) as rangoFinProcesado"
        ).collect()[0]
        p_star_process = row_summary.rangoIniProcesado
        p_end_process = row_summary.rangoFinProcesado
    else:
        error = "SIN DATOS"
        p_state = "FALLIDO"
        p_star_process = ""
        p_end_process = ""
    result_list = [
        {
            "estado": p_state,
            "error": str(error),
            "reproceso": p_replace_where,
            "reprocesoLog": reprocess_message,
            "vacuumLog": vaccum_message,
            "escrituraLog": write_message,
            "cantidadInsertado": str(output_count),
            "esquemaEntrada": str(input_schema_dict),
            "esquemaDestino": str(target_schema_dict),
            "casteoAplicado": str(cast_expr),
            "rangoIniProcesado": str(p_star_process),
            "rangoFinProcesado": str(p_end_process),
        }
    ]
    output_df = spark.createDataFrame(result_list).selectExpr("*")
    return output_df