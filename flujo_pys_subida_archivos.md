# Diagrama de flujo técnico — `pys_subida_archivos.py`

```mermaid
flowchart LR
  %% =========================
  %% Subproceso 1
  %% =========================
  subgraph S1[Subproceso 1: Inicialización y contrato de entrada]
    direction TB
    s1_0([Inicio: pyspark_transform])
    s1_1[Inicializar process_log y helpers\nappend_log, build_status_df, sanitizadores]
    s1_2[Definir columnas requeridas\nSFTP_HOST, SFTP_VAULT_NAME, NOMBRE_ARCHIVO, SFTP_PATH, BUCKET_BLOB]
    s1_3{¿Se pueden leer columnas de df?}
    s1_4([Fin FALLIDO:\nNo fue posible leer columnas])
    s1_5{¿Faltan columnas requeridas?}
    s1_6([Fin FALLIDO:\nFaltan columnas requeridas])
    s1_7{¿df.first() disponible?}
    s1_8([Fin FALLIDO:\nNo fue posible leer parametría])
    s1_9{¿Existe fila de parámetros?}
    s1_10([Fin FALLIDO:\nSin registros de parametría])
    s1_11[Extraer parámetros base\n(host, port, vault, archivo, fechas, path, bucket)]
    s1_12{¿Faltan SFTP_HOST / SFTP_VAULT_NAME / NOMBRE_ARCHIVO?}
    s1_13([Fin FALLIDO:\nParámetro requerido ausente])

    s1_0 --> s1_1 --> s1_2 --> s1_3
    s1_3 -- No --> s1_4
    s1_3 -- Sí --> s1_5
    s1_5 -- Sí --> s1_6
    s1_5 -- No --> s1_7
    s1_7 -- No --> s1_8
    s1_7 -- Sí --> s1_9
    s1_9 -- No --> s1_10
    s1_9 -- Sí --> s1_11 --> s1_12
    s1_12 -- Sí --> s1_13
    s1_12 -- No --> A((A))
  end

  %% =========================
  %% Subproceso 2
  %% =========================
  subgraph S2[Subproceso 2: Validaciones, secretos y configuración]
    direction TB
    a2((A))
    s2_1[Leer credenciales desde spark.conf\nspark.db.{vault}.user / pass]
    s2_2{¿Credenciales disponibles?}
    s2_3([Fin FALLIDO:\nCredenciales faltantes])
    s2_4{¿SFTP_PATH válido?}
    s2_5([Fin FALLIDO:\nFalta SFTP_PATH])
    s2_6[Configurar conexión SFTP\ncnopts.hostkeys=None]
    s2_7[Leer rutas destino de param_dict\nrelative_upload_file_path y relative_upload_control_path]
    s2_8{¿Bucket y rutas destino válidos?}
    s2_9([Fin FALLIDO:\nFaltan bucket/rutas])
    s2_10[Construir expected_filenames\n(rango diario o nombre único)]
    s2_11{¿Fechas/formato válidos?}
    s2_12([Fin FALLIDO:\nError en ventana de fechas])
    s2_13[Crear sesión y cliente S3 con boto3]
    s2_14{¿Cliente S3 inicializado?}
    s2_15([Fin FALLIDO:\nNo fue posible inicializar S3])
    s2_16[Resolver parámetros técnicos\nconcurrencia, multipart, readiness]
    s2_17[Construir TransferConfig y prefijos\nCHECKPOINT, QUARANTINE, RAW, UNCOMP]

    a2 --> s2_1 --> s2_2
    s2_2 -- No --> s2_3
    s2_2 -- Sí --> s2_4
    s2_4 -- No --> s2_5
    s2_4 -- Sí --> s2_6 --> s2_7 --> s2_8
    s2_8 -- No --> s2_9
    s2_8 -- Sí --> s2_10 --> s2_11
    s2_11 -- No --> s2_12
    s2_11 -- Sí --> s2_13 --> s2_14
    s2_14 -- No --> s2_15
    s2_14 -- Sí --> s2_16 --> s2_17 --> B((B))
  end

  %% =========================
  %% Subproceso 3
  %% =========================
  subgraph S3[Subproceso 3: Carga de checkpoint e idempotencia]
    direction TB
    b3((B))
    s3_1[Inicializar estructuras:\nprocessed_hashes, processed_signatures]
    s3_2[Intentar leer parquet de CHECKPOINT]
    s3_3{¿Checkpoint legible?}
    s3_4[Normalizar columnas faltantes\nremote_size/remote_mtime]
    s3_5[Cargar hashes y firmas\n(relative_path,size,mtime)]
    s3_6[Continuar sin checkpoint\n(sin error fatal)]
    s3_7[Inicializar resultados:\nresultados, uploaded_paths, checkpoint_records]

    b3 --> s3_1 --> s3_2 --> s3_3
    s3_3 -- Sí --> s3_4 --> s3_5 --> s3_7
    s3_3 -- No --> s3_6 --> s3_7
    s3_7 --> C((C))
  end

  %% =========================
  %% Subproceso 4
  %% =========================
  subgraph S4[Subproceso 4: Descubrimiento y filtro de archivos SFTP]
    direction TB
    c4((C))
    s4_1[Conectar a SFTP (with pysftp.Connection)]
    s4_2[Listar archivos recursivamente\n(list_files_recursive)]
    s4_3{¿Nombre remoto ∈ expected_filenames?}
    s4_4[Omitir archivo por filtro de naming]
    s4_5[Obtener stat inicial\nsize1 y mtime1]
    s4_6{¿Firma metadata ya procesada?}
    s4_7[Registrar OMITIDO_YA_PROCESADO\n(checkpoint metadata)]
    s4_8{¿Archivo reciente (< skip_wait_age)?}
    s4_9[Esperar readiness_wait_seconds\ny releer tamaño size2]
    s4_10{¿size estable y > 0?}
    s4_11[Registrar OMITIDO_NO_LISTO]

    c4 --> s4_1 --> s4_2 --> s4_3
    s4_3 -- No --> s4_4 --> s4_3
    s4_3 -- Sí --> s4_5 --> s4_6
    s4_6 -- Sí --> s4_7 --> s4_3
    s4_6 -- No --> s4_8
    s4_8 -- Sí --> s4_9 --> s4_10
    s4_8 -- No --> s4_10
    s4_10 -- No --> s4_11 --> s4_3
    s4_10 -- Sí --> D((D))
  end

  %% =========================
  %% Subproceso 5
  %% =========================
  subgraph S5[Subproceso 5: Descarga, subida S3 y tratamiento de compresión]
    direction TB
    d5((D))
    s5_1[Descargar remoto a /tmp\n(sftp.get)]
    s5_2[Calcular hash SHA-256]
    s5_3{¿Hash ya procesado?}
    s5_4[Eliminar tmp y registrar\nOMITIDO_YA_PROCESADO (hash)]
    s5_5[Subir archivo original a S3\nRAW_PREFIX + rel_path]
    s5_6[Detectar si archivo es gzip real]
    s5_7{¿Es gzip?}
    s5_8[Descomprimir a tmp_uncomp]
    s5_9[Subir descomprimido a S3\nUNCOMP_PREFIX]
    s5_10[Registrar checkpoint record\n(path, hash, key, size, mtime)]
    s5_11[Actualizar sets en memoria\nprocessed_hashes/signatures]
    s5_12[Registrar PROCESADO y uploaded_paths\nappend_log éxito]
    s5_13[Eliminar temporales]
    s5_14[En excepción de carga:\nSubir a QUARANTINE (si aplica),\nregistrar ERROR_CARGA y log]

    d5 --> s5_1 --> s5_2 --> s5_3
    s5_3 -- Sí --> s5_4 --> C2((C-LOOP))
    s5_3 -- No --> s5_5 --> s5_6 --> s5_7
    s5_7 -- Sí --> s5_8 --> s5_9 --> s5_10
    s5_7 -- No --> s5_10
    s5_10 --> s5_11 --> s5_12 --> s5_13 --> C2
    d5 -. excepción .-> s5_14 --> C2
  end

  %% =========================
  %% Subproceso 6
  %% =========================
  subgraph S6[Subproceso 6: Persistencia de checkpoint y cierre de conexión]
    direction TB
    c6((C-LOOP))
    s6_1{¿Quedan archivos por iterar?}
    s6_2[Volver a Subproceso de filtro/ready]
    s6_3[Si checkpoint_records > 0:\ncrear DataFrame + ingestion_ts\nwrite append parquet CHECKPOINT]
    s6_4{¿Fallo general del bloque SFTP?}
    s6_5([Fin FALLIDO:\nFallo general en subida])

    c6 --> s6_1
    s6_1 -- Sí --> s6_2 --> C
    s6_1 -- No --> s6_3 --> s6_4
    s6_4 -- Sí --> s6_5
    s6_4 -- No --> E((E))
  end

  %% =========================
  %% Subproceso 7
  %% =========================
  subgraph S7[Subproceso 7: Consolidación de resultado y finalización]
    direction TB
    e7((E))
    s7_1[Calcular totales\nPROCESADO / ERROR_* / OMITIDO_*]
    s7_2{¿total_error > 0?}
    s7_3([Fin FALLIDO:\nErrores durante carga])
    s7_4[Deduplicar uploaded_paths]
    s7_5{¿Hay paths cargados?}
    s7_6([Fin ÉXITO:\nRetornar DataFrame(path)])
    s7_7{¿Hubo omitidos?}
    s7_8([Fin FALLIDO:\nNo se cargaron archivos por reglas])
    s7_9([Fin FALLIDO:\nNo se encontraron archivos válidos])

    e7 --> s7_1 --> s7_2
    s7_2 -- Sí --> s7_3
    s7_2 -- No --> s7_4 --> s7_5
    s7_5 -- Sí --> s7_6
    s7_5 -- No --> s7_7
    s7_7 -- Sí --> s7_8
    s7_7 -- No --> s7_9
  end

  %% Conectores entre subprocesos
  A --> a2
  B --> b3
  C --> c4
  D --> d5
  C2 --> c6
  E --> e7
```
