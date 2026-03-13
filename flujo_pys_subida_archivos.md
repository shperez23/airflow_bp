# Diagrama de flujo — `pys_subida_archivos.py`

```mermaid
flowchart LR
  %% Subproceso 1
  subgraph S1[Subproceso 1: Inicio, contrato de salida y utilidades]
    direction TB
    s1_1[Inicio pyspark_transform]
    s1_2[Inicializar process_log]
    s1_3[Definir helpers:
append_log, build_status_df,
get_*_param, sanitize_error_message,
build_expected_filenames]
    s1_4[Log: Inicio pys_subida_archivos]
    s1_1 --> s1_2 --> s1_3 --> s1_4 --> A((A))
  end

  %% Subproceso 2
  subgraph S2[Subproceso 2: Validación de entrada y parametría base]
    direction TB
    A2((A))
    s2_1[Validar columnas requeridas en df]
    s2_2[Leer param_row = df.first()]
    s2_3[Extraer SFTP_HOST, SFTP_PORT,
SFTP_VAULT_NAME, NOMBRE_ARCHIVO,
FECHA_DESDE, FECHA_HASTA,
SFTP_PATH, BUCKET_BLOB]
    s2_4{¿Parámetros obligatorios presentes?}
    s2_5[Retornar FALLIDO por
faltantes de parametría]
    A2 --> s2_1 --> s2_2 --> s2_3 --> s2_4
    s2_4 -- No --> s2_5 --> Z1((FIN))
    s2_4 -- Sí --> B((B))
  end

  %% Subproceso 3
  subgraph S3[Subproceso 3: Credenciales, rutas y cliente S3]
    direction TB
    B2((B))
    s3_1[Obtener credenciales desde
spark.db.{vault}.user/pass]
    s3_2{¿Credenciales y rutas válidas?}
    s3_3[Retornar FALLIDO por
credenciales/rutas faltantes]
    s3_4[Construir expected_filenames
(según rango FECHA_DESDE/FECHA_HASTA)]
    s3_5[Inicializar sesión boto3 y
cliente s3]
    s3_6{¿S3 inicializado correctamente?}
    s3_7[Retornar FALLIDO por
error al crear cliente S3]
    B2 --> s3_1 --> s3_2
    s3_2 -- No --> s3_3 --> Z2((FIN))
    s3_2 -- Sí --> s3_4 --> s3_5 --> s3_6
    s3_6 -- No --> s3_7 --> Z3((FIN))
    s3_6 -- Sí --> C((C))
  end

  %% Subproceso 4
  subgraph S4[Subproceso 4: Performance, checkpoints y namespaces]
    direction TB
    C2((C))
    s4_1[Resolver parámetros de performance:
max_concurrency, use_threads,
multipart_chunksize_mb,
multipart_threshold_mb]
    s4_2[Resolver readiness policy:
readiness_wait_seconds,
readiness_skip_wait_age_seconds]
    s4_3[Crear TransferConfig]
    s4_4[Definir CHECKPOINT,
QUARANTINE, RAW_PREFIX,
UNCOMP_PREFIX]
    s4_5[Definir helpers:
file_hash y list_files_recursive]
    C2 --> s4_1 --> s4_2 --> s4_3 --> s4_4 --> s4_5 --> D((D))
  end

  %% Subproceso 5
  subgraph S5[Subproceso 5: Carga de checkpoint e inicio de conexión SFTP]
    direction TB
    D2((D))
    s5_1[Leer checkpoint parquet
(si existe) y normalizar columnas]
    s5_2[Construir processed_hashes y
processed_signatures]
    s5_3[Inicializar resultados:
resultados, uploaded_paths,
checkpoint_records]
    s5_4[Abrir conexión SFTP]
    s5_5{¿Conexión SFTP exitosa?}
    s5_6[Retornar FALLIDO
por error general de subida]
    D2 --> s5_1 --> s5_2 --> s5_3 --> s5_4 --> s5_5
    s5_5 -- No --> s5_6 --> Z4((FIN))
    s5_5 -- Sí --> E((E))
  end

  %% Subproceso 6
  subgraph S6[Subproceso 6: Descubrimiento y filtro inicial de archivos]
    direction TB
    E2((E))
    s6_1[Listar archivos recursivamente
en SFTP_PATH]
    s6_2[Para cada archivo remoto:
calcular rel_path, filename,
tmp_file]
    s6_3{¿filename está en
expected_filenames?}
    s6_4[Omitir archivo y continuar]
    E2 --> s6_1 --> s6_2 --> s6_3
    s6_3 -- No --> s6_4 --> s6_2
    s6_3 -- Sí --> F((F))
  end

  %% Subproceso 7
  subgraph S7[Subproceso 7: Readiness, idempotencia y carga a S3]
    direction TB
    F2((F))
    s7_1[Leer stat remoto: size1, mtime1]
    s7_2{¿Signature ya procesada?}
    s7_3[Registrar OMITIDO_YA_PROCESADO
(metadata checkpoint)]
    s7_4[Aplicar readiness:
espera y revalidación size2]
    s7_5{¿Archivo listo
(size estable y > 0)?}
    s7_6[Registrar OMITIDO_NO_LISTO]
    s7_7[Descargar tmp_file]
    s7_8[Calcular hash SHA-256]
    s7_9{¿Hash ya procesado?}
    s7_10[Registrar OMITIDO_YA_PROCESADO
(hash)]
    s7_11[Subir raw a S3 (RAW_PREFIX)]
    s7_12[Detectar gzip real]
    s7_13{¿Es gzip?}
    s7_14[Descomprimir y subir a
UNCOMP_PREFIX]
    s7_15[Agregar checkpoint_records,
actualizar sets y resultados PROCESADO]
    s7_16{¿Error en archivo?}
    s7_17[Subir a QUARANTINE,
registrar ERROR_CARGA]
    F2 --> s7_1 --> s7_2
    s7_2 -- Sí --> s7_3 --> E3((E_LOOP))
    s7_2 -- No --> s7_4 --> s7_5
    s7_5 -- No --> s7_6 --> E3
    s7_5 -- Sí --> s7_7 --> s7_8 --> s7_9
    s7_9 -- Sí --> s7_10 --> E3
    s7_9 -- No --> s7_11 --> s7_12 --> s7_13
    s7_13 -- Sí --> s7_14 --> s7_15 --> E3
    s7_13 -- No --> s7_15 --> E3
    s7_7 -. excepción .-> s7_16
    s7_11 -. excepción .-> s7_16
    s7_14 -. excepción .-> s7_16
    s7_16 -- Sí --> s7_17 --> E3
    E3 --> s6_2
    E3 --> G((G))
  end

  %% Subproceso 8
  subgraph S8[Subproceso 8: Persistencia de checkpoint y salida final]
    direction TB
    G2((G))
    s8_1{¿checkpoint_records > 0?}
    s8_2[Persistir checkpoint parquet
(append) + log]
    s8_3[Calcular resumen:
procesados, errores, omitidos]
    s8_4{¿total_error > 0?}
    s8_5[Retornar FALLIDO por errores]
    s8_6[Construir unique_uploaded_paths]
    s8_7{¿Hay uploads?}
    s8_8[Retornar DataFrame con
columna path]
    s8_9{¿Hay omitidos?}
    s8_10[Retornar FALLIDO por omitidos]
    s8_11[Retornar FALLIDO:
no se encontraron archivos válidos]
    G2 --> s8_1
    s8_1 -- Sí --> s8_2 --> s8_3
    s8_1 -- No --> s8_3
    s8_3 --> s8_4
    s8_4 -- Sí --> s8_5 --> Z5((FIN))
    s8_4 -- No --> s8_6 --> s8_7
    s8_7 -- Sí --> s8_8 --> Z6((FIN))
    s8_7 -- No --> s8_9
    s8_9 -- Sí --> s8_10 --> Z7((FIN))
    s8_9 -- No --> s8_11 --> Z8((FIN))
  end

  %% Conectores entre subprocesos (fin -> inicio)
  A --> A2
  B --> B2
  C --> C2
  D --> D2
  E --> E2
  F --> F2
  G --> G2
```
