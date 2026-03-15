# Diagrama de flujo — `pys_lectura_normalizacion.py`

```mermaid
flowchart LR
  %% Subproceso 1
  subgraph S1[Subproceso 1: Inicio y resolución de entradas]
    direction TB
    s1_1[Inicio pyspark_transform]
    s1_2[Inicializar process_log y helpers base:
append_log / build_error_df / is_missing / sanitize_error_message]
    s1_3[Resolver input_df y param_read_row
(resolve_input_frame)]
    s1_4[Extraer BUCKET_BLOB]
    s1_5[Log: Inicio pys_lectura_normalizacion]
    s1_1 --> s1_2 --> s1_3 --> s1_4 --> s1_5 --> A((A))
  end

  %% Subproceso 2
  subgraph S2[Subproceso 2: Passthrough de error upstream y READER_OPTIONS]
    direction TB
    A2((A))
    s2_1{¿input_df tiene contrato
estado,error,log_control,log_detail?}
    s2_2{¿estado == FALLIDO?}
    s2_3[Retornar contrato FALLIDO upstream]
    s2_4[Leer READER_OPTIONS]
    s2_5{¿READER_OPTIONS válido?
(dict o JSON parseable)}
    s2_6[Retornar FALLIDO:
READER_OPTIONS inválido]
    A2 --> s2_1
    s2_1 -- Sí --> s2_2
    s2_2 -- Sí --> s2_3 --> Z1((FIN))
    s2_2 -- No --> s2_4
    s2_1 -- No --> s2_4
    s2_4 --> s2_5
    s2_5 -- No --> s2_6 --> Z2((FIN))
    s2_5 -- Sí --> B((B))
  end

  %% Subproceso 3
  subgraph S3[Subproceso 3: Resolver contrato de entrada (files_df)]
    direction TB
    B2((B))
    s3_1{¿input_df existe y es DataFrame?}
    s3_2[Retornar FALLIDO:
no hay DataFrame de entrada]
    s3_3{¿input_df contiene column path?}
    s3_4[Construir files_df desde input_df(path,status,error,source_file)]
    s3_5[Log: Input de descubrimiento con paths detectado]
    s3_6{¿input_df trae contrato estandarizado?}
    s3_7{¿BUCKET_BLOB y relative_upload_file_path válidos?}
    s3_8[Retornar FALLIDO:
faltan parámetros requeridos]
    s3_9[Leer binaryFile en s3a://bucket/path y construir files_df PENDIENTE]
    s3_10[Log: descubrimiento interno ejecutado]
    s3_11[Retornar FALLIDO:
input no tiene contrato soportado]
    B2 --> s3_1
    s3_1 -- No --> s3_2 --> Z3((FIN))
    s3_1 -- Sí --> s3_3
    s3_3 -- Sí --> s3_4 --> s3_5 --> C((C))
    s3_3 -- No --> s3_6
    s3_6 -- No --> s3_11 --> Z4((FIN))
    s3_6 -- Sí --> s3_7
    s3_7 -- No --> s3_8 --> Z5((FIN))
    s3_7 -- Sí --> s3_9 --> s3_10 --> C2((C))
  end

  %% Subproceso 4
  subgraph S4[Subproceso 4: Preparación de listas y helpers de lectura]
    direction TB
    C3((C))
    s4_1[files_df.distinct().collect()]
    s4_2[Construir files (status=PENDIENTE)
y inherited_errors (status ERROR_*)]
    s4_3[Definir helpers:
apply_poi_runtime_overrides / compression_suffix / get_ext]
    s4_4[Definir helpers:
dataset_name / resolve_zip_staging_target / expand_input_paths]
    s4_5[Definir helpers:
cast_all_to_string / read_dynamic(csv-txt-json-parquet-excel)]
    C3 --> s4_1 --> s4_2 --> s4_3 --> s4_4 --> s4_5 --> D((D))
  end

  %% Subproceso 5
  subgraph S5[Subproceso 5: Inicialización de acumuladores y errores heredados]
    direction TB
    D2((D))
    s5_1[Inicializar datasets={} y error_records=[]]
    s5_2[Recorrer inherited_errors]
    s5_3[Agregar error_records con ERROR_UPSTREAM]
    D2 --> s5_1 --> s5_2 --> s5_3 --> E((E))
  end

  %% Subproceso 6
  subgraph S6[Subproceso 6: Lectura dinámica y unión por dataset]
    direction TB
    E2((E))
    s6_1[Para cada file en files]
    s6_2{¿expand_input_paths(file)
exitoso?}
    s6_3[Registrar ERROR_LECTURA y continuar]
    s6_4[Para cada target expandido]
    s6_5{¿read_dynamic(read_path)
exitoso?}
    s6_6[Registrar ERROR_LECTURA y continuar]
    s6_7[Enriquecer DF: ingestion_ts, source_file, path,
batch_id, dataset, record_status, error_*]
    s6_8[cast_all_to_string]
    s6_9[UnionByName en datasets[dset]]
    E2 --> s6_1 --> s6_2
    s6_2 -- No --> s6_3 --> s6_1
    s6_2 -- Sí --> s6_4 --> s6_5
    s6_5 -- No --> s6_6 --> s6_4
    s6_5 -- Sí --> s6_7 --> s6_8 --> s6_9 --> s6_4
    s6_4 --> F((F))
  end

  %% Subproceso 7
  subgraph S7[Subproceso 7: Consolidación final y anexado de errores]
    direction TB
    F2((F))
    s7_1{¿datasets vacío y error_records vacío?}
    s7_2[Retornar DataFrame vacío con esquema base]
    s7_3[Construir final_df con union de datasets]
    s7_4{¿Hay error_records?}
    s7_5[Crear error_df + error_stage=LECTURA_NORMALIZACION]
    s7_6[Union final_df con error_df]
    s7_7{¿final_df es None?}
    s7_8[Retornar FALLIDO:
no fue posible construir final_df]
    F2 --> s7_1
    s7_1 -- Sí --> s7_2 --> Z6((FIN))
    s7_1 -- No --> s7_3 --> s7_4
    s7_4 -- Sí --> s7_5 --> s7_6 --> s7_7
    s7_4 -- No --> s7_7
    s7_7 -- Sí --> s7_8 --> Z7((FIN))
    s7_7 -- No --> G((G))
  end

  %% Subproceso 8
  subgraph S8[Subproceso 8: Limpieza de columnas de contrato y salida]
    direction TB
    G2((G))
    s8_1[Eliminar columnas estado/error/log_control/log_detail si existen]
    s8_2{¿error_records está vacío?}
    s8_3[Generar salida de éxito:
fecha_ingesta, nombre_archivo y select final]
    s8_4[Log: Lectura y normalización finalizada]
    s8_5[Retornar final_df]
    G2 --> s8_1 --> s8_2
    s8_2 -- Sí --> s8_3 --> s8_4 --> s8_5 --> Z8((FIN))
    s8_2 -- No --> s8_4 --> s8_5 --> Z9((FIN))
  end

  %% Conectores entre subprocesos
  A --> A2
  B --> B2
  C --> C3
  C2 --> C3
  D --> D2
  E --> E2
  F --> F2
  G --> G2
```
