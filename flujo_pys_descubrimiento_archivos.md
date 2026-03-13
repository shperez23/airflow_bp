# Diagrama de flujo — `pys_descubrimiento_archivos.py`

```mermaid
flowchart LR
  %% Subproceso 1
  subgraph S1[Subproceso 1: Inicio y resolución de entradas]
    direction TB
    s1_1[Inicio pyspark_transform]
    s1_2[Inicializar process_log y helpers\nappend_log / build_status_df / is_missing]
    s1_3[Resolver entradas:\nupload_df y param_discovery_row]
    s1_4[Extraer BUCKET_BLOB y BUCKET_RAW]
    s1_1 --> s1_2 --> s1_3 --> s1_4 --> A((A))
  end

  %% Subproceso 2
  subgraph S2[Subproceso 2: Detección de contrato de entrada]
    direction TB
    A2((A))
    s2_1[Detectar columnas de upload_df]
    s2_2{¿Contrato\nestandarizado?\n(estado,error,log_*)}
    s2_3{¿Contrato éxito\nupload?\n(path)}
    A2 --> s2_1 --> s2_2
    s2_2 -- Sí --> B((B))
    s2_2 -- No --> s2_3
    s2_3 -- Sí --> C((C))
    s2_3 -- No --> D((D))
  end

  %% Subproceso 3
  subgraph S3[Subproceso 3: Rama contrato estandarizado]
    direction TB
    B2((B))
    s3_1[Leer primera fila de estado]
    s3_2{¿estado == FALLIDO?}
    s3_3[Log: contrato FALLIDO recibido]
    s3_4[Retornar columnas\nestado,error,log_control,log_detail]
    s3_5[Retornar FALLIDO:\ncontrato estandarizado sin paths]
    B2 --> s3_1 --> s3_2
    s3_2 -- Sí --> s3_3 --> s3_4 --> Z1((FIN))
    s3_2 -- No --> s3_5 --> Z2((FIN))
  end

  %% Subproceso 4
  subgraph S4[Subproceso 4: Rama contrato upload exitoso]
    direction TB
    C2((C))
    s4_1[Filtrar path no nulo/no vacío]
    s4_2[Construir pending con:\npath, discovery_status=PENDIENTE,\nerror_message=null, source_file=path]
    s4_3[distinct()]
    s4_4[Log: contrato éxito detectado]
    C2 --> s4_1 --> s4_2 --> s4_3 --> s4_4 --> E((E))
  end

  %% Subproceso 5
  subgraph S5[Subproceso 5: Rama standalone]
    direction TB
    D2((D))
    s5_1[Leer relative_upload_file_path]
    s5_2{¿BUCKET_BLOB válido?}
    s5_3[Retornar FALLIDO:\nFalta BUCKET_BLOB]
    s5_4{¿relative_upload_file_path válido?}
    s5_5[Retornar FALLIDO:\nFalta relative_upload_file_path]
    s5_6[Construir raw_path:\ns3a://BUCKET_BLOB/.../original/]
    s5_7[Leer binaryFile recursivo]
    s5_8[Construir pending y distinct()]
    s5_9[Log: standalone ejecutado]
    D2 --> s5_1 --> s5_2
    s5_2 -- No --> s5_3 --> Z3((FIN))
    s5_2 -- Sí --> s5_4
    s5_4 -- No --> s5_5 --> Z4((FIN))
    s5_4 -- Sí --> s5_6 --> s5_7 --> s5_8 --> s5_9 --> E2((E))
  end

  %% Subproceso 6
  subgraph S6[Subproceso 6: Validación de checkpoint y exclusión incremental]
    direction TB
    E3((E))
    s6_1[Leer checkpoint_prefix]
    s6_2{¿BUCKET_RAW válido?}
    s6_3[Retornar FALLIDO:\nFalta BUCKET_RAW]
    s6_4{¿checkpoint_prefix válido?}
    s6_5[Retornar FALLIDO:\nFalta checkpoint_prefix]
    s6_6[Construir checkpoint_path]
    s6_7{¿Lectura parquet\ncheckpoint exitosa?}
    s6_8[Adaptar contrato checkpoint:\npath y/o source_file]
    s6_9[Normalizar processed y distinct()]
    s6_10[pending left_anti processed]
    s6_11[Log: checkpoint aplicado]
    s6_12[pending = pending.distinct()]
    s6_13[Log: checkpoint no disponible]
    E3 --> s6_1 --> s6_2
    s6_2 -- No --> s6_3 --> Z5((FIN))
    s6_2 -- Sí --> s6_4
    s6_4 -- No --> s6_5 --> Z6((FIN))
    s6_4 -- Sí --> s6_6 --> s6_7
    s6_7 -- Sí --> s6_8 --> s6_9 --> s6_10 --> s6_11 --> F((F))
    s6_7 -- No --> s6_12 --> s6_13 --> F2((F))
  end

  %% Subproceso 7
  subgraph S7[Subproceso 7: Resultado final]
    direction TB
    F3((F))
    s7_1[Calcular total_pending válido]
    s7_2[Log: Archivos pendientes detectados]
    s7_3[Log: Descubrimiento finalizado]
    s7_4[Retornar pending]
    F3 --> s7_1 --> s7_2 --> s7_3 --> s7_4 --> Z7((FIN))
  end

  %% Conectores entre subprocesos (fin->inicio)
  A --> A2
  B --> B2
  C --> C2
  D --> D2
  E --> E3
  E2 --> E3
  F --> F3
  F2 --> F3
```
