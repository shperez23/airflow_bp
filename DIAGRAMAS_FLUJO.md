# Diagramas de flujo de los scripts PySpark

Este documento resume el flujo funcional de cada archivo `.py` del repositorio mediante diagramas Mermaid.

## Arquitectura inferida del proyecto

Se observa un **pipeline de ingestión modular por etapas** (upload → discovery → read/normalize → summary), con contratos de DataFrame entre nodos y patrones de resiliencia (checkpoint, idempotencia, quarantine, manejo de errores heredados).

## 1) `pys_upload.py`

```mermaid
flowchart TD
    A[Inicio pyspark_transform] --> B[Leer parámetros desde df.first]
    B --> C{Parámetros requeridos y credenciales OK?}
    C -- No --> Z1[ValueError]
    C -- Sí --> D[Construir expected_filenames por rango de fechas]
    D --> E[Inicializar cliente S3 + TransferConfig]
    E --> F[Leer checkpoint previo: hashes + firmas metadata]
    F --> G[Conectar a SFTP y descubrir archivos recursivamente]
    G --> H{Archivo coincide con expected_filenames?}
    H -- No --> HN[Ignorar archivo]
    H -- Sí --> I[Validar readiness por tamaño/mtime]
    I --> J{Ya procesado por firma metadata?}
    J -- Sí --> JS[Registrar SKIPPED_ALREADY_PROCESSED]
    J -- No --> K{Archivo estable y no vacío?}
    K -- No --> KS[Registrar SKIPPED_NOT_READY]
    K -- Sí --> L[Descargar temporal y calcular hash]
    L --> M{Ya procesado por hash?}
    M -- Sí --> MS[Registrar SKIPPED_ALREADY_PROCESSED]
    M -- No --> N[Subir a RAW_PREFIX en S3]
    N --> O{¿Es gzip válido?}
    O -- Sí --> OP[Descomprimir y subir a UNCOMP_PREFIX]
    O -- No --> OQ[Continuar]
    OP --> P[Agregar checkpoint_records + resultado PROCESADO]
    OQ --> P
    G -->|Error por archivo| QE[Subir a QUARANTINE + ERROR_UPLOAD]
    P --> R[Persistir checkpoint parquet append]
    R --> S[Construir dataframe auditoria]
    S --> T[Return DataFrame]
```

## 2) `pys_dicovery_node.py`

```mermaid
flowchart TD
    A[Inicio pyspark_transform] --> B[Resolver flags include_upload_errors / skipped]
    B --> C{Input contrato upload}
    C -- Sí --> D[Construir path s3a desde s3_key]
    D --> E{include_upload_errors?}
    E -- Sí --> F[Agregar errores/skipped heredados como filas sin path]
    E -- No --> G[Continuar solo pendientes]
    C -- No --> H[Modo standalone leer binaryFile]
    H --> G
    F --> G
    G --> I[Validar bucket_curated y checkpoint_prefix]
    I --> J[Intentar leer checkpoint parquet de procesados]
    J --> K{Checkpoint disponible?}
    K -- Sí --> L[left_anti join para excluir paths ya procesados]
    K -- No --> M[Usar pendientes distinct]
    L --> N{Hay errores heredados}
    M --> N
    N -- Sí --> O[unionByName con upload_errors]
    N -- No --> P[Continuar]
    O --> Q[Return pending]
    P --> Q
```

## 3) `pys_read_normalize.py`

```mermaid
flowchart TD
    A[Inicio pyspark_transform] --> B[Parsear reader_options dict/JSON]
    B --> C{Contrato de entrada}
    C -- path --> D[Tomar path/status/error/source_file]
    C -- s3_key+status --> E[Resolver bucket_raw y construir path s3a]
    C -- inválido --> Z1[ValueError]
    D --> F[Construir lista files PENDING e inherited_errors]
    E --> F
    F --> G[Definir helpers: get_ext, dataset_name, read_dynamic]
    G --> H[Inicializar datasets y error_records]
    H --> I[Mapear inherited_errors a ERROR_UPSTREAM]
    I --> J[Iterar archivos PENDING]
    J --> K[read_dynamic según extensión y reader_options]
    K --> L{Lectura exitosa?}
    L -- No --> M[Agregar ERROR_READ]
    L -- Sí --> N[Agregar metadatos ingestion/batch/dataset/status]
    N --> O[Normalizar esquema a string]
    O --> P[Union por dataset lógico]
    M --> J
    P --> J
    J --> Q{Hay datasets o errores?}
    Q -- No --> R[Return DF vacío con schema técnico]
    Q -- Sí --> S[Unir datasets en final_df]
    S --> T{Hay error_records?}
    T -- Sí --> U[Crear error_df + error_stage READ_NORMALIZE y union]
    T -- No --> V[Continuar]
    U --> W[Return final_df]
    V --> W
```

## 4) `pys_ingestion_summary.py`

```mermaid
flowchart TD
    A[Inicio pyspark_transform] --> B[Resolver flags include_global_summary y checksum espejo]
    B --> C[Normalizar contrato de entrada: dataset/batch/source_file/status/error_message]
    C --> D[Definir business_cols excluyendo columnas técnicas]
    D --> E[Calcular row_checksum por fila]
    E --> F[GroupBy dataset,batch_id y métricas de calidad/volumen]
    F --> G{use_source_checksum_as_destination?}
    G -- Sí --> H[checksum_destination = checksum_source]
    G -- No --> I[checksum_destination = null]
    H --> J[Calcular estado_ingesta COMPLETADO/COMPLETADO_CON_ERRORES]
    I --> J
    J --> K{include_global_summary?}
    K -- Sí --> L[Generar fila GLOBAL y unionByName]
    K -- No --> M[Conservar resumen granular]
    L --> N[Return summary]
    M --> N
```
