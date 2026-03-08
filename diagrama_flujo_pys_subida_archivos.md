# Diagrama de flujo — `pys_subida_archivos.py`

Este diagrama separa el proceso en **subprocesos paralelos (swimlanes)** conectados entre sí: validación, conexión SFTP, carga S3, checkpoint y salida.

```mermaid
flowchart LR

%% =====================
%% SWIMLANES
%% =====================
subgraph SP0[Subproceso 0: Inicialización y utilidades]
direction TB
A0[Inicio pyspark_transform]
A1[Crear process_log y helpers\nappend_log/build_status_df]
A2[Definir normalizadores\nget_int/get_bool/get_row/sanitize]
A3[Definir build_expected_filenames]
end

subgraph SP1[Subproceso 1: Validación de entrada y parámetros]
direction TB
B1[Validar columnas requeridas\nSFTP_HOST, SFTP_VAULT_NAME, NOMBRE_ARCHIVO, SFTP_PATH, BUCKET_BLOB]
B2{¿Faltan columnas?}
B3[Leer primera fila param_row]
B4{¿param_row es None?}
B5[Extraer parámetros de fila\nhost/port/vault/nombre/fechas/path/bucket]
B6{¿Falta algún requerido?}
B7[Leer credenciales spark db vault user pass]
B8{¿Credenciales válidas?}
B9[Leer rutas param_dict\nrelative_upload_file_path/control_path]
B10{¿Rutas/bucket válidos?}
B11[Construir expected filenames con fecha desde y fecha hasta]
B12{¿Fechas válidas?}
end

subgraph SP2[Subproceso 2: Preparación de conectores y políticas]
direction TB
C1[Crear cliente S3 con credenciales s3a]
C2{¿S3 inicializado?}
C3[Configurar TransferConfig multipart concurrencia hilos]
C4[Definir rutas\nCHECKPOINT, QUARANTINE, RAW_PREFIX, UNCOMP_PREFIX]
C5[Definir helpers de ejecución\nfile_hash y list_files_recursive]
end

subgraph SP3[Subproceso 3: Carga de estado previo (checkpoint)]
direction TB
D1[Intentar leer parquet CHECKPOINT]
D2[Normalizar columnas faltantes\nremote_size/remote_mtime]
D3[Reconstruir sets:\nprocessed_hashes y processed_signatures]
D4[Si falla lectura: continuar con sets vacíos]
end

subgraph SP4[Subproceso 4: Ingesta SFTP -> S3 por archivo]
direction TB
E1[Abrir conexión SFTP]
E2[Listar archivos recursivamente desde SFTP_PATH]
E3[Iterar cada remoto]
E4[Calcular rel_path, remote_filename, tmp_file]
E5{remote filename en expected filenames}
E6[Leer stat inicial\nsize1/mtime1 + signature]
E7{¿signature ya procesada?}
E8[Readiness check:\nesperar y comparar size1 vs size2]
E9{¿Archivo listo y no vacío?}
E10[Descargar archivo a tmp_file]
E11[Calcular hash SHA-256]
E12{¿hash ya procesado?}
E13[Subir crudo a RAW_PREFIX]
E14[Detectar gzip real por contenido]
E15{¿Es gzip?}
E16[Descomprimir temporal y subir a UNCOMP_PREFIX]
E17[Registrar checkpoint_records\n+ resultados PROCESADO + uploaded_paths]
E18[Eliminar temporales]
E19[On Error: subir a QUARANTINE\n+ resultados ERROR_CARGA]
end

subgraph SP5[Subproceso 5: Persistencia final y salida]
direction TB
F1{¿Hay checkpoint_records?}
F2[Escribir checkpoint en parquet append\ncon ingestion_ts]
F3[Calcular resumen:\nPROCESADO / ERROR / OMITIDO]
F4{¿total_error > 0?}
F5[Retornar estado FALLIDO\ncon cantidad de errores]
F6[Deduplicar uploaded_paths]
F7{¿Hay archivos cargados?}
F8[Retornar DataFrame(path) con rutas s3a]
F9{¿Hubo omitidos?}
F10[Retornar FALLIDO por omitidos readiness checkpoint filtro]
F11[Retornar FALLIDO no hubo archivos validos]
end

%% =====================
%% CROSS-LANE CONNECTIONS
%% =====================
A0 --> A1 --> A2 --> A3 --> B1

B1 --> B2
B2 -- Sí --> X1[FALLIDO: columnas faltantes]
B2 -- No --> B3 --> B4
B4 -- Sí --> X2[FALLIDO: sin parametría]
B4 -- No --> B5 --> B6
B6 -- Sí --> X3[FALLIDO: parámetro requerido ausente]
B6 -- No --> B7 --> B8
B8 -- No --> X4[FALLIDO: credenciales faltantes]
B8 -- Sí --> B9 --> B10
B10 -- No --> X5[FALLIDO: bucket/rutas faltantes]
B10 -- Sí --> B11 --> B12
B12 -- No --> X6[FALLIDO: ventana de fechas inválida]
B12 -- Sí --> C1

C1 --> C2
C2 -- No --> X7[FALLIDO: no inicializa S3]
C2 -- Sí --> C3 --> C4 --> C5 --> D1

D1 --> D2 --> D3 --> E1
D1 -. error lectura .-> D4 --> E1

E1 --> E2 --> E3 --> E4 --> E5
E5 -- No --> E3
E5 -- Sí --> E6 --> E7
E7 -- Sí --> E3
E7 -- No --> E8 --> E9
E9 -- No --> E3
E9 -- Sí --> E10 --> E11 --> E12
E12 -- Sí --> E18 --> E3
E12 -- No --> E13 --> E14 --> E15
E15 -- Sí --> E16 --> E17 --> E18 --> E3
E15 -- No --> E17 --> E18 --> E3
E6 -. excepción .-> E19 --> E3
E10 -. excepción .-> E19
E13 -. excepción .-> E19
E16 -. excepción .-> E19

E3 -->|fin iteración| F1
F1 -- Sí --> F2 --> F3
F1 -- No --> F3
F3 --> F4
F4 -- Sí --> F5
F4 -- No --> F6 --> F7
F7 -- Sí --> F8
F7 -- No --> F9
F9 -- Sí --> F10
F9 -- No --> F11
```

## Lectura rápida por subproceso

- **SP0**: Inicializa logging y funciones utilitarias para validación/conversión.
- **SP1**: Hace guardas tempranas de esquema, fila de parametría, secretos y rutas.
- **SP2**: Prepara clientes/conectores (S3) y políticas de carga (multipart/readiness).
- **SP3**: Carga checkpoint histórico para idempotencia por hash y metadata.
- **SP4**: Ejecuta el ciclo principal por archivo (filtro, readiness, descarga, upload, descompresión, cuarentena).
- **SP5**: Persiste checkpoint de lo nuevo y decide el retorno final (paths o estado FALLIDO).
