# Diagrama de flujo — `pys_descubrimiento_archivos.py`

```mermaid
flowchart LR

%% =====================================================
%% BLOQUE 1: INICIALIZACIÓN Y RESOLUCIÓN DE ENTRADAS
%% =====================================================
subgraph B1[Subproceso 1: Inicialización y resolución de entradas]
  direction TB
  B1_I([INICIO_BLOQUE_1])
  B1_A[Inicializa process_log, append_log y build_status_df]
  B1_B[Registra inicio del proceso]
  B1_C[Resuelve upload_df y tri_parametros_discovery]
  B1_D[Extrae BUCKET_BLOB y BUCKET_RAW]
  B1_E[Detecta contrato de entrada: path / estado-error-log]
  B1_F([FIN_BLOQUE_1])

  B1_I --> B1_A --> B1_B --> B1_C --> B1_D --> B1_E --> B1_F
end

%% =====================================================
%% BLOQUE 2: CONSTRUCCIÓN DEL DATASET PENDIENTE
%% =====================================================
subgraph B2[Subproceso 2: Construcción de candidatos pendientes]
  direction TB
  B2_I([INICIO_BLOQUE_2])
  B2_A{¿Contrato estandarizado?}
  B2_B{¿estado == FALLIDO?}
  B2_C[Retorna estado/error/log recibido]
  B2_D[Retorna FALLIDO por contrato sin paths]
  B2_E{¿Contrato con columna path?}
  B2_F[Construye pending desde upload_df(path)]
  B2_G[Modo standalone:
valida BUCKET_BLOB y relative_upload_file_path]
  B2_H[Lee s3a://BUCKET_BLOB/.../original/
y construye pending]
  B2_FI([FIN_BLOQUE_2])

  B2_I --> B2_A
  B2_A -->|Sí| B2_B
  B2_B -->|Sí| B2_C
  B2_B -->|No| B2_D
  B2_A -->|No| B2_E
  B2_E -->|Sí| B2_F --> B2_FI
  B2_E -->|No| B2_G --> B2_H --> B2_FI
end

%% =====================================================
%% BLOQUE 3: CHECKPOINT E INCREMENTALIDAD
%% =====================================================
subgraph B3[Subproceso 3: Aplicación de checkpoint]
  direction TB
  B3_I([INICIO_BLOQUE_3])
  B3_A[Valida BUCKET_RAW y checkpoint_prefix]
  B3_B[Define checkpoint_path]
  B3_C{¿Lectura parquet checkpoint OK?}
  B3_D[Adapta contrato checkpoint:
path/source_file -> path]
  B3_E[Excluye procesados con left_anti join]
  B3_F[Continúa sin exclusión incremental]
  B3_FI([FIN_BLOQUE_3])

  B3_I --> B3_A --> B3_B --> B3_C
  B3_C -->|Sí| B3_D --> B3_E --> B3_FI
  B3_C -->|No (Exception)| B3_F --> B3_FI
end

%% =====================================================
%% BLOQUE 4: CIERRE Y SALIDA
%% =====================================================
subgraph B4[Subproceso 4: Cierre y retorno]
  direction TB
  B4_I([INICIO_BLOQUE_4])
  B4_A[Cuenta total_pending (paths válidos)]
  B4_B[Registra logs de pendientes y finalización]
  B4_C([RETORNA pending])
  B4_F([FIN_BLOQUE_4])

  B4_I --> B4_A --> B4_B --> B4_C --> B4_F
end

%% =====================================================
%% CONECTORES ENTRE BLOQUES
%% =====================================================
B1_F --> B2_I
B2_FI --> B3_I
B3_FI --> B4_I
```
