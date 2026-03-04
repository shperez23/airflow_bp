from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, StringType
import uuid
import json
import io
import boto3
import zipfile


def pyspark_transform(spark, df, param_dict):


    # =====================================
    # Parameter Guard Pattern
    # Normaliza parámetros opcionales para evitar errores por null/vacío
    # =====================================
    def is_missing(value):
        return value is None or (isinstance(value, str) and value.strip() == "")

    def sanitize_error_message(exc):
        message = str(exc).strip()
        return message if message else exc.__class__.__name__

    def build_s3_client_from_runtime():
        access_key = spark.sparkContext.getConf().get("spark.hadoop.fs.s3a.access.key", None)
        secret_key = spark.sparkContext.getConf().get("spark.hadoop.fs.s3a.secret.key", None)
        session_token = spark.sparkContext.getConf().get("spark.hadoop.fs.s3a.session.token", None)

        if access_key and secret_key:
            return boto3.client(
                "s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
            )

        return boto3.client("s3")

    # =====================================
    # Multi-Input Resolver Pattern
    # Resuelve entradas cuando el orquestador envía múltiples dataframes
    # =====================================
    def resolve_input_frame(flow_inputs, preferred_keys):
        if not hasattr(flow_inputs, "get"):
            return flow_inputs

        for key in preferred_keys:
            candidate = flow_inputs.get(key)
            if candidate is not None:
                return candidate

        try:
            values = list(flow_inputs.values())
        except Exception:
            return None

        for candidate in values:
            if candidate is not None and hasattr(candidate, "columns"):
                return candidate

        return None

    input_df = resolve_input_frame(df, ["pys_descubrimiento_archivos"])
    param_read_row = resolve_input_frame(df, ["tri_parametros_read", "Tri_parametros_read"])
    def get_param_read_value(param_source, field_name):
        if param_source is None:
            return None

        if hasattr(param_source, "first"):
            param_source = param_source.first()

        try:
            return param_source[field_name]
        except Exception:
            return None

    bucket_name = get_param_read_value(param_read_row, "BUCKET_BLOB")

    # =====================================
    # External Trigger Pattern
    # Resuelve parámetros de ejecución enviados por el orquestador (Rocket/Airflow)
    # =====================================
    raw_reader = get_param_read_value(param_read_row, "READER_OPTIONS")

    if isinstance(raw_reader, str):
        raw_reader = raw_reader.strip()
        if raw_reader == "":
            reader_options = {}
        else:
            try:
                reader_options = json.loads(raw_reader)
            except json.JSONDecodeError as exc:
                raise ValueError("Parámetro 'reader_options' no es un JSON válido") from exc
    elif isinstance(raw_reader, dict):
        reader_options = raw_reader
    else:
        raise ValueError("Parámetro 'READER_OPTIONS' debe ser dict o JSON string en tri_parametros_read")

    # =====================================
    # Input Contract Resolver Pattern
    # Acepta distintos contratos de entrada para mantener compatibilidad
    # =====================================
    if input_df is None or not hasattr(input_df, "columns"):
        raise ValueError(
            "No se encontró DataFrame de entrada para pys_read_normalize. "
            "Verifique la salida de discovery y los nombres de nodos de entrada."
        )

    input_cols = set(input_df.columns)
    if "path" in input_cols:
        files_df = input_df.selectExpr(
            "path",
            "coalesce(discovery_status, 'PENDIENTE') as status",
            "error_message",
            "coalesce(source_file, path) as source_file"
        )
    else:
        raise ValueError("El input de pys_read_normalize debe incluir la columna 'path'")

    file_rows = files_df.distinct().collect()
    files = [r.path for r in file_rows if not is_missing(r.path) and (r.status == "PENDIENTE")]
    inherited_errors = [
        r for r in file_rows
        if (str(r.status).upper().startswith("ERROR")) and (r.status != "PENDIENTE")
    ]

    # =====================================
    # POI Runtime Guard Pattern
    # Permite subir límites internos de Apache POI cuando el Excel lo requiere
    # =====================================
    def apply_poi_runtime_overrides(max_byte_array_size):
        if is_missing(max_byte_array_size):
            return

        try:
            value = int(max_byte_array_size)
        except (TypeError, ValueError):
            return

        if value <= 0:
            return

        try:
            spark._jvm.java.lang.System.setProperty("poi.io.maxByteArraySize", str(value))
            spark._jvm.org.apache.poi.util.IOUtils.setByteArrayMaxOverride(value)
        except Exception:
            pass

    # =====================================
    # Dynamic Reader Pattern (helper)
    # Selecciona dinámicamente el lector según la extensión del archivo
    # =====================================
    def compression_suffix(path):
        lower_path = str(path).lower()
        for suffix in (".gzip", ".gz", ".bz2", ".zip"):
            if lower_path.endswith(suffix):
                return suffix
        return None

    def get_ext(path):
        p = str(path).lower()

        suffix = compression_suffix(p)
        if suffix is not None:
            p = p[: -len(suffix)]

        if p.endswith((".xlsx", ".xls")):
            return "excel"
        if p.endswith(".csv"):
            return "csv"
        if p.endswith(".txt"):
            return "txt"
        if p.endswith(".json"):
            return "json"
        if p.endswith(".parquet"):
            return "parquet"
        return "unknown"

    # =====================================
    # Dataset Resolver Pattern (helper)
    # Agrupa por dataset lógico para controlar unions y escrituras downstream
    # =====================================
    def dataset_name(path):
        filename = path.split("/")[-1]
        stem = filename.split(".")[0]
        return stem.split("_")[0] if "_" in stem else stem

    def resolve_zip_staging_target(zip_path):
        relative_upload_file_path = param_dict.get("relative_upload_file_path")
        target_bucket = bucket_name

        if is_missing(target_bucket):
            raise ValueError("Falta parámetro requerido 'BUCKET_BLOB' en tri_parametros_read")

        if is_missing(relative_upload_file_path):
            raise ValueError("Falta parámetro requerido 'relative_upload_file_path'")

        raw_prefix = f"{relative_upload_file_path}/original/descomprimido"
        normalized_prefix = str(raw_prefix).strip().strip("/")
        if normalized_prefix == "":
            normalized_prefix = f"{relative_upload_file_path}/original/descomprimido"

        run_id = str(uuid.uuid4())
        return target_bucket, f"{normalized_prefix}/{run_id}"

    # =====================================
    # Compressed Archive Expansion Pattern (helper)
    # Expande ZIPs en archivos temporales para procesar múltiples entradas internas
    # =====================================
    def expand_input_paths(path):
        suffix = compression_suffix(path)

        if suffix in {".gz", ".gzip", ".bz2"}:
            if get_ext(path) == "unknown":
                raise ValueError(f"No hay reader_options definidos para archivo comprimido: {path}")
            return [{"read_path": path, "trace_path": path, "source_file": path}]

        if suffix != ".zip":
            return [{"read_path": path, "trace_path": path, "source_file": path}]

        binary_rows = (
            spark.read
            .format("binaryFile")
            .load(path)
            .select("content")
            .take(1)
        )

        if not binary_rows:
            raise ValueError(f"No se pudo leer el contenido binario del ZIP: {path}")

        staging_bucket, staging_prefix = resolve_zip_staging_target(path)
        s3_client = build_s3_client_from_runtime()

        expanded_files = []
        with zipfile.ZipFile(io.BytesIO(binary_rows[0]["content"])) as zip_ref:
            for member in zip_ref.infolist():
                if member.is_dir():
                    continue

                member_name = member.filename
                member_ext = get_ext(member_name)
                if member_ext == "unknown":
                    continue

                member_basename = member_name.split("/")[-1]
                staged_key = f"{staging_prefix}/{uuid.uuid4()}_{member_basename}"

                with zip_ref.open(member) as source_member:
                    s3_client.put_object(Bucket=staging_bucket, Key=staged_key, Body=source_member.read())

                trace_path = f"{path}::{member_name}"
                expanded_files.append(
                    {
                        "read_path": f"s3a://{staging_bucket}/{staged_key}",
                        "trace_path": trace_path,
                        # Mantiene trazabilidad del archivo contenedor (ZIP) en source_file.
                        # El path conserva el archivo interno procesado (zip::member).
                        "source_file": path,
                    }
                )

        if not expanded_files:
            raise ValueError(f"El ZIP no contiene archivos legibles por pys_read_normalize: {path}")

        return expanded_files

    # =====================================
    # Schema Normalizer Pattern (helper)
    # Homogeniza el esquema: convierte todos los campos a string
    # =====================================
    def cast_all_to_string(df_in):
        return df_in.select([col(c).cast("string").alias(c) for c in df_in.columns])

    # =====================================
    # Reader Options Injection Pattern (helper)
    # Aplica opciones de lectura por tipo (csv/txt/json/parquet/excel) desde param_dict
    # =====================================
    def read_dynamic(path):

        ext = get_ext(path)

        if ext not in reader_options:
            raise ValueError(f"No hay reader_options definidos para extensión '{ext}'")

        opts = reader_options.get(ext, {}) or {}

        if ext == "csv":
            reader = spark.read
            for k, v in opts.items():
                reader = reader.option(k, v)
            return reader.csv(path)

        if ext == "txt":
            delimiter = opts.get("delimiter")
            reader = spark.read
            for k, v in opts.items():
                if k != "delimiter":
                    reader = reader.option(k, v)

            if delimiter:
                return reader.option("delimiter", delimiter).csv(path)
            return reader.text(path)

        if ext == "json":
            reader = spark.read
            for k, v in opts.items():
                reader = reader.option(k, v)
            return reader.json(path)

        if ext == "parquet":
            return spark.read.parquet(path)

        if ext == "excel":
            excel_opts = {}
            for k, v in dict(opts).items():
                key = str(k)
                normalized = key.strip().lower()

                if normalized in {"useheader", "header"}:
                    excel_opts["header"] = v
                elif normalized in {"inferschema", "infer_schema"}:
                    excel_opts["inferSchema"] = v
                elif normalized in {"sheetname", "sheet_name"}:
                    excel_opts["sheetName"] = v
                else:
                    excel_opts[key] = v

            if "header" not in excel_opts:
                excel_opts["header"] = "true"
            if "inferSchema" not in excel_opts:
                excel_opts["inferSchema"] = "false"
            if "maxRowsInMemory" not in excel_opts:
                excel_opts["maxRowsInMemory"] = "1000"
            if "excerptSize" not in excel_opts:
                excel_opts["excerptSize"] = "10"

            poi_max_from_excel_opts = excel_opts.pop("poi_max_byte_array_size", None)
            poi_max_from_param = param_dict.get("poi_max_byte_array_size")
            apply_poi_runtime_overrides(poi_max_from_param or poi_max_from_excel_opts)

            def excel_reader_with(options_dict):
                reader = spark.read.format("com.crealytics.spark.excel")
                for k, v in options_dict.items():
                    reader = reader.option(k, v)
                return reader

            try:
                return excel_reader_with(excel_opts).load(path)
            except Exception as exc:
                excel_opts_retry = dict(excel_opts)
                excel_opts_retry["inferSchema"] = "false"
                try:
                    return excel_reader_with(excel_opts_retry).load(path)
                except Exception:
                    raise exc

        raise ValueError(f"Extensión no soportada para lectura dinámica: '{ext}'")

    # =====================================
    # Dataset Union Pattern
    # Une archivos del mismo dataset lógico en un DF por dataset
    # =====================================
    datasets = {}
    error_records = []

    for inherited in inherited_errors:
        src_file = inherited.source_file if not is_missing(inherited.source_file) else None
        inferred_dataset = dataset_name(src_file) if src_file else None
        error_records.append((
            src_file,
            "ERROR_UPSTREAM",
            inherited.error_message or inherited.status or "Error heredado desde upstream",
            None,
            src_file,
            None,
            inferred_dataset,
        ))

    for file in files:

        try:
            read_targets = expand_input_paths(file)
        except Exception as exc:
            dset = dataset_name(file)
            error_records.append((
                file,
                "ERROR_LECTURA",
                sanitize_error_message(exc),
                None,
                file,
                None,
                dset,
            ))
            continue

        for target in read_targets:
            dset = dataset_name(target["source_file"])

            try:
                df_read = read_dynamic(target["read_path"])
            except Exception as exc:
                error_records.append((
                    target["trace_path"],
                    "ERROR_LECTURA",
                    sanitize_error_message(exc),
                    None,
                    target["source_file"],
                    None,
                    dset,
                ))
                continue

            batch_id = str(uuid.uuid4())
            df_read = (
                df_read
                .withColumn("ingestion_ts", current_timestamp())
                .withColumn("source_file", lit(target["source_file"]))
                .withColumn("path", lit(target["trace_path"]))
                .withColumn("batch_id", lit(batch_id))
                .withColumn("dataset", lit(dset))
                .withColumn("record_status", lit("PROCESADO"))
                .withColumn("error_stage", lit(None).cast("string"))
                .withColumn("error_message", lit(None).cast("string"))
            )

            df_read = cast_all_to_string(df_read)

            if dset not in datasets:
                datasets[dset] = df_read
            else:
                datasets[dset] = datasets[dset].unionByName(df_read, allowMissingColumns=True)
    if not datasets and not error_records:
        empty_schema = StructType([
            StructField("ingestion_ts", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("path", StringType(), True),
            StructField("batch_id", StringType(), True),
            StructField("dataset", StringType(), True),
            StructField("record_status", StringType(), True),
            StructField("error_stage", StringType(), True),
            StructField("error_message", StringType(), True),
        ])
        return spark.createDataFrame([], empty_schema)

    final_df = None
    for d in datasets.values():
        final_df = d if final_df is None else final_df.unionByName(d, allowMissingColumns=True)

    if error_records:
        error_schema = StructType([
            StructField("path", StringType(), True),
            StructField("record_status", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("ingestion_ts", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("batch_id", StringType(), True),
            StructField("dataset", StringType(), True),
        ])

        error_df = spark.createDataFrame(error_records, error_schema)
        error_df = (
            error_df
            .withColumn("error_stage", lit("LECTURA_NORMALIZACION"))
            .withColumn("source_file", col("source_file").cast("string"))
        )

        if final_df is None:
            final_df = error_df
        else:
            final_df = final_df.unionByName(error_df, allowMissingColumns=True)

    return final_df
