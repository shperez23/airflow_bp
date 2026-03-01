from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, StringType
import uuid
import json


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

    # =====================================
    # External Trigger Pattern
    # Resuelve parámetros de ejecución enviados por el orquestador (Rocket/Airflow)
    # =====================================
    raw_reader = param_dict.get("reader_options", {})

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
        raise ValueError("Parámetro 'reader_options' debe ser dict o JSON string")

    # =====================================
    # Input Contract Resolver Pattern
    # Acepta distintos contratos de entrada para mantener compatibilidad
    # =====================================
    input_cols = set(df.columns)
    if "path" in input_cols:
        files_df = df.selectExpr(
            "path",
            "coalesce(discovery_status, 'PENDING') as status",
            "error_message",
            "coalesce(source_file, path) as source_file"
        )
    elif {"s3_key", "status"}.issubset(input_cols):
        bucket_raw = param_dict.get("bucket_raw") or param_dict.get("bucket")
        if is_missing(bucket_raw):
            raise ValueError("Falta parámetro requerido 'bucket_raw' (o 'bucket') para resolver s3_key")
        files_df = (
            df
            .where((df.status == "PROCESADO") & (df.s3_key.isNotNull()) & (df.s3_key != ""))
            .selectExpr(
                f"concat('s3a://{bucket_raw}/', s3_key) as path",
                "'PENDING' as status",
                "cast(null as string) as error_message",
                f"concat('s3a://{bucket_raw}/', s3_key) as source_file"
            )
        )
    else:
        raise ValueError("El input de pys_read_normalize debe incluir columna 'path' o contrato con 's3_key'/'status'")

    file_rows = files_df.distinct().collect()
    files = [r.path for r in file_rows if not is_missing(r.path) and (r.status == "PENDING")]
    inherited_errors = [
        r for r in file_rows
        if (str(r.status).upper().startswith("ERROR")) and (r.status != "PENDING")
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
    def get_ext(path):
        p = path.lower()

        compression_suffixes = (".gz", ".gzip", ".bz2", ".zip")
        for suffix in compression_suffixes:
            if p.endswith(suffix):
                p = p[: -len(suffix)]
                break

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

        dset = dataset_name(file)

        try:
            df_read = read_dynamic(file)
        except Exception as exc:
            error_records.append((
                file,
                "ERROR_READ",
                sanitize_error_message(exc),
                None,
                file,
                None,
                dset,
            ))
            continue

        batch_id = str(uuid.uuid4())
        df_read = (
            df_read
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn("source_file", lit(file))
            .withColumn("path", lit(file))
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
            .withColumn("error_stage", lit("READ_NORMALIZE"))
            .withColumn("source_file", col("source_file").cast("string"))
        )

        if final_df is None:
            final_df = error_df
        else:
            final_df = final_df.unionByName(error_df, allowMissingColumns=True)

    return final_df
