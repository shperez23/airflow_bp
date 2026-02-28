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
        files_df = df.select("path")
    elif {"s3_key", "status"}.issubset(input_cols):
        bucket_raw = param_dict.get("bucket_raw") or param_dict.get("bucket")
        if is_missing(bucket_raw):
            raise ValueError("Falta parámetro requerido 'bucket_raw' (o 'bucket') para resolver s3_key")
        files_df = (
            df
            .where((df.status == "PROCESADO") & (df.s3_key.isNotNull()) & (df.s3_key != ""))
            .selectExpr(f"concat('s3a://{bucket_raw}/', s3_key) as path")
        )
    else:
        raise ValueError("El input de pys_read_normalize debe incluir columna 'path' o contrato con 's3_key'/'status'")

    files = [r.path for r in files_df.distinct().collect() if not is_missing(r.path)]

    # =====================================
    # Dynamic Reader Pattern (helper)
    # Selecciona dinámicamente el lector según la extensión del archivo
    # =====================================
    def get_ext(path):
        p = path.lower()

        # Compression Resolver Pattern
        # Soporta archivos comprimidos (.gz, .gzip, .bz2, .zip) conservando tipo lógico
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
            return None

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
            reader = spark.read.format("com.crealytics.spark.excel")

            # =====================================
            # Excel Options Compatibility Pattern
            # Tolera variantes de opciones (useHeader/header) para evitar fallos de configuración
            # =====================================
            excel_opts = dict(opts)
            if "header" not in excel_opts and "useHeader" in excel_opts:
                excel_opts["header"] = excel_opts["useHeader"]

            # Header es obligatorio en algunas versiones del reader de Excel
            if "header" not in excel_opts:
                excel_opts["header"] = "true"

            for k, v in excel_opts.items():
                reader = reader.option(k, v)
            return reader.load(path)

        return None

    # =====================================
    # Dataset Union Pattern
    # Une archivos del mismo dataset lógico en un DF por dataset
    # =====================================
    datasets = {}

    for file in files:

        dset = dataset_name(file)

        df_read = read_dynamic(file)
        if df_read is None:
            continue

        # =====================================
        # Metadata Decorator Pattern
        # Agrega columnas de trazabilidad (lineage + auditoría)
        # =====================================
        batch_id = str(uuid.uuid4())
        df_read = (
            df_read
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn("source_file", lit(file))
            .withColumn("path", lit(file))
            .withColumn("batch_id", lit(batch_id))
            .withColumn("dataset", lit(dset))
        )

        # =====================================
        # Schema Normalizer Pattern
        # Convierte todos los campos del contenido a string para estandarizar
        # =====================================
        df_read = cast_all_to_string(df_read)

        if dset not in datasets:
            datasets[dset] = df_read
        else:
            datasets[dset] = datasets[dset].unionByName(df_read, allowMissingColumns=True)

    # =====================================
    # Empty Result Guard Pattern
    # Retorna un DataFrame vacío tipado cuando no hay archivos legibles
    # =====================================
    if not datasets:
        empty_schema = StructType([
            StructField("ingestion_ts", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("path", StringType(), True),
            StructField("batch_id", StringType(), True),
            StructField("dataset", StringType(), True),
        ])
        return spark.createDataFrame([], empty_schema)

    # =====================================
    # Process Result Dataset Pattern
    # Retorna un único DF unificado para nodos downstream (snapshot/delta/checkpoint)
    # =====================================
    final_df = None
    for d in datasets.values():
        final_df = d if final_df is None else final_df.unionByName(d, allowMissingColumns=True)

    return final_df
