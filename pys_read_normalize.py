from pyspark.sql.functions import current_timestamp, lit, col
import uuid
import json

def pyspark_transform(spark, df, param_dict):

    # =====================================
    # External Trigger Pattern
    # Resuelve parámetros de ejecución enviados por el orquestador (Rocket/Airflow)
    # =====================================
    raw_reader = param_dict.get("reader_options", {})

    if isinstance(raw_reader,str):
        reader_options = json.loads(raw_reader)
    else:
        reader_options = raw_reader

    # =====================================
    # Dynamic Reader Pattern (helper)
    # Selecciona dinámicamente el lector según la extensión del archivo
    # =====================================
    def get_ext(path):
        p = path.lower()
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
        # Dataset basado en nombre de archivo (prefijo antes de _)
        filename = path.split("/")[-1]
        return filename.split("_")[0] if "_" in filename else filename

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

        opts = reader_options.get(ext, {})

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
            else:
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
            for k, v in opts.items():
                reader = reader.option(k, v)
            return reader.load(path)

        return None


    # =====================================
    # Dataset Discovery Pattern
    # Consume el listado de archivos de entrada (df del nodo DISCOVERY)
    # =====================================
    files = [r.path for r in df.select("path").collect()]

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
        df_read = (
            df_read
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn("source_file", lit(file))
            .withColumn("batch_id", lit(str(uuid.uuid4())))
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
    # Process Result Dataset Pattern
    # Retorna un único DF unificado para nodos downstream (snapshot/delta/checkpoint)
    # =====================================
    final_df = None
    for d in datasets.values():
        final_df = d if final_df is None else final_df.unionByName(d, allowMissingColumns=True)

    return final_df