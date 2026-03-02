from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit
import pysftp
import boto3
import uuid
import os
import time
import gzip
import shutil
import hashlib
from datetime import datetime, timedelta
from stat import S_ISDIR
from boto3.s3.transfer import TransferConfig

def pyspark_transform(spark, df, param_dict):

    # =====================================
    # Parameter Guard Pattern
    # Normaliza parámetros opcionales para evitar errores por None/vacío
    # =====================================
    def get_int_param(name, default):
        raw_value = param_dict.get(name, default)
        if raw_value is None or raw_value == "":
            return default
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return default

    def get_int_value(raw_value, default):
        if raw_value is None or raw_value == "":
            return default
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return default

    def get_bool_param(name, default):
        raw_value = param_dict.get(name, default)
        if isinstance(raw_value, bool):
            return raw_value
        if raw_value is None or raw_value == "":
            return default

        normalized = str(raw_value).strip().lower()
        if normalized in {"true", "1", "yes", "y", "si", "sí"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
        return default

    def get_row_value(row, column_name, default=None):
        if row is None:
            return default

        value = row[column_name]
        if value is None:
            return default
        if isinstance(value, str) and value.strip() == "":
            return default
        return value


    def sanitize_error_message(exc):
        message = str(exc).strip()
        return message if message else exc.__class__.__name__

    def build_expected_filenames(nombre_archivo, fecha_desde, fecha_hasta):
        default_date = "YYYY-MM-DD"

        if fecha_desde == default_date and fecha_hasta == default_date:
            return {nombre_archivo}

        if fecha_desde == default_date or fecha_hasta == default_date:
            raise ValueError("'fecha_desde' y 'fecha_hasta' deben venir ambas con fecha real o ambas con valor por defecto 'YYYY-MM-DD'")

        try:
            start_date = datetime.strptime(fecha_desde, "%Y-%m-%d").date()
            end_date = datetime.strptime(fecha_hasta, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("Formato inválido para fechas. Use 'YYYY-MM-DD'") from exc

        if start_date > end_date:
            raise ValueError("'fecha_desde' no puede ser mayor que 'fecha_hasta'")

        stem, extension = os.path.splitext(nombre_archivo)
        total_days = (end_date - start_date).days

        filenames = set()
        for day_offset in range(total_days + 1):
            current = start_date + timedelta(days=day_offset)
            filenames.add(f"{stem}_{current.strftime('%Y%m%d')}{extension}")

        return filenames

    # =====================================
    # External Trigger
    # Define los parámetros de ejecución enviados por el orquestador
    # =====================================
    param_row = df.first()

    if param_row is None:
        raise ValueError("El dataframe de entrada no contiene registros de parametría")

    sftp_host = get_row_value(param_row, "SFTP_HOST")
    sftp_port = get_int_value(get_row_value(param_row, "SFTP_PORT", 22), 22)
    sftp_vault_name = get_row_value(param_row, "SFTP_VAULT_NAME")
    nombre_archivo = get_row_value(param_row, "NOMBRE_ARCHIVO")
    fecha_desde = get_row_value(param_row, "FECHA_DESDE", "YYYY-MM-DD")
    fecha_hasta = get_row_value(param_row, "FECHA_HASTA", "YYYY-MM-DD")
    sftp_path = get_row_value(param_row, "SFTP_PATH")
    bucket_name = get_row_value(param_row, "BUCKET")

    if not sftp_host:
        raise ValueError("Falta parámetro requerido 'SFTP_HOST'")
    if not sftp_vault_name:
        raise ValueError("Falta parámetro requerido 'SFTP_VAULT_NAME'")
    if not nombre_archivo:
        raise ValueError("Falta parámetro requerido 'NOMBRE_ARCHIVO'")

    # =====================================
    # Secrets Pointer Pattern
    # Obtiene credenciales desde sftp_vault_name sin hardcodearlas
    # =====================================
    user = spark.conf.get(f"spark.db.{sftp_vault_name}.user", "")
    pwd = spark.conf.get(f"spark.db.{sftp_vault_name}.pass", "")

    if not user or not pwd:
        raise ValueError(f"Faltan credenciales para sftp_vault_name '{sftp_vault_name}'")

    if not sftp_path:
        raise ValueError("Falta parámetro requerido 'SFTP_PATH'")

    # =====================================
    # Dynamic Source Resolver Pattern
    # Permite cambiar el origen SFTP sin modificar el código
    # =====================================
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    # =====================================
    # Dataset Routing Pattern
    # Define dinámicamente el destino del dataset en el data lake
    # =====================================
    relative_upload_file_path = param_dict.get("relative_upload_file_path")
    relative_upload_control_path = param_dict.get("relative_upload_control_path")

    if not bucket_name:
        raise ValueError("Falta parámetro requerido 'bucket'")
    if not relative_upload_file_path:
        raise ValueError("Falta parámetro requerido 'relative_upload_file_path'")
    if not relative_upload_control_path:
        raise ValueError("Falta parámetro requerido 'relative_upload_control_path'")

    # =====================================
    # Naming Strategy Pattern
    # Construye los nombres esperados según ventana de fechas diaria
    # =====================================
    expected_filenames = build_expected_filenames(nombre_archivo, fecha_desde, fecha_hasta)

    secret_key = spark.sparkContext.getConf().get("spark.hadoop.fs.s3a.secret.key", None)
    access_key = spark.sparkContext.getConf().get("spark.hadoop.fs.s3a.access.key", None)
   
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    s3 = session.client("s3")

    # =====================================
    # Multipart / Concurrency Control Pattern
    # Parametriza performance del upload S3
    # =====================================

    concurrencia_maxima = get_int_param("max_concurrency", 5)
    usar_hilos = get_bool_param("use_threads", True)

    # =====================================
    # Readiness Policy Pattern
    # Parametriza espera para validación de archivos en copia activa
    # =====================================
    readiness_wait_seconds = get_int_param("readiness_wait_seconds", 2)
    readiness_skip_wait_age_seconds = get_int_param("readiness_skip_wait_age_seconds", 30)

    tamano_parte_multipart = get_int_param("multipart_chunksize_mb", 64) * 1024 * 1024
    umbral_multipart = get_int_param("multipart_threshold_mb", 64) * 1024 * 1024

    transfer_config = TransferConfig(
        multipart_threshold=umbral_multipart,
        multipart_chunksize=tamano_parte_multipart,
        max_concurrency=concurrencia_maxima,
        use_threads=usar_hilos
    )


    # =====================================
    # Checkpointer Pattern
    # Mantiene el estado de archivos procesados para evitar reprocesos
    # =====================================
    CHECKPOINT = f"s3a://{bucket_name}/{relative_upload_control_path}/checkpoints/files/"

    # =====================================
    # Namespace Isolation Pattern
    # Separa zonas del data lake para evitar contaminación de datos
    # =====================================
    QUARANTINE = f"{relative_upload_file_path}/quarantine/"
    RAW_PREFIX = f"{relative_upload_file_path}/original/"
    UNCOMP_PREFIX = f"{relative_upload_file_path}/uncompressed/"

    # =====================================
    # Idempotent Consumer Pattern
    # Genera hash para detectar archivos duplicados aunque cambien de nombre
    # =====================================
    def file_hash(path):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(1024 * 1024):
                h.update(chunk)
        return h.hexdigest()

    # =====================================
    # Dataset Discovery Pattern
    # Permite descubrir archivos en estructuras SFTP profundas
    # =====================================
    def list_files_recursive(sftp, path):
        files = []
        pending_dirs = [path]

        while pending_dirs:
            current_dir = pending_dirs.pop()
            try:
                for entry in sftp.listdir_attr(current_dir):
                    full = current_dir.rstrip("/") + "/" + entry.filename
                    if S_ISDIR(entry.st_mode):
                        pending_dirs.append(full)
                    else:
                        files.append(full)
            except Exception as exc:
                resultados.append((current_dir, "", "ERROR_DISCOVERY", "DISCOVERY", sanitize_error_message(exc)))
                continue

        return files

    # =====================================
    # Checkpointer + Idempotency Pattern
    # Recupera el historial de procesamiento para ingestión incremental
    # =====================================
    processed_hashes = set()
    processed_signatures = set()
    try:
        processed_df = spark.read.parquet(CHECKPOINT)
        checkpoint_cols = set(processed_df.columns)

        if "remote_size" not in checkpoint_cols:
            processed_df = processed_df.withColumn("remote_size", lit(None).cast("long"))
        if "remote_mtime" not in checkpoint_cols:
            processed_df = processed_df.withColumn("remote_mtime", lit(None).cast("long"))

        checkpoint_rows = processed_df.select("hash", "relative_path", "remote_size", "remote_mtime").collect()
        for row in checkpoint_rows:
            if row.hash:
                processed_hashes.add(row.hash)
            if row.relative_path is not None and row.remote_size is not None and row.remote_mtime is not None:
                processed_signatures.add((row.relative_path, int(row.remote_size), int(row.remote_mtime)))
    except Exception:
        pass

    resultados = []
    checkpoint_records = []

    # =====================================
    # External Connector Pattern (SFTP)
    # Maneja la conexión externa desacoplada del resto del pipeline
    # =====================================
    with pysftp.Connection(host=sftp_host, port=sftp_port, username=user, password=pwd, cnopts=cnopts) as sftp:

        archivos = list_files_recursive(sftp, sftp_path)

        for remoto in archivos:

            # =====================================
            # Path Preservation Pattern
            # Evita sobrescrituras y mantiene trazabilidad del origen
            # =====================================
            rel_path = remoto[len(sftp_path):].lstrip("/")
            remote_filename = os.path.basename(remoto)
            tmp_file = f"/tmp/{uuid.uuid4().hex}"

            # =====================================
            # Filename Filter Pattern
            # Procesa únicamente archivos esperados por estrategia de nombres
            # =====================================
            if remote_filename not in expected_filenames:
                continue

            try:

                # =====================================
                # Readiness Marker Pattern
                # Evita ingerir archivos mientras aún se están copiando
                # =====================================
                stat1 = sftp.stat(remoto)
                size1 = stat1.st_size
                mtime1 = int(stat1.st_mtime)

                # =====================================
                # Fast Metadata Checkpoint Pattern
                # Salta archivos ya procesados sin descargarlos nuevamente
                # =====================================
                signature = (rel_path, int(size1), int(mtime1))
                if signature in processed_signatures:
                    resultados.append((remoto, "", "SKIPPED_ALREADY_PROCESSED", "UPLOAD", "Archivo ya procesado por checkpoint de metadata"))
                    continue

                file_age_seconds = int(time.time()) - int(mtime1)
                if file_age_seconds < readiness_skip_wait_age_seconds:
                    time.sleep(readiness_wait_seconds)
                    size2 = sftp.stat(remoto).st_size
                else:
                    size2 = size1

                if size1 != size2 or size1 == 0:
                    resultados.append((remoto, "", "SKIPPED_NOT_READY", "READINESS", "Archivo en copia activa o vacío"))
                    continue

                # =====================================
                # Passthrough Replicator Pattern
                # Replica el archivo crudo para preservar la evidencia original
                # =====================================
                sftp.get(remoto, tmp_file)

                # =====================================
                # Idempotent Consumer Pattern
                # Evita reprocesar archivos ya ingeridos
                # =====================================
                hash_value = file_hash(tmp_file)

                if hash_value in processed_hashes:
                    os.remove(tmp_file)
                    resultados.append((remoto, "", "SKIPPED_ALREADY_PROCESSED", "UPLOAD", "Archivo ya procesado por hash"))
                    continue

                # =====================================
                # Raw Landing Zone Pattern
                # Guarda el archivo en la zona raw del data lake
                # =====================================
                s3_key = f"{RAW_PREFIX}{rel_path}"
                s3.upload_file(tmp_file, bucket_name, s3_key, Config=transfer_config)

                # =====================================
                # Compression Detection Pattern
                # Detecta compresión real y no solo extensión
                # =====================================
                is_gzip = False
                try:
                    with gzip.open(tmp_file, "rb") as test:
                        test.read(1)
                    is_gzip = True
                except:
                    pass

                # =====================================
                # Compression Normalizer Pattern
                # Genera versión descomprimida para consumo analítico
                # =====================================
                if is_gzip:
                    tmp_uncomp = f"/tmp/{uuid.uuid4().hex}"
                    with gzip.open(tmp_file, "rb") as f_in:
                        with open(tmp_uncomp, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)

                    s3.upload_file(tmp_uncomp, bucket_name, f"{UNCOMP_PREFIX}{rel_path.replace('.gz','')}", Config=transfer_config)
                    os.remove(tmp_uncomp)

                # =====================================
                # Metadata Decorator Pattern
                # Añade metadata de ingestión para lineage y auditoría
                # =====================================
                checkpoint_records.append((rel_path, hash_value, s3_key, int(size1), int(mtime1)))

                processed_hashes.add(hash_value)
                processed_signatures.add(signature)

                resultados.append((remoto, s3_key, "PROCESADO", "UPLOAD", ""))

                os.remove(tmp_file)

            except Exception as e:

                # =====================================
                # Dead-Letter Pattern
                # Aísla archivos corruptos o con errores para análisis posterior
                # =====================================
                if os.path.exists(tmp_file):
                    s3.upload_file(tmp_file, bucket_name, f"{QUARANTINE}{rel_path}", Config=transfer_config)
                    os.remove(tmp_file)

                resultados.append((remoto, "", "ERROR_UPLOAD", "UPLOAD", sanitize_error_message(e)))

    # =====================================
    # Batched Checkpointer Pattern
    # Persiste el estado en lote para reducir small-files y latencia
    # =====================================
    if checkpoint_records:
        meta = spark.createDataFrame(
            checkpoint_records,
            ["relative_path", "hash", "s3_key", "remote_size", "remote_mtime"]
        ).withColumn("ingestion_ts", current_timestamp())
        meta.write.mode("append").parquet(CHECKPOINT)

    # =====================================
    # Process Result Dataset Pattern
    # Genera dataset de auditoría del resultado de la ingestión
    # =====================================
    schema = StructType([
        StructField("full_path", StringType(), False),
        StructField("s3_key", StringType(), False),
        StructField("status", StringType(), False),
        StructField("error_stage", StringType(), False),
        StructField("error_message", StringType(), False),
    ])

    return spark.createDataFrame(resultados, schema)