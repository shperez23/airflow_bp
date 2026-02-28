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
from stat import S_ISDIR
from boto3.s3.transfer import TransferConfig

def pyspark_transform(spark, df, param_dict):

    # =====================================
    # External Trigger
    # Define los parámetros de ejecución enviados por el orquestador
    # =====================================
    host = param_dict["sftp_host"]
    port = int(param_dict["sftp_port"])
    vault = param_dict["sftp_vault_name"]

    # =====================================
    # Secrets Pointer Pattern
    # Obtiene credenciales desde vault sin hardcodearlas
    # =====================================
    user = spark.conf.get(f"spark.db.{vault}.user", "")
    pwd = spark.conf.get(f"spark.db.{vault}.pass", "")

    if not user or not pwd:
        raise ValueError(f"Faltan credenciales para vault '{vault}'")

    # =====================================
    # Dynamic Source Resolver Pattern
    # Permite cambiar el origen SFTP sin modificar el código
    # =====================================
    sftp_root = df.select("pathSftp").first()[0]

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    # =====================================
    # Dataset Routing Pattern
    # Define dinámicamente el destino del dataset en el data lake
    # =====================================
    bucket_name = param_dict["bucket"]
    base_s3 = param_dict["base_s3"]
    base_control_s3 = param_dict["base_control_s3"]

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

    concurrencia_maxima = int(param_dict.get("max_concurrency", 5))
    usar_hilos = param_dict.get("use_threads", True)

    # =====================================
    # Readiness Policy Pattern
    # Parametriza espera para validación de archivos en copia activa
    # =====================================
    readiness_wait_seconds = int(param_dict.get("readiness_wait_seconds", 2))
    readiness_skip_wait_age_seconds = int(param_dict.get("readiness_skip_wait_age_seconds", 30))

    tamano_parte_multipart = int(param_dict.get("multipart_chunksize_mb", 64)) * 1024 * 1024
    umbral_multipart = int(param_dict.get("multipart_threshold_mb", 64)) * 1024 * 1024

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
    CHECKPOINT = f"s3a://{bucket_name}/{base_control_s3}/checkpoints/files/"

    # =====================================
    # Namespace Isolation Pattern
    # Separa zonas del data lake para evitar contaminación de datos
    # =====================================
    QUARANTINE = f"{base_s3}/quarantine/"
    RAW_PREFIX = f"{base_s3}/original/"
    UNCOMP_PREFIX = f"{base_s3}/uncompressed/"

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
            except Exception:
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
    with pysftp.Connection(host=host, port=port, username=user, password=pwd, cnopts=cnopts) as sftp:

        archivos = list_files_recursive(sftp, sftp_root)

        for remoto in archivos:

            # =====================================
            # Path Preservation Pattern
            # Evita sobrescrituras y mantiene trazabilidad del origen
            # =====================================
            rel_path = remoto[len(sftp_root):].lstrip("/")
            tmp_file = f"/tmp/{uuid.uuid4().hex}"

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
                    resultados.append((remoto, "", "SKIPPED_ALREADY_PROCESSED"))
                    continue

                file_age_seconds = int(time.time()) - int(mtime1)
                if file_age_seconds < readiness_skip_wait_age_seconds:
                    time.sleep(readiness_wait_seconds)
                    size2 = sftp.stat(remoto).st_size
                else:
                    size2 = size1

                if size1 != size2 or size1 == 0:
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

                processed_hashes.add(hash_value)
                processed_signatures.add(signature)

                resultados.append((remoto, s3_key, "PROCESADO"))

                os.remove(tmp_file)

            except Exception as e:

                # =====================================
                # Dead-Letter Pattern
                # Aísla archivos corruptos o con errores para análisis posterior
                # =====================================
                if os.path.exists(tmp_file):
                    s3.upload_file(tmp_file, bucket_name, f"{QUARANTINE}{rel_path}", Config=transfer_config)
                    os.remove(tmp_file)

                resultados.append((remoto, "", f"ERROR:{str(e)}"))

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
    ])

    return spark.createDataFrame(resultados, schema)
