import sys
from airflow.models import Variable

path_utils = Variable.get("PATH_DAG_UTILS", default_var="./dags")
sys.path.append(f"{path_utils}")
import factory_zr


# ============================================================
# CONFIGURACIÓN DE DAGS
# ============================================================
# NOTA: No es necesario definir global_config ni operator_config.
#       El factory usa configuración por defecto automáticamente.
#       Solo define configs personalizadas si necesitas overrides específicos.
# ============================================================

DAGS_CONFIG = [
    {
        # dag de wteller
        "dag_id": "dag_oracle_fcm_tablas_impuestos",
        "codigo_malla": "EMP_FCM_001",
        "schedule": "2 5 * * *",
        "flujos": [
            # Flujo con sensor - inicia a las 02:00 AM
            {
                "workflow_name": "pl-ingesta-Uti-MotorTablas",
                "id_ingesta": "CAN_KUSTOM_ISD",
                "talla": "SparkResources",
                "nombre_tabla": "ZR_BP_Can_Cash_fcm_kustom_isd",
            },
            # Flujo dependiente: espera el dataset creado por ZR_BP_Can_Cash_fcm_kustom_isd
            {
                "workflow_name": "pl-ingesta-Uti-MotorTablas",
                "id_ingesta": "CAN_KUSTOM_ISD_DEP",
                "talla": "SparkResources",
                "nombre_tabla": "ZR_BP_Can_Cash_fcm_kustom_isd_dep",
                "depends_on_datasets": ["ZR_BP_Can_Cash_fcm_kustom_isd"],
            },
        ],
        "tags": ["tribe: can", "cell: datos", "domain: cash", "type: DataZonaRaw"],
        "description": "DAG de carga de información a ZR de oracle fcm grupo tablas impuestos",
        "owner": "sperezpe",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay_minutes": 5,
        "catchup": False,
        "max_active_runs": 1,
        "dagrun_timeout_minutes": 1200,
        "start_year": 2026,
        "start_month": 1,
        "start_day": 20,
    },
]


# ============================================================
# GENERACIÓN Y REGISTRO DE DAGS
# ============================================================

factory_zr.register_dags(DAGS_CONFIG, globals())
