import sys
from airflow.models import Variable

path_utils = Variable.get("PATH_DAG_UTILS", default_var="./dags")
sys.path.append(f"{path_utils}")
import factory_zr_zc


# ============================================================
# CONFIGURACIÓN DE DAGS EVENT-DRIVEN
# ============================================================
# NOTA: No es necesario definir global_config ni operator_config.
#       El factory usa configuración por defecto automáticamente.
#       Solo define configs personalizadas si necesitas overrides específicos.
# ============================================================

DAGS_CONFIG = [
    {
        # dag event-driven basado en datasets de ZR
        "dag_id": "dag_oracle_fcm_tablas_impuestos_event",
        "codigo_malla": "EMP_FCM_001",
        "schedule": "dataset",
        "flujos": [
            {
                "workflow_name": "pl-ingesta-Uti-MotorTablas",
                "id_ingesta": "CAN_KUSTOM_ISD_ZC",
                "talla": "SparkResources",
                "nombre_tabla": "ZC_BP_Can_Cash_fcm_kustom_isd",
                "dataset_origen": "ZR_BP_Can_Cash_fcm_kustom_isd",
            },
        ],
        "tags": ["tribe: can", "cell: datos", "domain: cash", "type: DataZonaCurada"],
        "description": "DAG event-driven ZC que depende de datasets ZR de oracle fcm",
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

factory_zr_zc.register_dags(DAGS_CONFIG, globals())
