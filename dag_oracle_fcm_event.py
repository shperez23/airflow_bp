import sys
from airflow.models import Variable

path_utils = Variable.get("PATH_DAG_UTILS", default_var="./dags")
sys.path.append(f"{path_utils}")
import factory_zr_zc


# ============================================================
# CONFIGURACIÓN DE DAGS EVENT-DRIVEN (DATASET)
# ============================================================
# NOTA: El schedule del DAG se deriva de los datasets generados
#       en los DAGs upstream definidos en los flujos.
# ============================================================

DAGS_CONFIG = [
    {
        "dag_id": "dag_oracle_fcm_tablas_impuestos_event",
        "codigo_malla": "EMP_FCM_001",
        "schedule": "none",
        "flujos": [
            {
                "workflow_name": "pl-ingesta-Uti-MotorTablas",
                "id_ingesta": "CAN_KUSTOM_ISD",
                "talla": "SparkResources",
                "nombre_tabla": "ZR_BP_Can_Cash_fcm_kustom_isd",
            },
        ],
        "tags": ["tribe: can", "cell: datos", "domain: cash", "type: DataZonaCurada"],
        "description": (
            "DAG event-driven por datasets para consumir la salida del DAG "
            "dag_oracle_fcm_tablas_impuestos"
        ),
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
