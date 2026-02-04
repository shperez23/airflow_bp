"""
Framework POO para construcción de DAGs dinámicos de Airflow.
Clase padre abstracta y clase hija especializada.
Principios: Clean Code, SOLID, DRY.
SIN valores por defecto ni datos quemados.
Soporte para ejecución programada y event-driven con datasets opcionales.
"""

from abc import ABC, abstractmethod
from airflow import DAG
from airflow.models.param import Param
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.datasets import Dataset
from airflow.sensors.time_sensor import TimeSensor
from providers.stratio.rocket.operators.rocket_operator import RocketOperator
import pendulum
import pytz
from typing import List, Dict, Tuple, Optional, Callable, Any, Union, Iterable
from dataclasses import dataclass
import sys


# ============================================================
# CONSTANTES
# ============================================================

DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT_LONG = "%H:%M:%S"
DEFAULT_POKE_INTERVAL = 60
DEFAULT_SENSOR_TIMEOUT = 3600
SENSOR_MODE = "reschedule"
TASK_PREFIX_RUN = "run_"
TASK_PREFIX_DATASET = "dataset_"
TASK_PREFIX_SENSOR = "sensor_"
TASK_PREFIX_FILTER = "filter_"
TASK_FIN_PIPELINE = "fin_pipeline"
EXECUTION_NAME_PREFIX = "airflow_run_"

# Parámetros de RocketOperator
PARAM_ID_GRUPO = "P_ID_GRUPO_INGESTA"
PARAM_FECHA_DESDE = "P_FECHA_DESDE"
PARAM_FECHA_HASTA = "P_FECHA_HASTA"
PARAM_ENVIO_CORREO = "P_ENVIO_CORREO_ST"
PARAM_LIMIT = "P_LIMIT"
PARAM_DAG_ID = "P_DAG_ID"
PARAM_RUN_ID = "P_RUN_ID"
PARAM_CODIGO_MALLA = "P_CODIGO_MALLA"

PARAM_NUM_INGESTAS_CONCURRENTE = "P_NUM_INGESTAS_CONCURRENTE"
PARAM_NATIVO_LPARTICIONES = "P_NATIVO_LPARTICIONES"
PARAM_NATIVO_LINFERIOR = "P_NATIVO_LINFERIOR"
PARAM_NATIVO_LSUPERIOR = "P_NATIVO_LSUPERIOR"
PARAM_FETCH_SIZE_ASSIGNED = "P_FETCH_SIZE_ASSIGNED"
PARAM_PERSISTENT_VOLUME_GB = "P_PERSISTENT_VOLUME_GB"
PARAM_ENABLE_MD5 = "P_ENABLE_MD5"

# Valores por defecto de parámetros
DEFAULT_ENVIO_CORREO = "0"
DEFAULT_LIMIT = ""
DEFAULT_DAYS_BACK_FROM = 1  # Por defecto, 1 día atrás para fecha_desde
DEFAULT_DAYS_BACK_TO = 1    # Por defecto, 1 día atrás para fecha_hasta

# Jinja templates
TEMPLATE_FECHA_DESDE = "{{ params.fecha_desde }}"
TEMPLATE_FECHA_HASTA = "{{ params.fecha_hasta }}"
TEMPLATE_DAG_ID = "{{ dag.dag_id }}"
TEMPLATE_RUN_ID = "{{ run_id }}"

# Descripciones de parámetros
DESC_FECHA_DESDE = "Fecha hora desde (YYYY-MM-DD HH:MM:SS)"
DESC_FECHA_HASTA = "Fecha hora hasta (YYYY-MM-DD HH:MM:SS)"

# Callback status
STATUS_SUCCESS = 0
STATUS_FAILURE = 1


# ============================================================
# CONFIGURACIÓN (Data Classes) - TODOS LOS PARÁMETROS OBLIGATORIOS
# ============================================================

@dataclass
class GlobalConfig:
    """Configuración global - TODOS los parámetros son obligatorios"""

    path_utils: str
    ruta_primaria_ds: str
    ruta_grupo: str
    timezone: str

    def __post_init__(self):
        self.tz = pytz.timezone(self.timezone)
        sys.path.append(f"{self.path_utils}")


@dataclass
class DAGMetadata:
    """Metadata del DAG - TODOS los parámetros son obligatorios"""

    dag_id: str
    codigo_malla: str
    schedule: str
    # Usuario envía lista de dicts, se convierten a FlujoConfig internamente
    flujos: List[Dict]
    tags: List[str]
    description: str
    owner: str
    depends_on_past: bool
    email_on_failure: bool
    email_on_retry: bool
    retries: int
    retry_delay_minutes: int
    catchup: bool
    max_active_runs: int
    dagrun_timeout_minutes: int
    start_year: int
    start_month: int
    start_day: int
    days_back_from: int = DEFAULT_DAYS_BACK_FROM  # Días hacia atrás para fecha_desde
    days_back_to: int = DEFAULT_DAYS_BACK_TO      # Días hacia atrás para fecha_hasta

    def __post_init__(self):
        if self.schedule.lower() == "none":
            self.schedule = None

        # Convertir dicts a FlujoConfig
        self.flujos = [FlujoConfig(**flujo) for flujo in self.flujos]

        # Validar days_back_from y days_back_to
        if self.days_back_from < 0:
            raise ValueError(
                f"days_back_from debe ser >= 0, recibido: {self.days_back_from}"
            )
        if self.days_back_to < 0:
            raise ValueError(
                f"days_back_to debe ser >= 0, recibido: {self.days_back_to}"
            )
        if self.days_back_from < self.days_back_to:
            raise ValueError(
                f"days_back_from ({self.days_back_from}) debe ser >= days_back_to ({self.days_back_to}). "
                f"Ejemplo: days_back_from=7, days_back_to=1 genera rango desde hace 7 días hasta hace 1 día"
            )


@dataclass
class OperatorConfig:
    """Configuración de operadores - TODOS los parámetros son obligatorios"""

    connection_id: str
    retries_status: int
    status_polling_frequency: int
    extended_audit_info: bool
    params_lists_base: List[str]


@dataclass
class FlujoConfig:
    """
    Configuración de un flujo de ingesta.
    dataset_origen es opcional para soportar ejecución event-driven.
    """

    workflow_name: str
    id_ingesta: str
    talla: str
    nombre_tabla: str
    dataset_origen: Optional[str] = None
    horario: Optional[str] = None  # Opcional: formato "HH:MM:SS" o "HH:MM"

    p_num_ingestas_concurrente: Optional[Union[int, str]] = None
    p_nativo_lparticiones: Optional[Union[int, str]] = None
    p_nativo_linferior: Optional[Union[int, str]] = None
    p_nativo_lsuperior: Optional[Union[int, str]] = None
    p_fetch_size_assigned: Optional[Union[int, str]] = None
    p_persistent_volume_gb: Optional[Union[int, str]] = None
    p_enable_md5: Optional[Union[bool, int, str]] = None

    def __post_init__(self):
        """Validaciones y normalización"""
        if self.horario is not None and not self.horario.strip():
            self.horario = None

        if self.dataset_origen is not None and not self.dataset_origen.strip():
            self.dataset_origen = None

        if self.horario:
            parts = self.horario.split(":")
            if len(parts) not in (2, 3):
                raise ValueError(
                    f"Horario inválido '{self.horario}'. Use formato HH:MM o HH:MM:SS"
                )


# ============================================================
# CLASE PADRE ABSTRACTA
# ============================================================

class BaseDAGFactory(ABC):
    """
    Clase padre abstracta para construcción de DAGs.
    Define la estructura común y métodos abstractos.
    """

    def __init__(
        self,
        metadata: DAGMetadata,
        global_config: GlobalConfig,
        operator_config: OperatorConfig,
    ):
        self.metadata = metadata
        self.global_config = global_config
        self.operator_config = operator_config
        self.notifica = self._load_notifica()
        self.dag: Optional[DAG] = None

    def _load_notifica(self):
        """Carga módulo de notificaciones"""
        try:
            import notifica
            return notifica
        except ImportError:
            return None

    def _build_default_args(self) -> Dict:
        """Construye default args del DAG"""
        return {
            "owner": self.metadata.owner,
            "depends_on_past": self.metadata.depends_on_past,
            "email_on_failure": self.metadata.email_on_failure,
            "email_on_retry": self.metadata.email_on_retry,
            "retries": self.metadata.retries,
            "retry_delay": timedelta(minutes=self.metadata.retry_delay_minutes),
        }

    def _build_dag_params(self) -> Dict:
        now = datetime.now(self.global_config.tz)

        fecha_desde_date = now - timedelta(days=self.metadata.days_back_from)
        hora_desde = fecha_desde_date.replace(hour=0, minute=0, second=0, microsecond=0)

        fecha_hasta_date = now - timedelta(days=self.metadata.days_back_to)
        hora_hasta = fecha_hasta_date.replace(
            hour=23, minute=59, second=59, microsecond=999000
        )

        days_back_info = ""
        if self.metadata.days_back_from > 0 or self.metadata.days_back_to > 0:
            if self.metadata.days_back_from == self.metadata.days_back_to:
                days_back_info = f" (hace {self.metadata.days_back_from} días)"
            else:
                days_back_info = (
                    f" (rango: hace {self.metadata.days_back_from} días "
                    f"hasta hace {self.metadata.days_back_to} días)"
                )

        return {
            "fecha_desde": Param(
                default=hora_desde.strftime("%Y-%m-%d %H:%M:%S"),
                type="string",
                description=f"{DESC_FECHA_DESDE}{days_back_info}",
            ),
            "fecha_hasta": Param(
                default=hora_hasta.strftime("%Y-%m-%d %H:%M:%S"),
                type="string",
                description=f"{DESC_FECHA_HASTA}{days_back_info}",
            ),
        }

    def _build_success_callback(self) -> Optional[Callable]:
        """Construye callback de éxito"""
        if not self.notifica:
            return None
        return lambda context: self.notifica.send_notification(
            context,
            codigo_malla=self.metadata.codigo_malla,
            status=STATUS_SUCCESS,
            extra_message="",
        )

    def _build_failure_callback(self, extra_message: str) -> Optional[Callable]:
        """Construye callback de fallo"""
        if not self.notifica:
            return None
        return lambda context: self.notifica.send_notification(
            context,
            codigo_malla=self.metadata.codigo_malla,
            status=STATUS_FAILURE,
            extra_message=extra_message,
        )

    def create_dag(self) -> DAG:
        """Crea la instancia del DAG"""
        schedule = self._build_schedule()
        schedule_kwargs = (
            {"schedule": schedule}
            if isinstance(schedule, list)
            else {"schedule_interval": schedule}
        )
        self.dag = DAG(
            dag_id=self.metadata.dag_id,
            default_args=self._build_default_args(),
            description=self.metadata.description,
            **schedule_kwargs,
            start_date=pendulum.datetime(
                self.metadata.start_year,
                self.metadata.start_month,
                self.metadata.start_day,
                tz=self.global_config.timezone,
            ),
            catchup=self.metadata.catchup,
            max_active_runs=self.metadata.max_active_runs,
            dagrun_timeout=timedelta(minutes=self.metadata.dagrun_timeout_minutes),
            params=self._build_dag_params(),
            tags=self.metadata.tags,
            on_success_callback=self._build_success_callback(),
        )
        return self.dag

    @abstractmethod
    def _build_schedule(self):
        """Método abstracto: construir schedule"""
        pass

    @abstractmethod
    def build_tasks(self) -> List:
        """Método abstracto: construir tareas"""
        pass

    @abstractmethod
    def set_dependencies(self, tasks: List):
        """Método abstracto: establecer dependencias"""
        pass

    def build(self) -> DAG:
        """Template method: orquesta la construcción del DAG"""
        dag = self.create_dag()
        with dag:
            tasks = self.build_tasks()
            self.set_dependencies(tasks)
        return dag


# ============================================================
# CLASE HIJA: ROCKET DAG FACTORY UNIFICADO
# ============================================================

class RocketDAGFactory(BaseDAGFactory):
    """
    Clase hija especializada para DAGs con RocketOperator.
    Soporta ejecución programada y event-driven según dataset_origen.
    """

    def _build_schedule(self):
        dataset_origenes = [flujo.dataset_origen for flujo in self.metadata.flujos]
        if any(dataset_origenes):
            return self._build_dataset_schedule()
        return self.metadata.schedule

    def _build_dataset_schedule(self) -> List[Dataset]:
        dataset_paths = {
            self.build_dataset_path(flujo.dataset_origen)
            for flujo in self.metadata.flujos
            if flujo.dataset_origen
        }
        return [Dataset(path) for path in sorted(dataset_paths)]

    def _build_params_lists(self, talla: str) -> List[str]:
        """Construye lista de parámetros para RocketOperator"""
        params_lists = self.operator_config.params_lists_base.copy()
        talla = "SparkResources" if talla.lower() == "default" else talla
        params_lists.append(talla)
        return params_lists

    @staticmethod
    def _normalize_optional_value(value: Any) -> str:
        if isinstance(value, bool):
            return "1" if value else "0"
        return str(value)

    def _build_optional_extra_params(self, flujo: FlujoConfig) -> List[Dict[str, str]]:
        optional_params: List[Tuple[str, Any]] = [
            (PARAM_NUM_INGESTAS_CONCURRENTE, flujo.p_num_ingestas_concurrente),
            (PARAM_NATIVO_LPARTICIONES, flujo.p_nativo_lparticiones),
            (PARAM_NATIVO_LINFERIOR, flujo.p_nativo_linferior),
            (PARAM_NATIVO_LSUPERIOR, flujo.p_nativo_lsuperior),
            (PARAM_FETCH_SIZE_ASSIGNED, flujo.p_fetch_size_assigned),
            (PARAM_PERSISTENT_VOLUME_GB, flujo.p_persistent_volume_gb),
            (PARAM_ENABLE_MD5, flujo.p_enable_md5),
        ]

        extra: List[Dict[str, str]] = []
        for name, value in optional_params:
            if value is None:
                continue
            extra.append({"name": name, "value": self._normalize_optional_value(value)})
        return extra

    def _build_extra_params(self, flujo: FlujoConfig) -> List[Dict[str, str]]:
        """Construye parámetros extra para RocketOperator"""
        base_params = [
            {"name": PARAM_ID_GRUPO, "value": flujo.id_ingesta},
            {"name": PARAM_FECHA_DESDE, "value": TEMPLATE_FECHA_DESDE},
            {"name": PARAM_FECHA_HASTA, "value": TEMPLATE_FECHA_HASTA},
            {"name": PARAM_ENVIO_CORREO, "value": DEFAULT_ENVIO_CORREO},
            {"name": PARAM_LIMIT, "value": DEFAULT_LIMIT},
            {"name": PARAM_DAG_ID, "value": TEMPLATE_DAG_ID},
            {"name": PARAM_RUN_ID, "value": TEMPLATE_RUN_ID},
            {"name": PARAM_CODIGO_MALLA, "value": self.metadata.codigo_malla},
        ]
        return base_params + self._build_optional_extra_params(flujo)

    def _build_rocket_operator(self, flujo: FlujoConfig) -> RocketOperator:
        """Construye un RocketOperator"""
        return RocketOperator(
            task_id=f"{TASK_PREFIX_RUN}{flujo.nombre_tabla.lower()}",
            connection_id=self.operator_config.connection_id,
            group_name=self.global_config.ruta_grupo,
            workflow_name=flujo.workflow_name,
            retries_status=self.operator_config.retries_status,
            status_polling_frequency=self.operator_config.status_polling_frequency,
            paramsLists=self._build_params_lists(flujo.talla),
            extra_params=self._build_extra_params(flujo),
            extendedAuditInfo=self.operator_config.extended_audit_info,
            retry_delay=timedelta(minutes=self.metadata.retry_delay_minutes),
            execution_name=f"{EXECUTION_NAME_PREFIX}{flujo.nombre_tabla}",
            on_failure_callback=self._build_failure_callback(
                f"Fallo workflow {flujo.workflow_name}"
            ),
        )

    def build_dataset_path(self, nombre_tabla: str) -> str:
        """Construye la ruta del dataset"""
        return f"{self.global_config.ruta_primaria_ds}/{nombre_tabla}"

    def build_dataset_operator(self, nombre_tabla: str) -> DummyOperator:
        """Construye un DummyOperator para dataset"""
        dataset_path = self.build_dataset_path(nombre_tabla)
        return DummyOperator(
            task_id=f"{TASK_PREFIX_DATASET}{nombre_tabla.lower()}",
            outlets=[Dataset(dataset_path)],
            retry_delay=timedelta(minutes=1),
        )

    def _build_time_sensor(self, nombre_tabla: str, target_time: str) -> TimeSensor:
        return TimeSensor(
            task_id=f"{TASK_PREFIX_SENSOR}{nombre_tabla.lower()}",
            target_time=datetime.strptime(target_time, TIME_FORMAT_LONG).time(),
            poke_interval=DEFAULT_POKE_INTERVAL,
            timeout=DEFAULT_SENSOR_TIMEOUT,
            mode=SENSOR_MODE,
        )

    def _normalize_time(self, time_str: str) -> str:
        if len(time_str) == 5 and time_str.count(":") == 1:
            return f"{time_str}:00"
        return time_str

    def _create_sensor_if_needed(
        self,
        nombre_tabla: str,
        horario: Optional[str],
    ) -> Optional[TimeSensor]:
        if not horario:
            return None
        normalized_time = self._normalize_time(horario)
        return self._build_time_sensor(nombre_tabla, normalized_time)

    @staticmethod
    def _extract_dataset_uris(events: Iterable[Any]) -> List[str]:
        dataset_uris: List[str] = []
        for event in events:
            if isinstance(event, str):
                dataset_uris.append(event)
                continue
            if isinstance(event, dict):
                uri = event.get("dataset_uri") or event.get("dataset")
                if uri:
                    dataset_uris.append(uri)
                continue
            uri = getattr(event, "dataset_uri", None)
            if uri:
                dataset_uris.append(uri)
        return dataset_uris

    def _should_run_for_dataset(self, dataset_uri: str, **context) -> bool:
        triggering_events = context.get("triggering_dataset_events")
        if not triggering_events:
            return True
        if isinstance(triggering_events, dict):
            dataset_uris = triggering_events.keys()
        else:
            dataset_uris = self._extract_dataset_uris(triggering_events)
        return dataset_uri in set(dataset_uris)

    def _build_dataset_filter(
        self,
        nombre_tabla: str,
        dataset_origen: Optional[str],
    ) -> Optional[ShortCircuitOperator]:
        if not dataset_origen:
            return None
        dataset_uri = self.build_dataset_path(dataset_origen)
        return ShortCircuitOperator(
            task_id=f"{TASK_PREFIX_FILTER}{nombre_tabla.lower()}",
            python_callable=self._should_run_for_dataset,
            op_kwargs={"dataset_uri": dataset_uri},
        )

    def _build_task_group(
        self,
        flujo: FlujoConfig,
    ) -> Tuple[Optional[ShortCircuitOperator], Optional[TimeSensor], RocketOperator, DummyOperator]:
        dataset_filter = self._build_dataset_filter(flujo.nombre_tabla, flujo.dataset_origen)
        sensor = self._create_sensor_if_needed(flujo.nombre_tabla, flujo.horario)
        ingesta = self._build_rocket_operator(flujo)
        dataset = self.build_dataset_operator(flujo.nombre_tabla)
        return dataset_filter, sensor, ingesta, dataset

    def build_tasks(self) -> List[Tuple]:
        task_groups = []
        for flujo in self.metadata.flujos:
            task_group = self._build_task_group(flujo)
            task_groups.append(task_group)
        return task_groups

    def _set_task_chain(
        self,
        dataset_filter: Optional[ShortCircuitOperator],
        sensor: Optional[TimeSensor],
        ingesta: RocketOperator,
        dataset: DummyOperator,
    ) -> None:
        if dataset_filter and sensor:
            dataset_filter >> sensor >> ingesta >> dataset
        elif dataset_filter:
            dataset_filter >> ingesta >> dataset
        elif sensor:
            sensor >> ingesta >> dataset
        else:
            ingesta >> dataset

    def _create_final_task(self, dataset_tasks: List[DummyOperator]) -> None:
        fin_pipeline = DummyOperator(task_id=TASK_FIN_PIPELINE)
        for dataset in dataset_tasks:
            dataset >> fin_pipeline

    def set_dependencies(self, task_groups: List[Tuple]):
        dataset_tasks = []
        for dataset_filter, sensor, ingesta, dataset in task_groups:
            self._set_task_chain(dataset_filter, sensor, ingesta, dataset)
            dataset_tasks.append(dataset)
        self._create_final_task(dataset_tasks)


# ============================================================
# CONFIGURACIÓN POR DEFECTO
# ============================================================

def get_default_global_config() -> GlobalConfig:
    from airflow.models import Variable

    return GlobalConfig(
        path_utils=Variable.get("PATH_DAG_UTILS", default_var="/dags"),
        ruta_primaria_ds=Variable.get("P_RUTA_PRIMARIA_DATASETS", default_var="/ruta/"),
        ruta_grupo="/home/cda-motor-ingesta-datos/tablas-v2",
        timezone="America/Guayaquil",
    )


def get_default_operator_config() -> OperatorConfig:
    return OperatorConfig(
        connection_id="rocket-connectors",
        retries_status=100,
        status_polling_frequency=30,
        extended_audit_info=True,
        params_lists_base=["Environment", "SparkConfigurations"],
    )


# ============================================================
# FUNCIONES DE UTILIDAD
# ============================================================

def create_dag(
    config: Dict,
    global_config: Optional[GlobalConfig] = None,
    operator_config: Optional[OperatorConfig] = None,
) -> DAG:
    if global_config is None:
        global_config = get_default_global_config()
    if operator_config is None:
        operator_config = get_default_operator_config()

    metadata = DAGMetadata(**config)
    factory = RocketDAGFactory(metadata, global_config, operator_config)
    return factory.build()


def create_multiple_dags(
    configs: List[Dict],
    global_config: Optional[GlobalConfig] = None,
    operator_config: Optional[OperatorConfig] = None,
) -> Dict[str, DAG]:
    if global_config is None:
        global_config = get_default_global_config()
    if operator_config is None:
        operator_config = get_default_operator_config()

    dags = {}
    for config in configs:
        dag = create_dag(config, global_config, operator_config)
        dags[config["dag_id"]] = dag
    return dags


def register_dags(
    configs: List[Dict],
    globals_dict: Dict,
    global_config: Optional[GlobalConfig] = None,
    operator_config: Optional[OperatorConfig] = None,
):
    dags = create_multiple_dags(configs, global_config, operator_config)
    for dag_id, dag in dags.items():
        globals_dict[dag_id] = dag
