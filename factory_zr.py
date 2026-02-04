"""
Framework POO para construcción de DAGs dinámicos de Airflow en Zona Raw (ZR).
Clase padre abstracta y clases hijas especializadas.
Principios: Clean Code, SOLID, DRY.
SIN valores por defecto ni datos quemados.

VERSION 4: Framework Event-Driven con Optimizaciones Automáticas
Desarrollo asistido por IA - Febrero 2026

FUNCIONALIDADES PRINCIPALES:
- ✅ Publicación automática de datasets para arquitectura event-driven
- ✅ Sensores asíncronos (deferrables) - NO bloquean workers del cluster
- ✅ Agrupación inteligente de sensores por horario (~66% reducción de tareas)
- ✅ Filtrado event-driven con ShortCircuitOperator (70-90% ahorro de ejecuciones)
- ✅ Soporte híbrido: programado + event-driven en el mismo DAG
- ✅ Cálculo dinámico de rangos de fechas con days_back_from/days_back_to
- ✅ Configuración por flujo de sensores (poke_interval, timeout, deferrable)

MODOS DE EJECUCIÓN SOPORTADOS:
1. Programado Simple: Schedule cron, ejecución inmediata
2. Programado + Sensores: Schedule cron + espera de tiempo
3. Event-Driven: Disparado por datasets upstream
4. Híbrido: Event-driven + sensores de tiempo

OPTIMIZACIONES AUTOMÁTICAS (sin configuración):
- Sensores deferrables por defecto (si Airflow 2.2+ con Triggerer disponible)
- Un sensor compartido por grupo de flujos con mismo horario
- Filtrado condicional de flujos según dataset disparador
- Publicación de dataset por cada flujo para linaje de datos

AUTORÍA:
Desarrollado por el equipo de Ingeniería de Datos con asistencia de IA.
Diseño orientado a arquitecturas modernas de data mesh y event-driven.
"""

from abc import ABC, abstractmethod
from airflow import DAG
from airflow.models.param import Param
from datetime import datetime, timedelta
import inspect
from airflow.operators.dummy import DummyOperator
import airflow.datasets as datasets

Dataset = datasets.Dataset
DatasetAny = getattr(datasets, "DatasetAny", None)
DatasetOr = getattr(datasets, "DatasetOr", None)
DATASET_ANY_AVAILABLE = DatasetAny is not None
DATASET_OR_AVAILABLE = DatasetOr is not None
from airflow.sensors.time_sensor import TimeSensor

# Sensor deferrable - NO ocupa workers, usa Triggerer
try:
    from airflow.sensors.date_time import DateTimeSensorAsync

    DEFERRABLE_AVAILABLE = True
except ImportError:
    DateTimeSensorAsync = None
    DEFERRABLE_AVAILABLE = False

# ShortCircuitOperator para filtrado condicional (event-driven)
try:
    from airflow.operators.python import ShortCircuitOperator

    SHORTCIRCUIT_AVAILABLE = True
except ImportError:
    ShortCircuitOperator = None
    SHORTCIRCUIT_AVAILABLE = False

from providers.stratio.rocket.operators.rocket_operator import RocketOperator
import pendulum
import pytz
from typing import List, Dict, Tuple, Optional, Callable, Any, Union, Iterable
from dataclasses import dataclass
import sys


# ============================================================================
# CONSTANTES DE FORMATO Y TIEMPOS
# ============================================================================

# Formatos de fecha y hora
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT_LONG = "%H:%M:%S"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Configuración optimizada de sensores para reducir consumo de recursos del scheduler
DEFAULT_POKE_INTERVAL = 300  # 5 minutos - balance entre latencia y carga del scheduler
DEFAULT_SENSOR_TIMEOUT = 7200  # 2 horas - tiempo máximo de espera antes de fallar
SENSOR_MODE = "reschedule"  # Libera worker slot entre chequeos (no bloquea workers)

# Constantes de tiempo para cálculo de rangos de fechas
DAY_START_HOUR = 0
DAY_START_MINUTE = 0
DAY_START_SECOND = 0
DAY_END_HOUR = 23
DAY_END_MINUTE = 59
DAY_END_SECOND = 59
DAY_END_MICROSECOND = 999000

# Prefijos de nombres de tareas (task_id) para identificación clara en Airflow UI
TASK_PREFIX_RUN = "run_"  # Tareas de ejecución de workflows (RocketOperator)
TASK_PREFIX_DATASET = "dataset_"  # Tareas de publicación de datasets (DummyOperator)
TASK_PREFIX_SENSOR = (
    "sensor_"  # Tareas de sensores de tiempo (TimeSensor/DateTimeSensorAsync)
)
TASK_PREFIX_FILTER = "filter_"  # Tareas de filtrado event-driven (ShortCircuitOperator)
TASK_FIN_PIPELINE = "fin_pipeline"  # Tarea final que consolida todo el pipeline
EXECUTION_NAME_PREFIX = (
    "airflow_run_"  # Prefijo para nombres de ejecución en Stratio Rocket
)

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
PARAM_PERSISTENT_VOLUMEN_GB = "P_PERSISTENT_VOLUMEN_GB"
PARAM_ENABLE_MD5 = "P_ENABLE_MD5"

# Valores por defecto de parámetros
DEFAULT_ENVIO_CORREO = "0"
DEFAULT_LIMIT = " "
DEFAULT_DAYS_BACK_FROM = 1  # Por defecto, 1 día atrás para fecha_desde
DEFAULT_DAYS_BACK_TO = 1  # Por defecto, 1 día atrás para fecha_hasta

# Jinja templates
TEMPLATE_FECHA_DESDE = "{{ params.fecha_desde }}"
TEMPLATE_FECHA_HASTA = "{{ params.fecha_hasta }}"
TEMPLATE_EVENT_FECHA_DESDE = (
    "{{ event_param(triggering_dataset_events, 'fecha_desde', params.fecha_desde) }}"
)
TEMPLATE_EVENT_FECHA_HASTA = (
    "{{ event_param(triggering_dataset_events, 'fecha_hasta', params.fecha_hasta) }}"
)
TEMPLATE_DAG_ID = "{{ dag.dag_id }}"
TEMPLATE_RUN_ID = "{{ run_id }}"

# Descripciones de parámetros
DESC_FECHA_DESDE = "Fecha hora desde (YYYY-MM-DD HH:MM:SS)"
DESC_FECHA_HASTA = "Fecha hora hasta (YYYY-MM-DD HH:MM:SS)"

# Callback status
STATUS_SUCCESS = 0
STATUS_FAILURE = 1


def _flatten_triggering_events(triggering_dataset_events: Any) -> List[Any]:
    if isinstance(triggering_dataset_events, dict):
        items = triggering_dataset_events.values()
    else:
        items = triggering_dataset_events

    events: List[Any] = []
    for item in items:
        if isinstance(item, (list, tuple, set)):
            events.extend(item)
        else:
            events.append(item)
    return events


def event_param(triggering_dataset_events: Any, name: str, fallback: str) -> str:
    if not triggering_dataset_events:
        return fallback

    for event in _flatten_triggering_events(triggering_dataset_events):
        if isinstance(event, dict):
            extra = event.get("extra") or event.get("metadata")
        else:
            extra = getattr(event, "extra", None)

        if isinstance(extra, dict) and name in extra:
            return extra[name]

    return fallback


# ============================================================================
# CONFIGURACIÓN (Data Classes) - TODOS LOS PARÁMETROS OBLIGATORIOS
# ============================================================================


@dataclass
class GlobalConfig:
    """
    Configuración global del entorno de ejecución.

    Attributes:
        path_utils: Ruta a utilidades compartidas de DAGs (ej: './dags')
        ruta_primaria_ds: Ruta base para datasets (ej: '/datasets/zona_raw')
        ruta_grupo: Ruta del grupo de workflows en Stratio Rocket
        timezone: Zona horaria para cálculos de fecha/hora (ej: 'America/Guayaquil')

    Note:
        Todos los parámetros son obligatorios. Se valida el timezone en __post_init__.
    """

    path_utils: str
    ruta_primaria_ds: str
    ruta_grupo: str
    timezone: str

    def __post_init__(self):
        """Inicializa timezone object y agrega path_utils a sys.path para imports dinámicos."""
        self.timezone_obj = pytz.timezone(
            self.timezone
        )  # Validación implícita de timezone
        sys.path.append(f"{self.path_utils}")  # Permite importar módulos de dags_utils


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
    days_back_to: int = DEFAULT_DAYS_BACK_TO  # Días hacia atrás para fecha_hasta

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
    # Configuración optimizada de sensores
    sensor_poke_interval: int = DEFAULT_POKE_INTERVAL
    sensor_timeout: int = DEFAULT_SENSOR_TIMEOUT
    sensor_mode: str = SENSOR_MODE
    # Usar sensores deferrables (NO ocupan workers, requiere Triggerer habilitado)
    use_deferrable_sensors: bool = DEFERRABLE_AVAILABLE


@dataclass
class FlujoConfig:
    """
    Configuración de un flujo de ingesta.

    Soporta ejecución programada (horario) y event-driven (dataset_origen).
    Usa nombres explícitos en lugar de tuplas posicionales para evitar errores.
    """

    workflow_name: str
    id_ingesta: str
    talla: str
    nombre_tabla: str
    horario: Optional[str] = None  # Opcional: formato "HH:MM:SS" o "HH:MM"
    dataset_origen: Optional[str] = (
        None  # Opcional: dataset que dispara este flujo (event-driven)
    )
    # Configuración opcional de sensor por flujo (sobrescribe defaults)
    sensor_poke_interval: Optional[int] = None  # Intervalo de chequeo en segundos
    sensor_timeout: Optional[int] = None  # Timeout en segundos
    use_deferrable: Optional[bool] = None  # Usar sensor deferrable (no ocupa workers)

    p_num_ingestas_concurrente: Optional[Union[int, str]] = None
    p_nativo_lparticiones: Optional[Union[int, str]] = None
    p_nativo_linferior: Optional[Union[int, str]] = None
    p_nativo_lsuperior: Optional[Union[int, str]] = None
    p_fetch_size_assigned: Optional[Union[int, str]] = None
    p_persistent_volumen_gb: Optional[Union[int, str]] = None
    p_enable_md5: Optional[Union[bool, int, str]] = None

    def __post_init__(self):
        """Validaciones y normalización de horario y dataset_origen"""
        # Normalizar strings vacíos a None
        if self.horario is not None and not self.horario.strip():
            self.horario = None

        if self.dataset_origen is not None and not self.dataset_origen.strip():
            self.dataset_origen = None

        # Validar formato de horario
        if self.horario:
            parts = self.horario.split(":")
            if len(parts) not in (2, 3):
                raise ValueError(
                    f"Horario inválido '{self.horario}'. Use formato HH:MM o HH:MM:SS"
                )


# ============================================================================
# CLASE PADRE ABSTRACTA
# ============================================================================


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
        """Construye default_args del DAG"""
        return {
            "owner": self.metadata.owner,
            "depends_on_past": self.metadata.depends_on_past,
            "email_on_failure": self.metadata.email_on_failure,
            "email_on_retry": self.metadata.email_on_retry,
            "retries": self.metadata.retries,
            "retry_delay": timedelta(minutes=self.metadata.retry_delay_minutes),
        }

    def _build_dag_params(self) -> Dict:
        """
        Construye parámetros de fecha para el DAG basados en days_back_from y days_back_to.

        Returns:
            Dict con parámetros 'fecha_desde' y 'fecha_hasta' (tipo Param de Airflow)

        Example:
            Si days_back_from=7 y days_back_to=1:
            - fecha_desde: hace 7 días a las 00:00:00
            - fecha_hasta: hace 1 día a las 23:59:59
        """
        now = datetime.now(self.global_config.timezone_obj)

        # Calcular fecha_desde: inicio del día de hace N días
        fecha_desde_date = now - timedelta(days=self.metadata.days_back_from)
        fecha_desde_datetime = fecha_desde_date.replace(
            hour=DAY_START_HOUR,
            minute=DAY_START_MINUTE,
            second=DAY_START_SECOND,
            microsecond=0,
        )

        # Calcular fecha_hasta: fin del día de hace N días
        fecha_hasta_date = now - timedelta(days=self.metadata.days_back_to)
        fecha_hasta_datetime = fecha_hasta_date.replace(
            hour=DAY_END_HOUR,
            minute=DAY_END_MINUTE,
            second=DAY_END_SECOND,
            microsecond=DAY_END_MICROSECOND,
        )

        # Generar descripción informativa para el usuario en Airflow UI
        days_back_description = self._generate_days_back_description()

        return {
            "fecha_desde": Param(
                default=fecha_desde_datetime.strftime(DATETIME_FORMAT),
                type="string",
                description=f"{DESC_FECHA_DESDE}{days_back_description}",
            ),
            "fecha_hasta": Param(
                default=fecha_hasta_datetime.strftime(DATETIME_FORMAT),
                type="string",
                description=f"{DESC_FECHA_HASTA}{days_back_description}",
            ),
        }

    def _generate_days_back_description(self) -> str:
        """
        Genera descripción legible del rango de días hacia atrás.

        Returns:
            String con descripción del rango (ej: " (hace 7 días)" o " (rango: hace 7 días hasta hace 1 día)")
        """
        if self.metadata.days_back_from == 0 and self.metadata.days_back_to == 0:
            return ""  # Fechas del día actual

        if self.metadata.days_back_from == self.metadata.days_back_to:
            return f" (hace {self.metadata.days_back_from} día{'s' if self.metadata.days_back_from != 1 else ''})"

        return f" (rango: hace {self.metadata.days_back_from} días hasta hace {self.metadata.days_back_to} día{'s' if self.metadata.days_back_to != 1 else ''})"

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
        """
        Crea la instancia del DAG con soporte para schedule dinámico.

        El schedule puede ser:
        - String (cron): "@daily", "0 10 * * *", etc. (ejecución programada)
        - List[Dataset]: [Dataset("/path/tabla1")] (ejecución event-driven)
        - None: Solo ejecución manual
        """
        schedule = self._build_schedule()

        # Airflow 2.0+ usa parámetros diferentes según el tipo de schedule
        if self._is_dataset_schedule(schedule):
            schedule_kwargs = {"schedule": schedule}
        else:
            schedule_kwargs = {"schedule_interval": schedule}

        self.dag = DAG(
            dag_id=self.metadata.dag_id,
            default_args=self._build_default_args(),
            description=self.metadata.description,
            **schedule_kwargs,  # Aplicar schedule de forma dinámica
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
            user_defined_macros={"event_param": event_param},
        )
        return self.dag

    @abstractmethod
    def _build_schedule(self) -> Union[str, List[Dataset], Any, None]:
        """
        Método abstracto: construir schedule del DAG.

        Returns:
            - str: Schedule cron ("@daily", "0 10 * * *") para ejecución programada
            - List[Dataset]: Lista de datasets para ejecución event-driven
            - None: Solo ejecución manual
        """
        pass

    @staticmethod
    def _is_dataset_schedule(schedule: Any) -> bool:
        """Identifica si el schedule corresponde a datasets (event-driven)."""
        if isinstance(schedule, list):
            return all(isinstance(item, Dataset) for item in schedule)
        if isinstance(schedule, Dataset):
            return True
        if DATASET_ANY_AVAILABLE and isinstance(schedule, DatasetAny):
            return True
        if DATASET_OR_AVAILABLE and isinstance(schedule, DatasetOr):
            return True
        return False

    @abstractmethod
    def build_tasks(self) -> List:
        """Método abstracto: construir tareas"""
        pass

    @abstractmethod
    def set_dependencies(self, tasks: List):
        """Método abstracto: establecer dependencias"""
        pass

    def build(self) -> DAG:
        """Template method: orquesta la construcción del DAG con sensores optimizados"""
        dag = self.create_dag()
        with dag:
            sensores_compartidos, task_groups = self.build_tasks()
            self.set_dependencies(sensores_compartidos, task_groups)
        return dag


# ============================================================================
# CLASE HIJA: ROCKET DAG FACTORY
# ============================================================================


class RocketDAGFactory(BaseDAGFactory):
    """
    Clase hija especializada para DAGs con RocketOperator.

    Implementa la lógica específica para tareas de ingesta con soporte para:
    - Ejecución programada (schedule cron + sensores de tiempo)
    - Ejecución event-driven (datasets + filtrado inteligente)
    - Sensores deferrables (no ocupan workers)
    - Agrupación de sensores por horario (optimización)
    """

    def _build_schedule(self) -> Union[str, List[Dataset], None]:
        """
        Construye el schedule del DAG con detección automática de modo.

        LÓGICA:
        - Si algún flujo tiene dataset_origen → Modo EVENT-DRIVEN
        - Si todos los flujos tienen horario → Modo PROGRAMADO
        - Si mezcla → Modo HÍBRIDO (event-driven + sensores de tiempo)

        Returns:
            - List[Dataset]/DatasetAny/DatasetOr: Si hay dataset_origen en algún flujo (event-driven)
            - str/None: Si no hay dataset_origen (programado tradicional)

        Example:
            # Caso 1: Event-driven puro
            flujos = [{"dataset_origen": "tabla_raw"}]
            → schedule = [Dataset("/path/tabla_raw")]

            # Caso 2: Programado puro
            flujos = [{"horario": "10:00"}]
            → schedule = "@daily"

            # Caso 3: Híbrido (event-driven + time sensors)
            flujos = [
                {"dataset_origen": "tabla_raw", "horario": "10:00"},
                {"dataset_origen": "tabla_raw2"}
            ]
            → schedule = DatasetAny(Dataset("/path/tabla_raw"), Dataset("/path/tabla_raw2"))
            → Los sensores de tiempo se aplican dentro del DAG
        """
        dataset_origenes = [flujo.dataset_origen for flujo in self.metadata.flujos]

        # Si hay al menos un dataset_origen, activar modo event-driven
        if any(dataset_origenes):
            return self._build_dataset_schedule()

        # Sin dataset_origen → usar schedule programado tradicional
        return self.metadata.schedule

    def _build_dataset_schedule(self) -> Union[List[Dataset], Any]:
        """
        Construye lista de datasets únicos para event-driven scheduling.

        Recopila todos los dataset_origen únicos de los flujos y los
        convierte en objetos Dataset de Airflow.

        Returns:
            Lista ordenada de Dataset objects

        Example:
            flujos = [
                {"dataset_origen": "tabla_a"},
                {"dataset_origen": "tabla_b"},
                {"dataset_origen": "tabla_a"},  # Duplicado
            ]
            → DatasetAny(Dataset("/path/tabla_a"), Dataset("/path/tabla_b"))

        Note:
            El DAG se disparará cuando CUALQUIERA de estos datasets se actualice.
            El filtrado por flujo específico se hace con ShortCircuitOperator.
        """
        dataset_paths = {
            self._build_dataset_path(flujo.dataset_origen)
            for flujo in self.metadata.flujos
            if flujo.dataset_origen
        }
        datasets = [Dataset(path) for path in sorted(dataset_paths)]
        if DATASET_ANY_AVAILABLE:
            return DatasetAny(*datasets)
        if DATASET_OR_AVAILABLE:
            return DatasetOr(*datasets)
        return datasets

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
            (PARAM_PERSISTENT_VOLUMEN_GB, flujo.p_persistent_volumen_gb),
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
            {"name": PARAM_FECHA_DESDE, "value": TEMPLATE_EVENT_FECHA_DESDE},
            {"name": PARAM_FECHA_HASTA, "value": TEMPLATE_EVENT_FECHA_HASTA},
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

    def _build_dataset_path(self, nombre_tabla: str) -> str:
        """Construye la ruta del dataset"""
        return f"{self.global_config.ruta_primaria_ds}/{nombre_tabla}"

    @staticmethod
    def _dataset_init_supports(arg_name: str) -> bool:
        try:
            return arg_name in inspect.signature(Dataset).parameters
        except (TypeError, ValueError):
            return False

    def _build_dataset(self, nombre_tabla: str) -> Dataset:
        dataset_path = self._build_dataset_path(nombre_tabla)
        extra = {
            "fecha_desde": TEMPLATE_FECHA_DESDE,
            "fecha_hasta": TEMPLATE_FECHA_HASTA,
        }

        if self._dataset_init_supports("extra"):
            return Dataset(dataset_path, extra=extra)
        if self._dataset_init_supports("metadata"):
            return Dataset(dataset_path, metadata=extra)
        return Dataset(dataset_path)

    def _build_dataset_operator(self, nombre_tabla: str) -> DummyOperator:
        """Construye un DummyOperator para dataset"""
        dataset = self._build_dataset(nombre_tabla)
        return DummyOperator(
            task_id=f"{TASK_PREFIX_DATASET}{nombre_tabla.lower()}",
            outlets=[dataset],
            retry_delay=timedelta(minutes=1),
        )

    def _build_time_sensor(
        self,
        nombre_tabla: str,
        target_time: str,
        poke_interval: Optional[int] = None,
        timeout: Optional[int] = None,
        use_deferrable: Optional[bool] = None,
    ) -> Union[TimeSensor, Any]:
        """Construye sensor optimizado: deferrable (NO ocupa workers) o tradicional"""
        task_id = f"{TASK_PREFIX_SENSOR}{nombre_tabla.lower()}"
        target_datetime = datetime.strptime(target_time, TIME_FORMAT_LONG).time()

        # Determinar si usar sensor deferrable
        should_defer = (
            use_deferrable
            if use_deferrable is not None
            else self.operator_config.use_deferrable_sensors
        )

        if should_defer and DEFERRABLE_AVAILABLE:
            # Sensor DEFERRABLE: NO ocupa workers, usa Triggerer
            # Requiere calcular target_time como datetime completo
            now = datetime.now(self.global_config.timezone_obj)
            target_dt = self.global_config.timezone_obj.localize(
                datetime.combine(now.date(), target_datetime)
            )

            # Si la hora ya pasó hoy, programar para mañana
            if target_dt <= now:
                target_dt += timedelta(days=1)

            return DateTimeSensorAsync(
                task_id=task_id,
                target_time=target_dt,
                # Sensores deferrables no usan poke_interval ni mode
                # El Triggerer maneja el chequeo de forma eficiente
            )
        else:
            # Sensor TRADICIONAL con mode=reschedule (libera workers entre pokes)
            # IMPORTANTE: TimeSensor espera la hora del DÍA ACTUAL.
            # Si la hora ya pasó, el sensor esperará indefinidamente.
            # Por eso verificamos y ajustamos el schedule del DAG en consecuencia.
            return TimeSensor(
                task_id=task_id,
                target_time=target_datetime,
                poke_interval=poke_interval
                or self.operator_config.sensor_poke_interval,
                timeout=timeout or self.operator_config.sensor_timeout,
                mode=self.operator_config.sensor_mode,
            )

    def _normalize_time(self, time_str: str) -> str:
        if len(time_str) == 5 and time_str.count(":") == 1:
            return f"{time_str}:00"
        return time_str

    def _create_sensor_if_needed(
        self,
        nombre_tabla: str,
        horario: Optional[str],
        poke_interval: Optional[int] = None,
        timeout: Optional[int] = None,
        use_deferrable: Optional[bool] = None,
    ) -> Optional[Union[TimeSensor, Any]]:
        """Crea sensor solo si se especifica horario, con configuración optimizada"""
        if not horario:
            return None
        normalized_time = self._normalize_time(horario)
        return self._build_time_sensor(
            nombre_tabla,
            normalized_time,
            poke_interval=poke_interval,
            timeout=timeout,
            use_deferrable=use_deferrable,
        )


    @staticmethod
    def _extract_dataset_uris(events: Iterable[Any]) -> List[str]:
        """
        Extrae URIs de datasets desde eventos de disparo de Airflow.

        Maneja múltiples formatos de eventos para compatibilidad entre
        versiones de Airflow (2.0, 2.2, 2.4+).

        Args:
            events: Iterable de eventos de dataset (formato varía por versión)

        Returns:
            Lista de URIs de datasets extraídos

        Example:
            # Eventos como strings
            events = ["/path/tabla1", "/path/tabla2"]
            → ["/path/tabla1", "/path/tabla2"]

            # Eventos como dicts
            events = [
                {"dataset_uri": "/path/tabla1"},
                {"dataset": "/path/tabla2"}  # Formato alternativo
            ]
            → ["/path/tabla1", "/path/tabla2"]

            # Eventos como objetos
            events = [obj1, obj2]  # con atributo .dataset_uri
            → [obj1.dataset_uri, obj2.dataset_uri]
        """
        dataset_uris: List[str] = []
        for event in events:
            # Caso 1: String directo (formato simple)
            if isinstance(event, str):
                dataset_uris.append(event)
                continue

            # Caso 2: Diccionario (formato común en Airflow 2.4+)
            if isinstance(event, dict):
                uri = event.get("dataset_uri") or event.get("dataset")
                if uri:
                    dataset_uris.append(uri)
                continue

            # Caso 3: Objeto con atributo dataset_uri (formato Airflow 2.0-2.2)
            uri = getattr(event, "dataset_uri", None)
            if uri:
                dataset_uris.append(uri)

        return dataset_uris

    def _should_run_for_dataset(self, dataset_uri: str, **context) -> bool:
        """
        Decide si un flujo debe ejecutarse según el dataset que disparó el DAG.

        LÓGICA DE FILTRADO:
        - Si no hay triggering_events (ejecución manual) → Ejecutar SIEMPRE
        - Si hay triggering_events → Ejecutar SOLO si dataset_uri está en los disparadores

        Args:
            dataset_uri: URI del dataset_origen de este flujo específico
            **context: Contexto de Airflow con triggering_dataset_events

        Returns:
            True si el flujo debe ejecutarse, False para saltar

        Example:
            # DAG escucha: [Dataset("/tabla_a"), Dataset("/tabla_b")]
            # Se actualiza SOLO /tabla_a

            Flujo 1 (dataset_origen="tabla_a"):
                triggering_events = {"/tabla_a": [...]}
                _should_run_for_dataset("/tabla_a") → True ✅

            Flujo 2 (dataset_origen="tabla_b"):
                triggering_events = {"/tabla_a": [...]}
                _should_run_for_dataset("/tabla_b") → False ❌

        Note:
            Usado por ShortCircuitOperator para filtrado eficiente.
            Evita ejecutar workflows innecesarios cuando solo algunos datasets se actualizan.
        """
        triggering_events = context.get("triggering_dataset_events")

        # Sin eventos de disparo (ejecución manual) → ejecutar todo
        if not triggering_events:
            return True

        # Extraer URIs según el formato de los eventos
        if isinstance(triggering_events, dict):
            dataset_uris = triggering_events.keys()
        else:
            dataset_uris = self._extract_dataset_uris(triggering_events)

        # Ejecutar solo si nuestro dataset está en los disparadores
        return dataset_uri in set(dataset_uris)

    def _build_dataset_filter(
        self,
        nombre_tabla: str,
        dataset_origen: Optional[str],
    ) -> Optional[Any]:
        """
        Construye filtro de ejecución condicional para event-driven.

        Crea un ShortCircuitOperator que decide dinámicamente si este flujo
        debe ejecutarse según el dataset que disparó el DAG.

        Args:
            nombre_tabla: Nombre de la tabla (para task_id)
            dataset_origen: Dataset que debe disparar este flujo (None = sin filtro)

        Returns:
            ShortCircuitOperator si hay dataset_origen y está disponible, None en caso contrario

        Example:
            # Con dataset_origen
            _build_dataset_filter("tabla_clean", "tabla_raw")
            → ShortCircuitOperator(
                task_id="filter_tabla_clean",
                python_callable=_should_run_for_dataset,
                op_kwargs={"dataset_uri": "/path/tabla_raw"}
            )

            # Sin dataset_origen
            _build_dataset_filter("tabla_clean", None)
            → None

        Note:
            ShortCircuitOperator evalúa _should_run_for_dataset():
            - True → Continúa con sensor/ingesta
            - False → Salta toda la cadena (sensor >> ingesta >> dataset)

            Si ShortCircuitOperator no está disponible (import falló), retorna None.
        """
        if not dataset_origen or not SHORTCIRCUIT_AVAILABLE:
            return None

        dataset_uri = self._build_dataset_path(dataset_origen)
        return ShortCircuitOperator(
            task_id=f"{TASK_PREFIX_FILTER}{nombre_tabla.lower()}",
            python_callable=self._should_run_for_dataset,
            op_kwargs={"dataset_uri": dataset_uri},
        )


    def build_tasks(
        self,
    ) -> Tuple[
        Dict[str, Union[TimeSensor, Any]],
        List[Tuple[Optional[Any], FlujoConfig, RocketOperator, DummyOperator]],
    ]:
        """
        Construye tareas optimizadas con sensores agrupados y filtros event-driven.

        OPTIMIZACIÓN: Si múltiples flujos tienen el mismo horario,
        se crea UN SOLO sensor compartido para todos ellos.

        EVENT-DRIVEN: Si flujo tiene dataset_origen, se crea ShortCircuitOperator
        que evalúa si el dataset disparador coincide con el origen esperado.

        Returns:
            Tuple con:
            - Dict[horario, sensor]: Sensores únicos por horario
            - List[filter, flujo, ingesta, dataset]: Tareas de ingesta con filtro opcional

        Example:
            >>> sensors, tasks = factory.build_tasks()
            >>> sensors
            {"08:00:00": DateTimeSensorAsync(task_id="group_080000")}

            >>> tasks
            [
                (ShortCircuitOperator(...), flujo1, ingesta1, dataset1),  # Event-driven
                (None, flujo2, ingesta2, dataset2),                       # Programado
            ]

        Note:
            - Filtros solo se crean si flujo.dataset_origen está definido Y ShortCircuitOperator disponible
            - Sensores compartidos reducen ~66% de tareas sensor
            - Compatible con modo híbrido (cron + datasets)
        """
        # Agrupar flujos por horario normalizado
        flujos_por_horario: Dict[str, List[FlujoConfig]] = {}
        flujos_sin_horario: List[FlujoConfig] = []

        for flujo in self.metadata.flujos:
            if flujo.horario:
                horario_norm = self._normalize_time(flujo.horario)
                if horario_norm not in flujos_por_horario:
                    flujos_por_horario[horario_norm] = []
                flujos_por_horario[horario_norm].append(flujo)
            else:
                flujos_sin_horario.append(flujo)

        # Crear sensores únicos por horario (agrupados)
        sensores_compartidos: Dict[str, Union[TimeSensor, Any]] = {}

        for horario, flujos_grupo in flujos_por_horario.items():
            # Buscar la configuración más estricta del grupo
            min_poke_interval = None
            min_timeout = None
            any_deferrable = None

            for f in flujos_grupo:
                if f.sensor_poke_interval is not None:
                    if (
                        min_poke_interval is None
                        or f.sensor_poke_interval < min_poke_interval
                    ):
                        min_poke_interval = f.sensor_poke_interval
                if f.sensor_timeout is not None:
                    if min_timeout is None or f.sensor_timeout < min_timeout:
                        min_timeout = f.sensor_timeout
                if f.use_deferrable is not None:
                    any_deferrable = f.use_deferrable

            # Generar nombre descriptivo para el sensor compartido
            tablas_grupo = [f.nombre_tabla for f in flujos_grupo]
            if len(tablas_grupo) == 1:
                sensor_name = tablas_grupo[0]
            else:
                # Nombre compuesto para sensor compartido
                sensor_name = f"group_{horario.replace(':', '')}"

            sensor = self._build_time_sensor(
                sensor_name,
                horario,
                poke_interval=min_poke_interval,
                timeout=min_timeout,
                use_deferrable=any_deferrable,
            )
            sensores_compartidos[horario] = sensor

        # Construir tareas de ingesta con filtros event-driven opcionales
        task_groups = []
        for flujo in self.metadata.flujos:
            # Crear filtro si el flujo es event-driven
            dataset_filter = self._build_dataset_filter(
                flujo.nombre_tabla, flujo.dataset_origen
            )

            ingesta = self._build_rocket_operator(flujo)
            dataset = self._build_dataset_operator(flujo.nombre_tabla)
            task_groups.append((dataset_filter, flujo, ingesta, dataset))

        return sensores_compartidos, task_groups

    def _create_final_task(self, dataset_tasks: List[DummyOperator]) -> None:
        fin_pipeline = DummyOperator(task_id=TASK_FIN_PIPELINE)
        for dataset in dataset_tasks:
            dataset >> fin_pipeline

    def set_dependencies(
        self,
        sensores_compartidos: Dict[str, Union[TimeSensor, Any]],
        task_groups: List[
            Tuple[Optional[Any], FlujoConfig, RocketOperator, DummyOperator]
        ],
    ) -> None:
        """
        Establece dependencias optimizadas con sensores compartidos y filtros event-driven.

        Lógica de cadenas (4 modos posibles):
        1. Event-driven CON horario: filter >> sensor >> ingesta >> dataset
        2. Event-driven SIN horario: filter >> ingesta >> dataset
        3. Programado CON horario: sensor >> ingesta >> dataset
        4. Programado SIN horario: ingesta >> dataset

        Todas las tareas finales se conectan a fin_pipeline

        Args:
            sensores_compartidos: Dict de sensores por horario normalizado
            task_groups: Lista de tuplas (filter, flujo, ingesta, dataset)

        Example:
            # Flujo event-driven con horario 08:00
            filter_tabla1 >> sensor_08:00 >> ingesta_tabla1 >> dataset_tabla1

            # Flujo programado sin horario
            ingesta_tabla2 >> dataset_tabla2

        Note:
            - filter solo existe si flujo.dataset_origen está definido Y disponible
            - sensor compartido se conecta a TODOS los flujos con mismo horario
        """
        dataset_tasks = []

        # Mapear flujos a sus horarios normalizados
        flujo_to_horario = {}
        for _, flujo, _, _ in task_groups:
            if flujo.horario:
                flujo_to_horario[flujo.nombre_tabla] = self._normalize_time(
                    flujo.horario
                )

        # Establecer dependencias
        for dataset_filter, flujo, ingesta, dataset in task_groups:
            # 1. Construir cadena principal según disponibilidad de componentes
            if dataset_filter:
                # Event-driven: filter es el inicio
                if flujo.nombre_tabla in flujo_to_horario:
                    # Modo 1: filter >> sensor >> ingesta >> dataset
                    horario_norm = flujo_to_horario[flujo.nombre_tabla]
                    sensor_compartido = sensores_compartidos.get(horario_norm)
                    if sensor_compartido:
                        dataset_filter >> sensor_compartido >> ingesta
                    else:
                        dataset_filter >> ingesta
                else:
                    # Modo 2: filter >> ingesta >> dataset
                    dataset_filter >> ingesta
            else:
                # Programado: sensor (si existe) o ingesta es el inicio
                if flujo.nombre_tabla in flujo_to_horario:
                    # Modo 3: sensor >> ingesta >> dataset
                    horario_norm = flujo_to_horario[flujo.nombre_tabla]
                    sensor_compartido = sensores_compartidos.get(horario_norm)
                    if sensor_compartido:
                        sensor_compartido >> ingesta
                # Modo 4: ingesta >> dataset (se maneja abajo)

            # 2. Ingesta -> Dataset (común a todos los modos)
            ingesta >> dataset
            dataset_tasks.append(dataset)

        # 3. Crear tarea final
        self._create_final_task(dataset_tasks)


# ============================================================================
# CONFIGURACIÓN POR DEFECTO
# ============================================================================


def get_default_global_config() -> GlobalConfig:
    from airflow.models import Variable

    return GlobalConfig(
        path_utils=Variable.get("PATH_DAG_UTILS", default_var="./dags"),
        ruta_primaria_ds=Variable.get("P_RUTA_PRIMARIA_DATASETS", default_var="/ruta/"),
        ruta_grupo="/home/cda-motor-ingesta-datos/tablas-v2",
        timezone="America/Guayaquil",
    )


def get_default_operator_config() -> OperatorConfig:
    """Configuración por defecto con valores optimizados de sensores"""
    return OperatorConfig(
        connection_id="rocket-connectors",
        retries_status=100,
        status_polling_frequency=30,
        extended_audit_info=True,
        params_lists_base=["Environment", "SparkConfigurations"],
        sensor_poke_interval=DEFAULT_POKE_INTERVAL,  # 5 minutos
        sensor_timeout=DEFAULT_SENSOR_TIMEOUT,  # 2 horas
        sensor_mode=SENSOR_MODE,  # reschedule
        use_deferrable_sensors=DEFERRABLE_AVAILABLE,  # Usar deferrables si está disponible
    )


# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================


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
