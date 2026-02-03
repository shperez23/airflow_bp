import sys
import types
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
_API_UTILS_PATH = str(_REPO_ROOT / "dags" / "dags_utils")
if _API_UTILS_PATH not in sys.path:
    sys.path.insert(0, _API_UTILS_PATH)


def _ensure_factory_module_importable():
    if "factory_zr" in sys.modules:
        return

    try:
        __import__("factory_zr")
        return
    except Exception:
        pass

    airflow = types.ModuleType("airflow")

    class _DAG:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.dag_id = kwargs.get("dag_id")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    airflow.DAG = _DAG

    airflow_models_param = types.ModuleType("airflow.models.param")

    class _Param:
        def __init__(self, default=None, type=None, description=None):
            self.default = default
            self.type = type
            self.description = description

    airflow_models_param.Param = _Param

    airflow_operators_dummy = types.ModuleType("airflow.operators.dummy")

    class _BaseOp:
        def __init__(self, task_id=None, **kwargs):
            self.task_id = task_id
            self.kwargs = kwargs
            self.downstream = []

        def __rshift__(self, other):
            self.downstream.append(other)
            return other

    class _DummyOperator(_BaseOp):
        def __init__(self, task_id=None, outlets=None, retry_delay=None):
            super().__init__(task_id=task_id, outlets=outlets, retry_delay=retry_delay)
            self.outlets = outlets
            self.retry_delay = retry_delay

    airflow_operators_dummy.DummyOperator = _DummyOperator

    airflow_operators_python = types.ModuleType("airflow.operators.python")

    class _ShortCircuitOperator(_BaseOp):
        def __init__(self, task_id=None, python_callable=None, op_kwargs=None, **kwargs):
            super().__init__(
                task_id=task_id,
                python_callable=python_callable,
                op_kwargs=op_kwargs or {},
                **kwargs,
            )
            self.python_callable = python_callable
            self.op_kwargs = op_kwargs or {}

    airflow_operators_python.ShortCircuitOperator = _ShortCircuitOperator

    airflow_datasets = types.ModuleType("airflow.datasets")

    class _Dataset:
        def __init__(self, path):
            self.uri = path

    airflow_datasets.Dataset = _Dataset

    airflow_sensors_time_sensor = types.ModuleType("airflow.sensors.time_sensor")

    class _TimeSensor(_BaseOp):
        def __init__(
            self,
            task_id=None,
            target_time=None,
            poke_interval=None,
            timeout=None,
            mode=None,
        ):
            super().__init__(
                task_id=task_id,
                target_time=target_time,
                poke_interval=poke_interval,
                timeout=timeout,
                mode=mode,
            )
            self.target_time = target_time

    airflow_sensors_time_sensor.TimeSensor = _TimeSensor

    providers_stratio_rocket_operator_mod = types.ModuleType(
        "providers.stratio.rocket.operators.rocket_operator"
    )

    class _RocketOperator(_BaseOp):
        instances = []

        def __init__(self, **kwargs):
            task_id = kwargs.pop("task_id", None)
            super().__init__(task_id=task_id, **kwargs)
            self.__class__.instances.append(self)

    providers_stratio_rocket_operator_mod.RocketOperator = _RocketOperator

    pendulum_mod = types.ModuleType("pendulum")

    def _pendulum_datetime(year, month, day, tz=None):
        return (year, month, day, tz)

    pendulum_mod.datetime = _pendulum_datetime

    sys.modules.setdefault("airflow", airflow)
    sys.modules.setdefault("airflow.models.param", airflow_models_param)
    sys.modules.setdefault("airflow.operators.dummy", airflow_operators_dummy)
    sys.modules.setdefault("airflow.operators.python", airflow_operators_python)
    sys.modules.setdefault("airflow.datasets", airflow_datasets)
    sys.modules.setdefault("airflow.sensors.time_sensor", airflow_sensors_time_sensor)

    sys.modules.setdefault("providers", types.ModuleType("providers"))
    sys.modules.setdefault("providers.stratio", types.ModuleType("providers.stratio"))
    sys.modules.setdefault("providers.stratio.rocket", types.ModuleType("providers.stratio.rocket"))
    sys.modules.setdefault(
        "providers.stratio.rocket.operators",
        types.ModuleType("providers.stratio.rocket.operators"),
    )
    sys.modules.setdefault(
        "providers.stratio.rocket.operators.rocket_operator",
        providers_stratio_rocket_operator_mod,
    )

    sys.modules.setdefault("pendulum", pendulum_mod)

    __import__("factory_zr")


_ensure_factory_module_importable()

import factory_zr  # noqa: E402


class _BaseOp:
    def __init__(self, task_id=None, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs
        self.downstream = []

    def __rshift__(self, other):
        self.downstream.append(other)
        return other


class _DAG:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.dag_id = kwargs.get("dag_id")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Param:
    def __init__(self, default=None, type=None, description=None):
        self.default = default
        self.type = type
        self.description = description


class _Dataset:
    def __init__(self, uri):
        self.uri = uri


class _DummyOperator(_BaseOp):
    def __init__(self, task_id=None, outlets=None, retry_delay=None):
        super().__init__(task_id=task_id, outlets=outlets, retry_delay=retry_delay)
        self.outlets = outlets
        self.retry_delay = retry_delay


class _TimeSensor(_BaseOp):
    def __init__(
        self,
        task_id=None,
        target_time=None,
        poke_interval=None,
        timeout=None,
        mode=None,
    ):
        super().__init__(
            task_id=task_id,
            target_time=target_time,
            poke_interval=poke_interval,
            timeout=timeout,
            mode=mode,
        )
        self.target_time = target_time


class _ShortCircuitOperator(_BaseOp):
    def __init__(self, task_id=None, python_callable=None, op_kwargs=None, **kwargs):
        super().__init__(
            task_id=task_id,
            python_callable=python_callable,
            op_kwargs=op_kwargs or {},
            **kwargs,
        )
        self.python_callable = python_callable
        self.op_kwargs = op_kwargs or {}


class _RocketOperator(_BaseOp):
    instances = []

    def __init__(self, **kwargs):
        task_id = kwargs.pop("task_id", None)
        super().__init__(task_id=task_id, **kwargs)
        self.__class__.instances.append(self)


@pytest.fixture(autouse=True)
def _force_stubbed_airflow_objects(monkeypatch):
    monkeypatch.setattr(factory_zr, "DAG", _DAG, raising=False)
    monkeypatch.setattr(factory_zr, "Param", _Param, raising=False)
    monkeypatch.setattr(factory_zr, "Dataset", _Dataset, raising=False)
    monkeypatch.setattr(factory_zr, "DummyOperator", _DummyOperator, raising=False)
    monkeypatch.setattr(factory_zr, "TimeSensor", _TimeSensor, raising=False)
    monkeypatch.setattr(factory_zr, "ShortCircuitOperator", _ShortCircuitOperator, raising=False)
    monkeypatch.setattr(factory_zr, "RocketOperator", _RocketOperator, raising=False)
    return None


def _base_config(flujos):
    return {
        "dag_id": "dag_test_zr",
        "codigo_malla": "MALLA_001",
        "schedule": "0 1 * * *",
        "flujos": flujos,
        "tags": ["test"],
        "description": "desc",
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay_minutes": 1,
        "catchup": False,
        "max_active_runs": 1,
        "dagrun_timeout_minutes": 60,
        "start_year": 2025,
        "start_month": 1,
        "start_day": 1,
        "days_back_from": 2,
        "days_back_to": 1,
    }


def test_flujo_config_horario_empty_string_normalizes_to_none():
    flujo = factory_zr.FlujoConfig(
        workflow_name="wf",
        id_ingesta="ID1",
        talla="M",
        nombre_tabla="TABLA",
        horario="  ",
    )
    assert flujo.horario is None


def test_flujo_config_horario_invalid_format_raises():
    with pytest.raises(ValueError, match="Horario inválido"):
        factory_zr.FlujoConfig(
            workflow_name="wf",
            id_ingesta="ID1",
            talla="M",
            nombre_tabla="TABLA",
            horario="10",
        )


def test_flujo_config_dataset_origen_normalizes_and_drops_empty_values():
    flujo = factory_zr.FlujoConfig(
        workflow_name="wf",
        id_ingesta="ID1",
        talla="M",
        nombre_tabla="TABLA",
        dataset_origen="  ",
    )
    assert flujo.dataset_origen is None

    flujo_list = factory_zr.FlujoConfig(
        workflow_name="wf",
        id_ingesta="ID1",
        talla="M",
        nombre_tabla="TABLA",
        dataset_origen=["ORIGEN_A", "  ", "", "ORIGEN_B"],
    )
    assert flujo_list.dataset_origen == ["ORIGEN_A", "ORIGEN_B"]


def test_dagmetadata_converts_flujos_and_validates_days_back():
    config = _base_config(
        [
            {
                "workflow_name": "wf",
                "id_ingesta": "ID1",
                "talla": "M",
                "nombre_tabla": "TABLA",
            }
        ]
    )
    metadata = factory_zr.DAGMetadata(**config)
    assert metadata.flujos
    assert isinstance(metadata.flujos[0], factory_zr.FlujoConfig)

    config_bad = dict(config)
    config_bad["days_back_from"] = 0
    config_bad["days_back_to"] = 1
    with pytest.raises(ValueError, match="days_back_from"):
        factory_zr.DAGMetadata(**config_bad)


def test_global_config_invalid_timezone_raises():
    with pytest.raises(Exception):
        factory_zr.GlobalConfig(
            path_utils="./dags",
            ruta_primaria_ds="/ruta",
            ruta_grupo="/home/grupo",
            timezone="Invalid/Timezone",
        )


def test_factory_builds_sensor_when_horario_provided_and_normalizes_hh_mm():
    metadata = factory_zr.DAGMetadata(
        **_base_config(
            [
                {
                    "workflow_name": "wf",
                    "id_ingesta": "ID1",
                    "talla": "default",
                    "nombre_tabla": "TABLA",
                    "horario": "10:30",
                }
            ]
        )
    )
    global_config = factory_zr.GlobalConfig(
        path_utils="./dags",
        ruta_primaria_ds="/ruta",
        ruta_grupo="/home/grupo",
        timezone="America/Guayaquil",
    )
    operator_config = factory_zr.OperatorConfig(
        connection_id="rocket-connectors",
        retries_status=100,
        status_polling_frequency=30,
        extended_audit_info=True,
        params_lists_base=["Environment", "SparkConfigurations"],
    )

    factory = factory_zr.RocketDAGFactory(metadata, global_config, operator_config)
    task_groups = factory.build_tasks()

    assert len(task_groups) == 1
    _, sensor, rocket, dataset = task_groups[0]

    assert sensor is not None
    assert sensor.task_id.startswith(factory_zr.TASK_PREFIX_SENSOR)

    assert rocket.kwargs.get("paramsLists")[-1] == "SparkResources"

    extra = rocket.kwargs.get("extra_params")
    assert any(p["name"] == factory_zr.PARAM_ID_GRUPO and p["value"] == "ID1" for p in extra)

    assert dataset.kwargs.get("outlets")


def test_optional_extra_params_normalize_bool_and_include_only_non_none():
    flujo = factory_zr.FlujoConfig(
        workflow_name="wf",
        id_ingesta="ID1",
        talla="M",
        nombre_tabla="TABLA",
        p_enable_md5=True,
        p_fetch_size_assigned=None,
    )

    metadata = factory_zr.DAGMetadata(
        **_base_config(
            [
                {
                    "workflow_name": "wf",
                    "id_ingesta": "ID1",
                    "talla": "M",
                    "nombre_tabla": "TABLA",
                }
            ]
        )
    )
    global_config = factory_zr.GlobalConfig(
        path_utils="./dags",
        ruta_primaria_ds="/ruta",
        ruta_grupo="/home/grupo",
        timezone="America/Guayaquil",
    )
    operator_config = factory_zr.OperatorConfig(
        connection_id="rocket-connectors",
        retries_status=100,
        status_polling_frequency=30,
        extended_audit_info=True,
        params_lists_base=["Environment", "SparkConfigurations"],
    )

    factory = factory_zr.RocketDAGFactory(metadata, global_config, operator_config)

    optional = factory._build_optional_extra_params(flujo)
    assert {"name": factory_zr.PARAM_ENABLE_MD5, "value": "1"} in optional
    assert not any(p["name"] == factory_zr.PARAM_FETCH_SIZE_ASSIGNED for p in optional)


def test_factory_builds_dataset_filter_when_dataset_origen_provided():
    metadata = factory_zr.DAGMetadata(
        **_base_config(
            [
                {
                    "workflow_name": "wf",
                    "id_ingesta": "ID1",
                    "talla": "M",
                    "nombre_tabla": "DESTINO",
                    "dataset_origen": "ORIGEN",
                }
            ]
        )
    )
    global_config = factory_zr.GlobalConfig(
        path_utils="./dags",
        ruta_primaria_ds="/ruta",
        ruta_grupo="/home/grupo",
        timezone="America/Guayaquil",
    )
    operator_config = factory_zr.OperatorConfig(
        connection_id="rocket-connectors",
        retries_status=100,
        status_polling_frequency=30,
        extended_audit_info=True,
        params_lists_base=["Environment", "SparkConfigurations"],
    )

    factory = factory_zr.RocketDAGFactory(metadata, global_config, operator_config)
    task_groups = factory.build_tasks()

    assert len(task_groups) == 1
    dataset_filter, sensor, rocket, dataset = task_groups[0]

    assert dataset_filter is not None
    assert dataset_filter.task_id.startswith(factory_zr.TASK_PREFIX_FILTER)
    assert sensor is None
    assert rocket is not None
    assert dataset is not None

    schedule = factory._build_schedule()
    assert isinstance(schedule, list)
    assert schedule
    assert hasattr(schedule[0], "uri")


def test_factory_supports_multiple_dataset_origenes():
    metadata = factory_zr.DAGMetadata(
        **_base_config(
            [
                {
                    "workflow_name": "wf",
                    "id_ingesta": "ID1",
                    "talla": "M",
                    "nombre_tabla": "DESTINO",
                    "dataset_origen": ["ORIGEN_A", "ORIGEN_B"],
                }
            ]
        )
    )
    global_config = factory_zr.GlobalConfig(
        path_utils="./dags",
        ruta_primaria_ds="/ruta",
        ruta_grupo="/home/grupo",
        timezone="America/Guayaquil",
    )
    operator_config = factory_zr.OperatorConfig(
        connection_id="rocket-connectors",
        retries_status=100,
        status_polling_frequency=30,
        extended_audit_info=True,
        params_lists_base=["Environment", "SparkConfigurations"],
    )

    factory = factory_zr.RocketDAGFactory(metadata, global_config, operator_config)
    task_groups = factory.build_tasks()

    assert len(task_groups) == 1
    dataset_filter, sensor, rocket, dataset = task_groups[0]

    assert dataset_filter is not None
    assert dataset_filter.task_id.startswith(factory_zr.TASK_PREFIX_FILTER)
    assert sensor is None
    assert rocket is not None
    assert dataset is not None

    schedule = factory._build_schedule()
    assert isinstance(schedule, list)
    assert {dataset.uri for dataset in schedule} == {
        "/ruta/ORIGEN_A",
        "/ruta/ORIGEN_B",
    }


def test_should_run_for_any_dataset_matches_triggering_events():
    metadata = factory_zr.DAGMetadata(
        **_base_config(
            [
                {
                    "workflow_name": "wf",
                    "id_ingesta": "ID1",
                    "talla": "M",
                    "nombre_tabla": "DESTINO",
                }
            ]
        )
    )
    global_config = factory_zr.GlobalConfig(
        path_utils="./dags",
        ruta_primaria_ds="/ruta",
        ruta_grupo="/home/grupo",
        timezone="America/Guayaquil",
    )
    operator_config = factory_zr.OperatorConfig(
        connection_id="rocket-connectors",
        retries_status=100,
        status_polling_frequency=30,
        extended_audit_info=True,
        params_lists_base=["Environment", "SparkConfigurations"],
    )

    factory = factory_zr.RocketDAGFactory(metadata, global_config, operator_config)
    dataset_uris = ["/ruta/ORIGEN_A", "/ruta/ORIGEN_B"]

    assert factory._should_run_for_any_dataset(
        dataset_uris,
        triggering_dataset_events=["/ruta/ORIGEN_B"],
    )
    assert not factory._should_run_for_any_dataset(
        dataset_uris,
        triggering_dataset_events=["/ruta/OTRO"],
    )


def test_set_dependencies_creates_fin_pipeline_downstream_of_each_dataset():
    metadata = factory_zr.DAGMetadata(
        **_base_config(
            [
                {
                    "workflow_name": "wf_a",
                    "id_ingesta": "IDA",
                    "talla": "M",
                    "nombre_tabla": "A",
                },
                {
                    "workflow_name": "wf_b",
                    "id_ingesta": "IDB",
                    "talla": "M",
                    "nombre_tabla": "B",
                    "horario": "10:30",
                },
            ]
        )
    )
    global_config = factory_zr.GlobalConfig(
        path_utils="./dags",
        ruta_primaria_ds="/ruta",
        ruta_grupo="/home/grupo",
        timezone="America/Guayaquil",
    )
    operator_config = factory_zr.OperatorConfig(
        connection_id="rocket-connectors",
        retries_status=100,
        status_polling_frequency=30,
        extended_audit_info=True,
        params_lists_base=["Environment", "SparkConfigurations"],
    )

    factory = factory_zr.RocketDAGFactory(metadata, global_config, operator_config)
    task_groups = factory.build_tasks()

    factory.set_dependencies(task_groups)

    datasets = [dataset for _, _, _, dataset in task_groups]
    for dataset in datasets:
        assert dataset.downstream
        assert any(d.task_id == factory_zr.TASK_FIN_PIPELINE for d in dataset.downstream)
