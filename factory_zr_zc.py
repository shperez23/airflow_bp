"""
Framework POO para construcción de DAGs dinámicos de Airflow en ZC (event-driven).
Basado en factory_zr con disparo por datasets.
Principios: Clean Code, SOLID, DRY.
SIN valores por defecto ni datos quemados.
"""

from datetime import timedelta
from typing import Dict, List, Optional

from airflow import DAG
from airflow.datasets import Dataset
import pendulum

from factory_zr import (
    DAGMetadata,
    GlobalConfig,
    OperatorConfig,
    RocketDAGFactory,
    get_default_global_config,
    get_default_operator_config,
)


class DatasetTriggeredRocketDAGFactory(RocketDAGFactory):
    """
    DAG Factory que dispara por actualización de datasets.
    Reutiliza las tareas de RocketDAGFactory pero programa el DAG por Dataset.
    """

    def _build_dataset_schedule(self) -> Optional[List[Dataset]]:
        datasets = [
            Dataset(self.build_dataset_path(flujo.nombre_tabla))
            for flujo in self.metadata.flujos
        ]
        return datasets or None

    def create_dag(self) -> DAG:
        self.dag = DAG(
            dag_id=self.metadata.dag_id,
            default_args=self._build_default_args(),
            description=self.metadata.description,
            schedule_interval=self._build_dataset_schedule(),
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
    factory = DatasetTriggeredRocketDAGFactory(metadata, global_config, operator_config)
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
