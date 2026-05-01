from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models.param import Param
from datetime import datetime
from docker.types import Mount
import os

DBT_IMAGE = "ghcr.io/dbt-labs/dbt-postgres:latest"

#HOST_BASE_PATH = "C:\\projects\\tenant-controller\\tenants"
HOST_BASE_PATH = os.environ.get("HOST_BASE_PATH", "/opt/shared/tenants")
CONTAINER_BASE_PATH = "/dbt/tenants"

default_args = {"owner": "airflow"}

TENANT_EXPR = "{{ dag_run.conf.get('tenant', params.tenant) }}"

with DAG(
    dag_id="dbt_tenant_ephemeral_modified",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "tenant": Param(
            "customer_000",
            type="string",
            description="Tenant folder to run dbt against"
        )
    },
    tags=["dbt", "docker-operator"],
) as dag:

    def dbt_task(task_id, dbt_cmd):
        return DockerOperator(
            task_id=task_id,
            image=DBT_IMAGE,


            command=f"{dbt_cmd} --project-dir {CONTAINER_BASE_PATH}/{TENANT_EXPR} --profiles-dir {CONTAINER_BASE_PATH}/{TENANT_EXPR}",

            docker_url="unix://var/run/docker.sock",
            network_mode="backend",
            auto_remove=True,
            mount_tmp_dir=False,

            mounts=[
                Mount(
                    source=HOST_BASE_PATH,
                    target="/dbt/tenants",
                    type="bind"
                )
            ],
        )

    deps = dbt_task("dbt_deps", "deps")
    run_staging = dbt_task("staging", "run --select staging")
    snapshot = dbt_task("snapshot", "snapshot")
    run_marts = dbt_task("marts", "run --select marts")
    test = dbt_task("dbt_test", "test")

    deps >> run_staging >> snapshot >> run_marts >> test
