from airflow.models import Variable
from airflow.providers.cncf.kubernetes import secret
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from kubernetes.client import models as k8s

from utils.callbacks import send_slack_failure_message
from utils.constants import ECR_REGISTRY
from utils.dags import create_dag, get_finished_operator
from utils.images import get_dag_docker_tag

os_env = Variable.get("ORCHESTRATOR_ENV")

config = [("SNOWFLAKE_USER", "bi-secrets-snowflake-elt-env", "username"),
          ("SNOWFLAKE_PASSWORD", "bi-secrets-snowflake-elt-env", "password"),
          ("NEXTROLL_API_KEY", "bi-secrets-rollworks", "api_key"),
          ("NEXTROLL_API_SECRET", "bi-secrets-rollworks", "api_secret")]

secrets = [secret.Secret(deploy_type="env", deploy_target=x[0], secret=x[1], key=x[2]) for x in config]

af_schema = "ROLLWORKS"

env_vars = {"SNOWFLAKE_ACCOUNT": "pitchbook",
            "SNOWFLAKE_ROLE": "BI_ELT",
            "SNOWFLAKE_WAREHOUSE": "LOADING_WH",
            "SNOWFLAKE_DATABASE": "INBOUND_RAW_DEV" if os_env.lower() != "prod" else "INBOUND_RAW",
            "SNOWFLAKE_SCHEMA": af_schema,
            "OS_ENV": os_env}

image_name = "pipelines/snowflake/inbound_raw/rollworks"
image_tag = get_dag_docker_tag(image_name)

dag_name = "bi-inbound_raw-rollworks"
dag = create_dag(dag_name,
                 schedule="0 10 * * *",
                 owner="naomi.du",
                 email=["airflow_failures-aaaahhdg6bhfktjuv77q5ugu74@pitchbook.slack.com",
                        "naomi.du@pitchbook.com"],
                 on_failure_callback=send_slack_failure_message(["naomi.du@pitchbook.com"]),
                 tags=["dept=BI", f"source={af_schema}", "target=Snowflake"],
                 secrets=secrets,
                 extra_args={"email_on_retry": False},
                 extra_env=env_vars,
                 concurrency=2)

final_task = get_finished_operator(dag=dag)

with TaskGroup("rollworks_extract_load", dag=dag) as pipeline:
    rollworks_extract_task = KubernetesPodOperator(
                image=f"{ECR_REGISTRY}/{image_name}:{image_tag}",
                task_id="rollworks_extract", # Airflow task identifier
                email_on_failure=True,
                name="rollworks_extract", # Kubernetes pod name
                arguments=["/core/main.py"],
                container_resources=k8s.V1ResourceRequirements(requests={"memory": "256Mi"}),
                env_vars=env_vars,
                dag=dag
    )
    rollworks_load_task = KubernetesPodOperator(
                image=f"{ECR_REGISTRY}/{image_name}:{image_tag}",
                task_id="rollworks_dbt_load", # Airflow task identifier
                email_on_failure=True,
                name="rollworks_dbt_load", # Kubernetes pod name
                cmds=["bash", "-c"],
                arguments=["cd /core/dbt_project && dbt run --select models/staging"],
                container_resources=k8s.V1ResourceRequirements(requests={"memory": "256Mi"}),
                env_vars=env_vars,
                dag=dag
    )
    rollworks_extract_task >> rollworks_load_task
    # Only creates the replica views if the env is prod and not dev
    if os_env.lower() == "prod":
        table = "ad_spend"
        rollworks_replica_views_task = KubernetesPodOperator(
            image=f"{ECR_REGISTRY}/{image_name}:{image_tag}",
            task_id=f"the_den_dbt_replica_views_{table}",
            email_on_failure=True,
            name=f"the_den_dbt_replica_views_{table}",
            cmds=["bash", "-c"],
            arguments=[
                f"""
                cd /core/dbt_project && dbt run-operation replica_view --args '{{"schema":"{af_schema}", "x":"{table}"}}'
                """
            ],
            container_resources=k8s.V1ResourceRequirements(requests={"memory": "256Mi"}),
            env_vars=env_vars,
            dag=dag
        )
        rollworks_load_task >> rollworks_replica_views_task
pipeline >> final_task
