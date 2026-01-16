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
          ("SNOWFLAKE_PASSWORD", "bi-secrets-snowflake-elt-env", "password"),]

secrets = [secret.Secret(deploy_type="env", deploy_target=x[0], secret=x[1], key=x[2]) for x in config]

env_vars = {"SNOWFLAKE_ACCOUNT": "PITCHBOOK",
            "SNOWFLAKE_ROLE": "BI_ELT",
            "SNOWFLAKE_WAREHOUSE": "LOADING_WH",
            "SNOWFLAKE_DATABASE": "INBOUND_RAW_DEV" if os_env.lower() != "prod" else "INBOUND_RAW",
            "SNOWFLAKE_SCHEMA": "SIDETRADE",
            "OS_ENV": os_env}

image_name = "pipelines/snowflake/inbound_raw/sidetrade"
image_tag = get_dag_docker_tag(image_name)

dag_name = "bi-inbound_raw-sidetrade"
dag = create_dag(dag_name,
                 schedule="0 8 * * *",
                 timezone="America/Los_Angeles",
                 owner="naomi.du",
                 email=["airflow_failures-aaaahhdg6bhfktjuv77q5ugu74@pitchbook.slack.com"],
                 on_failure_callback=send_slack_failure_message(["naomi.du@pitchbook.com"]),
                 tags=["dept=EDP", "source=S3", "target=Snowflake"],
                 secrets=secrets,
                 extra_args={"email_on_retry": False, "service_account_name": "sidetrade"},
                 extra_env=env_vars)

final_task = get_finished_operator(dag=dag)


table_configs : list[dict] = [
    {
        "table": "Actions",
        "staging_models": ["Actions", "Actions_Current_State"],
        "snapshot": "actions_snapshot",
        "create_replica_views": True,
    },
    {
        "table": "Transaction",
        "staging_models": ["Transaction", "Transaction_Current_State"],
        "snapshot": "transaction_snapshot",
        "create_replica_views": True,
    },
    {
        "table": "KPI",
        "staging_models": ["KPI"],
        "snapshot": None,
        "create_replica_views": False,
    },
]

def create_sidetrade_pipeline(airflow_dag, table_config):
    """
    Creates a TaskGroup for one Sidetrade table:
    1. Extract recent files (past 2 days) from S3
    2. Run dbt staging models
    3. Optionally run snapshots & replica views
    """
    table = table_config["table"]
    staging_models = table_config.get("staging_models", [])
    snapshot = table_config.get("snapshot")
    create_replica_views = table_config.get("create_replica_views", False)

    with TaskGroup(group_id=f"{table}_elt", dag=airflow_dag) as tg:

        extract = KubernetesPodOperator(
            image=f"{ECR_REGISTRY}/{image_name}:{image_tag}",
            task_id=f"{table}_extract",
            email_on_failure=True,
            name=f"{table}_extract",
            cmds=["bash", "-c"],
            arguments=[
                f"""
                echo "==== Starting extraction for {table} ===="
                echo "Passing table name directly to main.py"
                python3 main.py "{table}"
                """
            ],
            container_resources=k8s.V1ResourceRequirements(requests={"memory": "256Mi"}),
            env_vars=env_vars,
            secrets=secrets,
            dag=airflow_dag,
        )

        dbt_staging = KubernetesPodOperator(
            image=f"{ECR_REGISTRY}/{image_name}:{image_tag}",
            task_id=f"{table}_dbt_staging",
            email_on_failure=True,
            name=f"{table}_dbt_staging",
            cmds=["bash", "-c"],
            arguments=[
                "cd dbt_project && "
                + " && ".join(
                    [f"dbt run --select models/staging/{m}.sql" for m in staging_models]
                )
            ],
            container_resources=k8s.V1ResourceRequirements(requests={"memory": "256Mi"}),
            env_vars=env_vars,
            secrets=secrets,
            dag=airflow_dag,
        )

        extract >> dbt_staging

        # -------------------------------------------------------------------
        # 3️⃣ Snapshot (if applicable)
        # -------------------------------------------------------------------
        if snapshot:
            dbt_snapshot = KubernetesPodOperator(
                image=f"{ECR_REGISTRY}/{image_name}:{image_tag}",
                task_id=f"{table}_dbt_snapshot",
                email_on_failure=True,
                name=f"{table}_dbt_snapshot",
                cmds=["bash", "-c"],
                arguments=[f"cd dbt_project && dbt snapshot --select {snapshot}"],
                container_resources=k8s.V1ResourceRequirements(requests={"memory": "256Mi"}),
                env_vars=env_vars,
                secrets=secrets,
                dag=airflow_dag,
            )
            dbt_staging >> dbt_snapshot

        # -------------------------------------------------------------------
        # 4️⃣ Replica views (only in prod)
        # -------------------------------------------------------------------
        if create_replica_views and os_env.lower() == "prod":
            replica_source = dbt_snapshot if snapshot else dbt_staging
            for model in staging_models + ([snapshot] if snapshot else []):
                replica = KubernetesPodOperator(
                    image=f"{ECR_REGISTRY}/{image_name}:{image_tag}",
                    task_id=f"{table}_replica_{model}",
                    email_on_failure=True,
                    name=f"{table}_replica_{model}",
                    cmds=["bash", "-c"],
                    arguments=[
                        f"""
                        cd dbt_project && \
                        dbt run-operation replica_view --args '{{"schema":"sidetrade", "x":"{model}"}}'
                        """
                    ],
                    container_resources=k8s.V1ResourceRequirements(requests={"memory": "256Mi"}),
                    env_vars=env_vars,
                    secrets=secrets,
                    dag=airflow_dag,
                )
                replica_source >> replica

    return tg

# ---------------------------------------------------------------------------
# DAG orchestration
# ---------------------------------------------------------------------------

for table_config in table_configs:
    task_group = create_sidetrade_pipeline(dag, table_config)
    task_group >> final_task
