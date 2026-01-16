from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes import secret
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from kubernetes.client import models as k8s
from utils.constants import ECR_REGISTRY
from utils.dags import create_dag, get_finished_operator, create_secret_volumes
from utils.images import get_dag_docker_tag

os_env = Variable.get("ORCHESTRATOR_ENV")

secrets = create_secret_volumes(['bi_secrets_aws.yaml'])

env_secrets = [('SVC_BOT_BI_ELT_PASSWORD', 'bi-secrets-snowflake-elt-env', 'password'),
               ('API_USERNAME', 'bi-secrets-rps-api', 'email'),
               ('API_PASSWORD', 'bi-secrets-rps-api', 'password'),
               ('SNOWFLAKE_USER', 'bi-secrets-snowflake-elt-env', 'username'),
               ('SNOWFLAKE_PASSWORD', 'bi-secrets-snowflake-elt-env', 'password'),
               ('CAPIQ_USER', 'bi-secrets-capiq', 'source_ftp_username'),
               ('CAPIQ_PASSWORD', 'bi-secrets-capiq', 'source_ftp_pwd'),
               ('CAPIQ_HOST', 'bi-secrets-capiq', 'source_ftp_host'),
               ('CAPIQ_PORT', 'bi-secrets-capiq', 'source_ftp_port'),
               ('PBRC_HOST', 'bi-secrets-pbrc', 'host'),
               ('PBRC_USER', 'bi-secrets-pbrc', 'login'),
               ('PBRC_PASSWORD', 'bi-secrets-pbrc', 'password'),
               ('BLOOMBERG_USER', 'bi-secrets-bloomberg', 'username'),
               ('BLOOMBERG_PASSWORD', 'bi-secrets-bloomberg', 'password'),
               ('BLOOMBERG_HOST', 'bi-secrets-bloomberg', 'host'),
               ('BLOOMBERG_PORT', 'bi-secrets-bloomberg', 'port')]

[secrets.append(secret.Secret(deploy_type='env', deploy_target=x[0], secret=x[1], key=x[2])) for x in env_secrets]

env_vars = {'SNOWFLAKE_ACCOUNT': 'pitchbook',
            'SNOWFLAKE_ROLE': 'BI_ELT',
            'SNOWFLAKE_WAREHOUSE': 'LOADING_WH',
            'SNOWFLAKE_DATABASE': 'INBOUND_RAW_DEV' if os_env.lower() != 'prod' else 'INBOUND_RAW',
            'SNOWFLAKE_SCHEMA': 'MIERS',
            'OS_ENV': os_env
            }

image_name = 'pipelines/snowflake/inbound_raw/miers'
image_tag = get_dag_docker_tag(image_name)

miers_common_image = 'pipelines/pipelines-snowflake/database/business_intelligence/schema/miers/miers_common'
miers_image_tag = get_dag_docker_tag(miers_common_image)

rps_common_image = 'pipelines/snowflake/inbound_raw/rps_morningstar_api'
rps_common_image_tag = get_dag_docker_tag(rps_common_image)

dag_name = 'bi-inbound_raw-miers'
dag = create_dag(dag_name,
                 schedule='0 4 * * *',
                 timezone='America/Los_Angeles',
                 owner='naomi.du',
                 email=['airflow_failures-aaaahhdg6bhfktjuv77q5ugu74@pitchbook.slack.com'],
                 tags=['dept=BI', 'source=MIERS', 'target=Snowflake'],
                 secrets=secrets,
                 extra_args={'email_on_retry': False, 'service_account_name': 'bi-dms-replication'},
                 extra_env=env_vars,
                 retries=1,
                 concurrency=4)

final_task = get_finished_operator(dag=dag)

miers_inbound_tasks = ['factset', 'capiq', 'pbrc', 'bloomberg', 'refinitiv']

with TaskGroup('miers_extract_load', dag=dag) as pipeline:

    miers_base_dbt_model = KubernetesPodOperator(
        image=f'{ECR_REGISTRY}/{image_name}:{image_tag}',
        task_id='miers_base_dbt_model',
        email_on_failure=False,
        name='base_miers_dbt',
        cmds=['bash', '-c'],
        arguments=[f"cd /core/dbt_project && dbt run"],
        container_resources=k8s.V1ResourceRequirements(requests={'memory': '2Gi'}),
        env_vars=env_vars,
        dag=dag
    )

    miers_readership_dbt_model = KubernetesPodOperator(
        image=f'{ECR_REGISTRY}/{miers_common_image}:{miers_image_tag}',
        task_id='miers_readership_dbt_model',
        email_on_failure=False,
        name=f'base_readership_models',
        cmds=['bash', '-c'],
        arguments=[
            "cd /core/dbt_project && dbt run"],
        container_resources=k8s.V1ResourceRequirements(requests={'memory': '2Gi'}),
        dag=dag
    )

    rps_api_ingestion = KubernetesPodOperator(
        image=f'{ECR_REGISTRY}/{rps_common_image}:{rps_common_image_tag}',
        task_id='rps_api_ingestion',
        email_on_failure=True,
        name=f'bi_pipe.rps_api.ingestion',
        arguments=["/core/pipeline.py"],
        container_resources=k8s.V1ResourceRequirements(requests={'memory': '1Gi'}),
        dag=dag
    )

    rps_dbt_base_model = KubernetesPodOperator(
        image=f'{ECR_REGISTRY}/{rps_common_image}:{rps_common_image_tag}',
        task_id='rps_dbt_base_model_run',
        email_on_failure=True,
        name=f'bi_pipe.rps_api.dbt',
        cmds=['bash', '-c'],
        arguments=[f"cd /core/dbt_project && dbt run --select models/base"],
        container_resources=k8s.V1ResourceRequirements(requests={'memory': '1Gi'}),
        dag=dag
    )

    rps_dbt_intermediate_model = KubernetesPodOperator(
        image=f'{ECR_REGISTRY}/{rps_common_image}:{rps_common_image_tag}',
        task_id='rps_dbt_intermediate_model_run',
        email_on_failure=True,
        name=f'bi_pipe.rps_api.dbt',
        cmds=['bash', '-c'],
        arguments=[f"cd /core/dbt_project && dbt run --select models/intermediate"],
        container_resources=k8s.V1ResourceRequirements(requests={'memory': '1Gi'}),
        dag=dag
    )

    rps_dbt_marts_model = KubernetesPodOperator(
        image=f'{ECR_REGISTRY}/{rps_common_image}:{rps_common_image_tag}',
        task_id='rps_dbt_marts_model_run',
        email_on_failure=True,
        name=f'bi_pipe.rps_api.dbt',
        cmds=['bash', '-c'],
        arguments=[f"cd /core/dbt_project && dbt run --select models/marts"],
        container_resources=k8s.V1ResourceRequirements(requests={'memory': '1Gi'}),
        dag=dag
    )

    for task_id in miers_inbound_tasks:
        miers_pipeline_task = KubernetesPodOperator(
            image=f'{ECR_REGISTRY}/{image_name}:{image_tag}',
            task_id=task_id,
            email_on_failure=False,
            name=f'stg_miers_{task_id}',
            arguments=[f'/core/pipeline/stg_miers_{task_id}.py'],
            container_resources=k8s.V1ResourceRequirements(requests={'memory': '2Gi'}),
            env_vars=env_vars,
            dag=dag
        )
        # Set dbt_model as downstream for all tasks in the pipeline TaskGroup
        miers_pipeline_task >> miers_base_dbt_model

rps_api_ingestion >> rps_dbt_base_model >> rps_dbt_intermediate_model >> miers_base_dbt_model >> miers_readership_dbt_model  >> rps_dbt_marts_model
pipeline >> final_task
