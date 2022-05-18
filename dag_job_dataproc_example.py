# [START import] Importações
import datetime
from importlib.metadata import metadata
from airflow import DAG
from airflow.utils.dates import days_ago
from os import getenv
from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
# [END import]

# [START variables] Definição de variáveis
gcp_project_id = models.Variable.get('gcp_project_id') # usa a key cadastrada diretamente no Airflow, o value aqui seria o project ID
dataproc_cluster_name = getenv('dataproc_cluster_name', 'NOME-DO-SEU-CLUSTER')
pyspark_job_script = getenv('pyspark_job_script', 'gs://SEU-BUCKET-AQUI/SEU-SCRIPT.py') # URI de onde o script do job está
service_account = 'SUA-SERVICE-ACCOUNT@SEU-PROJETO-ID.iam.gserviceaccount.com'
# [END variables]

# [START default_args]
default_args = {
    'start_date': days_ago(1), # Diz ao Airflow para começar um dia atrás. Sendo assim, assim que a DAG for carregada, ela irá iniciar imediatamente
    'project_id': gcp_project_id,
    'region': 'us-central1',
    'retries': 1,
    'service_account': service_account,
    'service_account_scopes': service_account,
}
# [END default_args]

## [START DAG] Define a DAG (directed acyclic graph) das tasks.
with models.DAG(
    'dag_start_dataproc_create_submit_delete',
    schedule_interval='@daily', # Intervalo de execução - cron
    default_args=default_args,
) as dag_composer:

# [START dummies]
    starting_task = DummyOperator(
        task_id='start_task'
    )
    ending_task = DummyOperator(
        task_id='end_task'
    )
# [END dummies]

# [START cluster] criar o cluster spark dataproc
# Passe as configurações conforme sua necessidade de cluster
    CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
        project_id=gcp_project_id,
        master_machine_type='n1-standard-4',
        worker_machine_type='n1-standard-4',
        num_workers=2,
        image_version='1.5.61-debian10',
        enable_component_gateway=True,
        scopes='cloud-platform'
    ).make()

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_config=CLUSTER_GENERATOR_CONFIG,
        cluster_name=dataproc_cluster_name,
        use_if_exists=True
    )
# [END cluster] criar o cluster spark dataproc

# [START job] rodar o job pyspark
    job_start_dataproc_submit = {
        'reference': {'project_id': gcp_project_id},
        'placement': {'cluster_name': dataproc_cluster_name},
        'pyspark_job': {'main_python_file_uri': pyspark_job_script},
    }
    start_dataproc_submit = DataprocSubmitJobOperator(
        task_id='start_dataproc_job',
        job=job_start_dataproc_submit,
    )
# [END job] rodar o job pyspark

# [START delete cluster] matar o cluster
    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        cluster_name=dataproc_cluster_name,
    )
# [END delete] matar o cluster

# [START move gcs] organizar bucket, move arquivos
    move_files = GCSToGCSOperator(
        task_id='move_files',
        source_bucket='SEU-BUCKET-DE-ORIGEM-AQUI',
        source_object='*.txt', # Objeto a ser movido
        delimiter='.txt', # Delimitador do(s) objeto(s) a ser(em) movido(s)
        destination_bucket='SEU-BUCKET-DE-DESTINO-AQUI', # Se o bucket de destino for None, ele irá para o bucket de origem
        destination_object='SUBPASTA/',
        move_object=True, # True Move ao invés de copiar, False copia para o bucket informado
    )
# [END move gcs] organizar bucket, move arquivos
## [END DAG]

# [START sequence] Definição da sequência das taskes
starting_task >> create_dataproc_cluster 
create_dataproc_cluster >> start_dataproc_submit 
start_dataproc_submit >> delete_dataproc_cluster
delete_dataproc_cluster >> move_files
move_files >> ending_task
# [END sequence]