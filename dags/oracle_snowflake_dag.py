from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import json

# Configuração dos schemas e tabelas
SCHEMAS_CONFIG = {
    'SCHEMA1': ['TABLE1', 'TABLE2', 'TABLE3'],
    'SCHEMA2': ['TABLE4', 'TABLE5'],
    'SCHEMA3': ['TABLE6', 'TABLE7', 'TABLE8']
}

# Configurações S3
S3_BUCKET = 'your-data-lake-bucket'
S3_PREFIX = 'oracle-extracts'

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 5),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 6,
    'retry_delay': timedelta(minutes=10),
    'max_active_runs': 1
}

dag = DAG(
    'oracle_to_snowflake_pipeline',
    default_args=default_args,
    description='Extract data from Oracle RDS to Snowflake',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['oracle', 'snowflake']
)

def extract_oracle_data(**context):
    """Extrai dados do Oracle e salva no S3"""
    from oracle_extractor import OracleExtractor
    
    schema = context['params']['schema']
    table = context['params']['table']
    execution_date = context['execution_date']
    
    extractor = OracleExtractor(
        connection_id='oracle_conn',
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX
    )
    
    # Extração incremental baseada em timestamp
    file_path = extractor.extract_table_incremental(
        schema=schema,
        table=table,
        execution_date=execution_date
    )
    
    # Passa o caminho do arquivo para próxima task
    context['task_instance'].xcom_push(
        key=f'{schema}_{table}_file_path', 
        value=file_path
    )
    
    return file_path

def load_csv_to_snowflake(**context):
    """Carrega dados CSV do S3 para Snowflake"""
    from snowflake_loader import SnowflakeLoader
    
    schema = context['params']['schema']
    table = context['params']['table']
    
    # Recupera caminho do arquivo do XCom
    file_path = context['task_instance'].xcom_pull(
        key=f'{schema}_{table}_file_path'
    )
    
    if not file_path:
        context['task_instance'].log.warning(f"No file path found for {schema}.{table}")
        return "No data to load"
    
    loader = SnowflakeLoader(connection_id='snowflake_conn')
    
    # Se file_path é uma lista (chunks), carrega múltiplos arquivos
    if isinstance(file_path, list):
        # Para múltiplos chunks, usa padrão wildcard
        base_path = file_path[0].rsplit('_chunk_', 1)[0]
        pattern_path = f"{base_path}_chunk_*.csv.gz"
        
        result = loader.copy_multiple_csv_files(
            s3_path_pattern=pattern_path,
            target_schema=schema.lower(),
            target_table=table.lower()
        )
    else:
        # Arquivo único
        result = loader.copy_csv_from_s3(
            s3_path=file_path,
            target_schema=schema.lower(),
            target_table=table.lower()
        )
    
    # Validação opcional
    try:
        validation_result = loader.validate_csv_data_quality(
            schema_name=schema.lower(),
            table_name=table.lower(),
            expected_min_rows=1
        )
        context['task_instance'].log.info(f"Data quality validation: {validation_result}")
    except Exception as e:
        context['task_instance'].log.warning(f"Data quality validation failed: {e}")
    
    return result

# Task de início
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Task groups por schema
for schema, tables in SCHEMAS_CONFIG.items():
    
    with TaskGroup(group_id=f'process_{schema.lower()}', dag=dag) as schema_group:
        
        schema_start = DummyOperator(task_id=f'start_{schema.lower()}')
        schema_end = DummyOperator(task_id=f'end_{schema.lower()}')
        
        for table in tables:
            
            # Extração do Oracle
            extract_task = PythonOperator(
                task_id=f'extract_{schema.lower()}_{table.lower()}',
                python_callable=extract_oracle_data,
                params={'schema': schema, 'table': table},
                pool='oracle_pool'  # Pool para limitar conexões simultâneas
            )
            
            # Carga no Snowflake
            load_task = PythonOperator(
                task_id=f'load_{schema.lower()}_{table.lower()}',
                python_callable=load_csv_to_snowflake,
                params={'schema': schema, 'table': table},
                pool='snowflake_pool'
            )
            
            # Fluxo: início → extração → carga → fim
            schema_start >> extract_task >> load_task >> schema_end
    
    # Conecta com task principal
    start_task >> schema_group

# Task de validação final
validate_task = PythonOperator(
    task_id='validate_pipeline',
    python_callable=lambda: print("Pipeline completed successfully"),
    dag=dag
)

# Conecta todos os grupos à validação final
for schema in SCHEMAS_CONFIG.keys():
    dag.get_task_group(f'process_{schema.lower()}') >> validate_task