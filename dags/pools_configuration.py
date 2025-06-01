# Configuração de Pools para controlar paralelismo
# Execute estes comandos no Airflow CLI ou Web UI

"""
Pools Configuration for Oracle to Snowflake Pipeline

Execute via Airflow CLI:
airflow pools set oracle_pool 5 "Oracle database connection pool"
airflow pools set snowflake_pool 10 "Snowflake warehouse connection pool"
airflow pools set s3_pool 15 "S3 operations pool"

Ou configure via Web UI:
Admin > Pools > Create
"""

from airflow.models import Pool
from airflow import settings

def setup_pools():
    """Configura pools programaticamente"""
    
    session = settings.Session()
    
    # Pool para conexões Oracle (limitado para não sobrecarregar o RDS)
    oracle_pool = Pool(
        pool='oracle_pool',
        slots=5,  # Máximo 5 conexões simultâneas
        description='Oracle RDS connection pool'
    )
    
    # Pool para Snowflake (pode ter mais conexões)
    snowflake_pool = Pool(
        pool='snowflake_pool', 
        slots=10,  # Máximo 10 operações simultâneas
        description='Snowflake warehouse connection pool'
    )
    
    # Pool para operações S3
    s3_pool = Pool(
        pool='s3_pool',
        slots=15,  # Máximo 15 operações S3 simultâneas
        description='S3 operations pool'
    )
    
    # Adiciona pools à sessão
    session.merge(oracle_pool)
    session.merge(snowflake_pool)
    session.merge(s3_pool)
    
    session.commit()
    session.close()

# Configuração de variáveis Airflow
AIRFLOW_VARIABLES = {
    'oracle_snowflake_config': {
        'oracle_connection_timeout': 300,
        'snowflake_warehouse': 'COMPUTE_WH',
        'snowflake_database': 'ANALYTICS_DB',
        's3_bucket': 'your-data-lake-bucket',
        's3_prefix': 'oracle-extracts',
        'max_rows_per_batch': 1000000,
        'retry_delay_minutes': 5,
        'email_alerts': ['data-team@company.com'],
        'slack_webhook': 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
    }
}