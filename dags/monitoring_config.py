from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import boto3
import json

def send_slack_notification(**context):
    """Envia notificação para Slack em caso de falha"""
    
    # Configuração do webhook do Slack
    slack_webhook_url = "YOUR_SLACK_WEBHOOK_URL"
    
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    
    message = {
        "text": f"🚨 Pipeline Oracle → Snowflake Failed",
        "attachments": [
            {
                "color": "danger",
                "fields": [
                    {
                        "title": "DAG",
                        "value": dag_id,
                        "short": True
                    },
                    {
                        "title": "Task",
                        "value": task_instance.task_id,
                        "short": True
                    },
                    {
                        "title": "Execution Date",
                        "value": execution_date.strftime('%Y-%m-%d %H:%M:%S'),
                        "short": True
                    },
                    {
                        "title": "Log URL",
                        "value": task_instance.log_url,
                        "short": False
                    }
                ]
            }
        ]
    }
    
    # Enviar via requests (adicionar requests no requirements.txt)
    import requests
    requests.post(slack_webhook_url, json=message)

def create_cloudwatch_metrics(**context):
    """Cria métricas customizadas no CloudWatch"""
    
    cloudwatch = boto3.client('cloudwatch')
    
    # Métricas do pipeline
    execution_date = context['execution_date']
    dag_id = context['dag'].dag_id
    
    # Métrica de execução bem-sucedida
    cloudwatch.put_metric_data(
        Namespace='Airflow/ETL',
        MetricData=[
            {
                'MetricName': 'PipelineSuccess',
                'Dimensions': [
                    {
                        'Name': 'DAG',
                        'Value': dag_id
                    }
                ],
                'Value': 1,
                'Unit': 'Count'
            },
            {
                'MetricName': 'PipelineExecutionTime',
                'Dimensions': [
                    {
                        'Name': 'DAG', 
                        'Value': dag_id
                    }
                ],
                'Value': (context['task_instance'].end_date - context['task_instance'].start_date).total_seconds(),
                'Unit': 'Seconds'
            }
        ]
    )

def validate_data_freshness(**context):
    """Valida se os dados estão atualizados"""
    
    from snowflake_loader import SnowflakeLoader
    from datetime import datetime, timedelta
    
    loader = SnowflakeLoader(connection_id='snowflake_conn')
    
    # Verifica cada schema/tabela
    schemas_to_check = ['schema1', 'schema2', 'schema3']
    
    issues = []
    
    for schema in schemas_to_check:
        
        # Query para verificar dados recentes
        freshness_sql = f"""
        SELECT 
            table_name,
            MAX(_EXTRACTION_TIMESTAMP) as last_update,
            DATEDIFF(hour, MAX(_EXTRACTION_TIMESTAMP), CURRENT_TIMESTAMP()) as hours_since_update
        FROM information_schema.tables t
        JOIN {schema}.* ON 1=1
        WHERE table_schema = '{schema.upper()}'
        GROUP BY table_name
        HAVING hours_since_update > 25  -- Alerta se não atualizou nas últimas 25h
        """
        
        try:
            stale_tables = loader.snowflake_hook.get_records(freshness_sql)
            
            for table_info in stale_tables:
                issues.append({
                    'schema': schema,
                    'table': table_info[0],
                    'last_update': table_info[1],
                    'hours_stale': table_info[2]
                })
        except Exception as e:
            issues.append({
                'schema': schema,
                'error': str(e)
            })
    
    if issues:
        # Envia alerta
        context['task_instance'].xcom_push(key='data_freshness_issues', value=issues)
        raise ValueError(f"Data freshness issues detected: {issues}")
    
    return "All data is fresh"

# DAG de monitoramento separado
from airflow import DAG
from datetime import datetime, timedelta

monitoring_dag = DAG(
    'oracle_snowflake_monitoring',
    default_args={
        'owner': 'data-team',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Monitor Oracle to Snowflake pipeline',
    schedule_interval='0 */4 * * *',  # A cada 4 horas
    catchup=False,
    tags=['monitoring', 'oracle', 'snowflake']
)

# Task de validação de atualização dos dados
data_freshness_check = PythonOperator(
    task_id='check_data_freshness',
    python_callable=validate_data_freshness,
    dag=monitoring_dag
)

# Task de métricas CloudWatch
metrics_task = PythonOperator(
    task_id='create_cloudwatch_metrics',
    python_callable=create_cloudwatch_metrics,
    dag=monitoring_dag
)

# Task de alerta (executada apenas em caso de falha)
alert_task = PythonOperator(
    task_id='send_failure_alert',
    python_callable=send_slack_notification,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=monitoring_dag
)

# Fluxo do DAG de monitoramento
data_freshness_check >> metrics_task
[data_freshness_check, metrics_task] >> alert_task

# Configuração de alertas por email
def setup_email_alerts():
    """Configuração de alertas por email"""
    
    email_alert = EmailOperator(
        task_id='email_alert',
        to=['data-team@company.com'],
        subject='Oracle to Snowflake Pipeline Alert',
        html_content="""
        <h3>Pipeline Oracle → Snowflake - Status Alert</h3>
        <p><strong>DAG:</strong> {{ dag.dag_id }}</p>
        <p><strong>Execution Date:</strong> {{ ds }}</p>
        <p><strong>Task:</strong> {{ task.task_id }}</p>
        <p><strong>Status:</strong> {{ task_instance.state }}</p>
        <p><strong>Log URL:</strong> <a href="{{ task_instance.log_url }}">View Logs</a></p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED
    )
    
    return email_alert

# Configuração de pools para controlar paralelismo