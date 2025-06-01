import os

PIPELINE_CONFIG = {
    # Conexões
    'oracle_conn_id': 'oracle_conn',
    'snowflake_conn_id': 'snowflake_conn',
    
    # S3 Settings
    's3_bucket': os.getenv('S3_BUCKET', 'your-data-lake-bucket'),
    's3_prefix': os.getenv('S3_PREFIX', 'oracle-extracts'),
    
    # Pipeline Settings
    'chunk_size': int(os.getenv('CHUNK_SIZE', '100000')),
    'max_retries': int(os.getenv('MAX_RETRIES', '3')),
    'timeout_minutes': int(os.getenv('TIMEOUT_MINUTES', '60')),
    
    # Monitoring
    'slack_webhook': os.getenv('SLACK_WEBHOOK_URL'),
    'email_alerts': os.getenv('EMAIL_ALERTS', '').split(','),
    
    # File Format
    'file_format': 'csv',
    'compression': 'gzip',
    'csv_separator': ';',
    'csv_quoting': 'all'
}