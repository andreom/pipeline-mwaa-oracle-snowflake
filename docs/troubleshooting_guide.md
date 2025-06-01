# Pipeline Oracle → Snowflake - Guia de Troubleshooting

## 🚨 Problemas Comuns e Soluções

### 1. Problemas de Conexão Oracle

#### Erro: `COPY command failed`
**Causa:** Problemas no formato dos dados ou permissões S3
**Soluções:**
```sql
-- Verificar erros detalhados do COPY
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'YOUR_TABLE',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));

-- Validar formato do arquivo Parquet
LIST @your_stage/path/to/file.parquet;

-- Testar COPY com ON_ERROR = 'CONTINUE' para ver erros específicos
COPY INTO schema.table FROM @stage/file.parquet
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE'
VALIDATION_MODE = 'RETURN_ERRORS';
```

#### Erro: `Warehouse timeout`
**Causa:** Warehouse muito pequeno ou suspenso
**Soluções:**
```sql
-- Verificar status do warehouse
SHOW WAREHOUSES;

-- Redimensionar warehouse temporariamente
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'LARGE';

-- Configurar auto-resume
ALTER WAREHOUSE COMPUTE_WH SET AUTO_RESUME = TRUE;
```

#### Erro: `Object does not exist` (Stage/Table)
**Soluções:**
```sql
-- Criar stage manualmente se necessário
CREATE STAGE IF NOT EXISTS oracle_stage
URL = 's3://your-bucket/oracle-extracts/'
FILE_FORMAT = (TYPE = PARQUET);

-- Verificar permissões no stage
DESCRIBE STAGE oracle_stage;

-- Listar arquivos no stage
LIST @oracle_stage;
```

### 4. Problemas S3/AWS

#### Erro: `Access Denied` no S3
**Causa:** Permissões IAM insuficientes
**Soluções:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket",
                "arn:aws:s3:::your-bucket/*"
            ]
        }
    ]
}
```

#### Erro: `NoCredentialsError`
**Soluções:**
```bash
# Verificar role do MWAA
aws sts get-caller-identity

# Verificar se role tem permissões corretas
aws iam get-role-policy --role-name your-mwaa-role --policy-name policy-name

# Verificar conexão AWS no Airflow
# Connection Type: Amazon Web Services
# Extra: {"region_name": "us-east-1"}
```

### 5. Problemas no Airflow/MWAA

#### DAG não aparece na UI
**Soluções:**
```bash
# Verificar sintaxe do DAG
python /opt/airflow/dags/oracle_to_snowflake_dag.py

# Verificar logs do MWAA
aws logs describe-log-groups --log-group-name-prefix airflow-your-environment

# Verificar se requirements.txt está correto
aws s3 ls s3://your-mwaa-bucket/requirements.txt
```

#### Task falha com `Import Error`
**Soluções:**
```python
# Verificar se módulos estão no requirements.txt
# Adicionar ao requirements.txt:
cx-Oracle==8.3.0
snowflake-connector-python==3.5.0

# Aguardar atualização do ambiente MWAA (10-20 min)
aws mwaa get-environment --name your-environment
```

#### Pool slots esgotados
**Soluções:**
```python
# Aumentar slots do pool via UI ou CLI
airflow pools set oracle_pool 10 "Increased Oracle pool"

# Ou ajustar paralelismo no DAG
default_args = {
    'max_active_runs': 1,
    'max_active_tasks': 5
}
```

### 6. Problemas de Dados

#### Dados duplicados no Snowflake
**Causa:** Múltiplas execuções ou falha no controle incremental
**Soluções:**
```sql
-- Verificar duplicatas
SELECT column1, column2, COUNT(*) 
FROM schema.table 
GROUP BY column1, column2 
HAVING COUNT(*) > 1;

-- Implementar MERGE em vez de INSERT
MERGE INTO target_table t
USING source_table s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;

-- Adicionar constraint unique se necessário
ALTER TABLE schema.table ADD CONSTRAINT uk_table UNIQUE (id);
```

#### Tipos de dados incompatíveis
**Soluções:**
```python
# No oracle_extractor.py, adicionar conversões específicas
def _process_dataframe(self, df):
    # Converter Oracle DATE para timestamp
    for col in df.columns:
        if 'date' in col.lower() or 'timestamp' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Converter NUMBER com precisão específica
    numeric_cols = df.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        if df[col].dtype == 'float64':
            df[col] = df[col].round(2)  # Ajustar precisão
    
    return df
```

### 7. Monitoramento e Alertas

#### Configurar Logs Detalhados
```python
# Adicionar no DAG
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_with_logging(**context):
    logger.info(f"Starting extraction for {context['params']}")
    try:
        # Lógica de extração
        result = extract_data()
        logger.info(f"Extraction completed: {len(result)} rows")
        return result
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise
```

#### Configurar Métricas Customizadas
```python
def publish_metrics(**context):
    import boto3
    
    cloudwatch = boto3.client('cloudwatch')
    
    # Métricas de sucesso/falha
    cloudwatch.put_metric_data(
        Namespace='Pipeline/Oracle-Snowflake',
        MetricData=[
            {
                'MetricName': 'ExtractionSuccess',
                'Value': 1,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Schema', 'Value': context['params']['schema']},
                    {'Name': 'Table', 'Value': context['params']['table']}
                ]
            }
        ]
    )
```

## 🔧 Comandos Úteis para Diagnóstico

### Verificar Status dos Serviços
```bash
# Status do MWAA
aws mwaa get-environment --name your-environment

# Status do RDS Oracle
aws rds describe-db-instances --db-instance-identifier your-oracle-instance

# Verificar conexões ativas no Oracle
sqlplus / as sysdba
SELECT username, count(*) FROM v$session GROUP BY username;
```

### Testes de Conectividade
```python
# Teste conexão Oracle via Python
import cx_Oracle
try:
    conn = cx_Oracle.connect('user/pass@host:1521/service')
    print("Oracle connection: OK")
    conn.close()
except Exception as e:
    print(f"Oracle connection failed: {e}")

# Teste conexão Snowflake
import snowflake.connector
try:
    conn = snowflake.connector.connect(
        user='user',
        password='pass',
        account='account',
        warehouse='warehouse'
    )
    print("Snowflake connection: OK")
    conn.close()
except Exception as e:
    print(f"Snowflake connection failed: {e}")
```

### Verificar Performance
```sql
-- Oracle: Query performance
SELECT sql_text, executions, elapsed_time 
FROM v$sql 
WHERE sql_text LIKE '%YOUR_TABLE%'
ORDER BY elapsed_time DESC;

-- Snowflake: Query history
SELECT query_text, execution_time, warehouse_size
FROM information_schema.query_history 
WHERE start_time > CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY execution_time DESC;
```

## 📞 Escalação e Suporte

### Níveis de Escalação
1. **Nível 1:** Verificar logs do Airflow e status dos serviços
2. **Nível 2:** Investigar conectividade Oracle/Snowflake 
3. **Nível 3:** Análise detalhada de performance e otimização
4. **Nível 4:** Contato com AWS Support para problemas de infraestrutura

### Informações para Coleta em Caso de Problemas
- Logs específicos da task que falhou
- Timestamps exatos do erro
- Configuração das conexões (sem senhas)
- Versões dos componentes (Airflow, Oracle, Snowflake)
- Volume de dados sendo processado
- Configuração atual do warehouse Snowflake

### Contatos de Suporte
- **AWS MWAA:** Através do AWS Support Console
- **Snowflake:** Portal de suporte Snowflake
- **Oracle RDS:** AWS Support Console
- **Time interno:** data-team@company.com `ORA-12170: TNS:Connect timeout occurred`
**Causa:** Timeout na conexão com Oracle RDS
**Soluções:**
```bash
# Verificar security groups do RDS
aws ec2 describe-security-groups --group-ids sg-xxxxx

# Verificar conectividade de rede
telnet your-oracle-endpoint.amazonaws.com 1521

# Ajustar timeout na conexão Airflow
# Extra fields na conexão Oracle:
{
  "encoding": "UTF-8",
  "nencoding": "UTF-8", 
  "threaded": true,
  "connect_timeout": 60,
  "retry_count": 3
}
```

#### Erro: `ORA-01017: invalid username/password`
**Causa:** Credenciais incorretas
**Soluções:**
- Verificar usuário/senha na conexão Airflow
- Testar credenciais diretamente no Oracle:
```sql
-- Conectar via SQL*Plus para testar
sqlplus username/password@//endpoint:1521/ORCL
```

#### Erro: `ORA-00942: table or view does not exist`
**Causa:** Permissões insuficientes ou schema incorreto
**Soluções:**
```sql
-- Verificar permissões do usuário
SELECT * FROM USER_TAB_PRIVS WHERE TABLE_NAME = 'YOUR_TABLE';

-- Verificar schemas disponíveis
SELECT DISTINCT OWNER FROM ALL_TABLES WHERE OWNER NOT IN ('SYS','SYSTEM');

-- Grant permissões se necessário
GRANT SELECT ON schema.table TO your_user;
```

### 2. Problemas de Memória/Performance

#### Erro: `MemoryError` durante extração
**Causa:** Tabelas muito grandes sendo carregadas na memória
**Soluções:**
```python
# Implementar chunking na extração
def extract_table_chunked(self, schema, table, chunk_size=100000):
    query = f"SELECT * FROM {schema}.{table}"
    
    for chunk_df in pd.read_sql(query, con=self.oracle_hook.get_conn(), 
                               chunksize=chunk_size):
        # Processa chunk por chunk
        yield chunk_df
```

#### Performance lenta na extração
**Soluções:**
```sql
-- Criar índices nas colunas de timestamp
CREATE INDEX idx_table_updated_at ON schema.table(updated_at);

-- Usar hints Oracle para melhor performance  
SELECT /*+ FIRST_ROWS(1000) */ * FROM schema.table 
WHERE updated_at > :last_timestamp;
```

### 3. Problemas Snowflake

#### Erro: