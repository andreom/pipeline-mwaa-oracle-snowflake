#!/bin/bash

# Script de Deploy do Pipeline Oracle → Snowflake
# Execute este script para configurar todo o ambiente

set -e

echo "🚀 Iniciando deploy do pipeline Oracle → Snowflake"

# Configuração de variáveis
MWAA_ENVIRONMENT_NAME="your-mwaa-environment"
S3_BUCKET_MWAA="your-mwaa-s3-bucket"
S3_BUCKET_DATA="your-data-lake-bucket"
REGION="us-east-1"

echo "📋 Configurações:"
echo "  MWAA Environment: $MWAA_ENVIRONMENT_NAME"
echo "  MWAA S3 Bucket: $S3_BUCKET_MWAA"
echo "  Data Lake Bucket: $S3_BUCKET_DATA"
echo "  Region: $REGION"

# 1. Upload dos arquivos Python para S3 do MWAA
echo "📁 Fazendo upload dos arquivos Python..."

aws s3 cp oracle_to_snowflake_dag.py s3://$S3_BUCKET_MWAA/dags/
aws s3 cp oracle_extractor.py s3://$S3_BUCKET_MWAA/dags/
aws s3 cp snowflake_loader.py s3://$S3_BUCKET_MWAA/dags/
aws s3 cp monitoring_and_alerts.py s3://$S3_BUCKET_MWAA/dags/

# 2. Upload do requirements.txt
echo "📦 Atualizando dependências..."

cat > requirements.txt << EOF
apache-airflow-providers-oracle==3.7.0
apache-airflow-providers-snowflake==4.4.2
apache-airflow-providers-amazon==8.8.0
cx-Oracle==8.3.0
snowflake-connector-python==3.5.0
pandas==1.5.3
pyarrow==12.0.1
requests==2.31.0
boto3==1.28.62
EOF

aws s3 cp requirements.txt s3://$S3_BUCKET_MWAA/

# 3. Criar estrutura de pastas no S3 Data Lake
echo "🗂️ Criando estrutura de pastas no Data Lake..."

aws s3 cp /dev/null s3://$S3_BUCKET_DATA/oracle-extracts/data/.keep
aws s3 cp /dev/null s3://$S3_BUCKET_DATA/oracle-extracts/metadata/.keep
aws s3 cp /dev/null s3://$S3_BUCKET_DATA/oracle-extracts/logs/.keep

# 4. Configurar conexões Airflow
echo "🔗 Configurando conexões..."

# Oracle Connection
cat > oracle_conn.json << EOF
{
    "conn_id": "oracle_conn",
    "conn_type": "oracle",
    "host": "your-oracle-rds-endpoint.amazonaws.com",
    "port": 1521,
    "schema": "ORCL",
    "login": "your_oracle_user",
    "password": "your_oracle_password",
    "extra": {
        "encoding": "UTF-8",
        "nencoding": "UTF-8",
        "threaded": true
    }
}
EOF

# Snowflake Connection  
cat > snowflake_conn.json << EOF
{
    "conn_id": "snowflake_conn",
    "conn_type": "snowflake",
    "host": "your-account.snowflakecomputing.com",
    "login": "your_snowflake_user",
    "password": "your_snowflake_password",
    "schema": "PUBLIC",
    "extra": {
        "account": "your_account",
        "warehouse": "COMPUTE_WH",
        "database": "ANALYTICS_DB",
        "region": "us-east-1",
        "role": "SYSADMIN"
    }
}
EOF

# AWS S3 Connection
cat > aws_s3_conn.json << EOF
{
    "conn_id": "aws_s3_conn",
    "conn_type": "aws",
    "extra": {
        "region_name": "$REGION",
        "aws_access_key_id": "",
        "aws_secret_access_key": ""
    }
}
EOF

echo "⚠️  IMPORTANTE: Configure as conexões manualmente no Airflow Web UI:"
echo "   1. Oracle: oracle_conn"
echo "   2. Snowflake: snowflake_conn" 
echo "   3. AWS S3: aws_s3_conn"

# 5. Atualizar ambiente MWAA
echo "🔄 Atualizando ambiente MWAA..."

aws mwaa update-environment \
    --name $MWAA_ENVIRONMENT_NAME \
    --requirements-s3-path requirements.txt \
    --region $REGION

echo "⏳ Aguardando atualização do MWAA (isso pode levar 10-20 minutos)..."

# Aguarda atualização
while true; do
    STATUS=$(aws mwaa get-environment --name $MWAA_ENVIRONMENT_NAME --region $REGION --query 'Environment.Status' --output text)
    if [ "$STATUS" = "AVAILABLE" ]; then
        echo "✅ MWAA environment atualizado com sucesso!"
        break
    elif [ "$STATUS" = "UPDATE_FAILED" ]; then
        echo "❌ Falha na atualização do MWAA"
        exit 1
    else
        echo "   Status atual: $STATUS"
        sleep 30
    fi
done

# 6. Criar IAM roles e políticas necessárias
echo "🔐 Configurando permissões IAM..."

# Política para MWAA acessar Oracle RDS
cat > mwaa-oracle-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "rds:DescribeDBInstances",
                "rds:DescribeDBClusters"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::$S3_BUCKET_DATA",
                "arn:aws:s3:::$S3_BUCKET_DATA/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
        }
    ]
}
EOF

# Anexar política ao role do MWAA (substitua pelo ARN correto)
echo "📎 Anexe a política 'mwaa-oracle-policy.json' ao role do MWAA"

# 7. Configurar alarmes CloudWatch
echo "📊 Configurando alarmes CloudWatch..."

aws cloudwatch put-metric-alarm \
    --alarm-name "Oracle-Snowflake-Pipeline-Failures" \
    --alarm-description "Alerta quando pipeline Oracle->Snowflake falha" \
    --metric-name "PipelineSuccess" \
    --namespace "Airflow/ETL" \
    --statistic "Sum" \
    --period 3600 \
    --threshold 1 \
    --comparison-operator "LessThanThreshold" \
    --evaluation-periods 1 \
    --alarm-actions "arn:aws:sns:$REGION:$(aws sts get-caller-identity --query Account --output text):data-alerts" \
    --region $REGION

# 8. Criar tópico SNS para alertas
echo "📢 Configurando notificações SNS..."

aws sns create-topic --name data-alerts --region $REGION

echo "📧 Adicione seu email ao tópico SNS 'data-alerts' para receber alertas"

# 9. Validação final
echo "🔍 Executando validações finais..."

# Verifica se os DAGs foram carregados
echo "   Verificando DAGs no MWAA..."
aws mwaa create-cli-token --name $MWAA_ENVIRONMENT_NAME --region $REGION > /dev/null

# Cleanup
rm -f oracle_conn.json snowflake_conn.json aws_s3_conn.json requirements.txt mwaa-oracle-policy.json

echo ""
echo "🎉 Deploy concluído com sucesso!"
echo ""
echo "📋 Próximos passos:"
echo "   1. Configure as conexões no Airflow Web UI"
echo "   2. Ajuste os schemas/tabelas no arquivo oracle_to_snowflake_dag.py"
echo "   3. Teste o pipeline executando o DAG manualmente"
echo "   4. Configure alertas no Slack (webhook URL)"
echo "   5. Monitore os logs durante as primeiras execuções"
echo ""
echo "🔗 URLs úteis:"
echo "   MWAA Web UI: https://$MWAA_ENVIRONMENT_NAME.${REGION}.airflow.amazonaws.com/"
echo "   CloudWatch Logs: https://console.aws.amazon.com/cloudwatch/home?region=$REGION#logsV2:log-groups"