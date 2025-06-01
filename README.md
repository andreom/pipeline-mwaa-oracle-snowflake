# Oracle to Snowflake Data Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.5+-red.svg)](https://airflow.apache.org/)

Um pipeline robusto e escalável para extrair dados de múltiplos schemas Oracle RDS e carregar no Snowflake usando AWS MWAA (Managed Apache Airflow).

## 🚀 Características Principais

- **Extração Incremental Inteligente**: Detecta automaticamente colunas de timestamp para extrações incrementais
- **Alta Performance**: Utiliza formato Parquet e staging em S3 para otimizar cargas
- **Monitoramento Completo**: Métricas CloudWatch, alertas Slack/email e validação de qualidade
- **Tolerância a Falhas**: Retry logic, pools de conexão e recovery automático
- **Escalabilidade**: Processamento paralelo por schema com controle de recursos

## 🏗️ Arquitetura

```
Oracle RDS → Airflow (MWAA) → S3 (Staging) → Snowflake
    ↓              ↓              ↓            ↓
Schemas/Tables → Extraction → Parquet Files → Data Warehouse
```

### Componentes

- **Oracle RDS**: Banco de dados fonte com múltiplos schemas
- **AWS MWAA**: Orquestração via Apache Airflow gerenciado
- **Amazon S3**: Staging area para arquivos Parquet
- **Snowflake**: Data warehouse de destino
- **CloudWatch**: Monitoramento e alertas

## 📋 Pré-requisitos

### Infraestrutura
- [ ] AWS Account com MWAA configurado
- [ ] Oracle RDS instance acessível
- [ ] Snowflake account ativo
- [ ] S3 bucket para staging
- [ ] IAM roles com permissões adequadas

### Credenciais
- [ ] Usuário Oracle com SELECT privileges
- [ ] Usuário Snowflake com CREATE privileges
- [ ] AWS credentials para MWAA

## 🛠️ Instalação

### 1. Clone o Repositório
```bash
git clone https://github.com/seu-usuario/oracle-snowflake-pipeline.git
cd oracle-snowflake-pipeline
```

### 2. Configure o Ambiente
```bash
# Tornar script executável
chmod +x deployment/deploy_pipeline.sh

# Editar configurações
cp .env.example .env
vim .env
```

### 3. Deploy Automatizado
```bash
./deployment/deploy_pipeline.sh
```

## ⚙️ Configuração

### Conexões Airflow

#### Oracle Connection
```json
{
  "conn_id": "oracle_conn",
  "conn_type": "oracle",
  "host": "your-oracle-endpoint.amazonaws.com",
  "port": 1521,
  "schema": "ORCL",
  "login": "your_user",
  "password": "your_password"
}
```

#### Snowflake Connection
```json
{
  "conn_id": "snowflake_conn",
  "conn_type": "snowflake",
  "host": "your-account.snowflakecomputing.com",
  "login": "your_user",
  "password": "your_password",
  "extra": {
    "account": "your_account",
    "warehouse": "COMPUTE_WH",
    "database": "ANALYTICS_DB"
  }
}
```

### Configuração de Schemas/Tabelas

Edite `dags/oracle_to_snowflake_dag.py`:
```python
SCHEMAS_CONFIG = {
    'SCHEMA1': ['TABLE1', 'TABLE2', 'TABLE3'],
    'SCHEMA2': ['TABLE4', 'TABLE5'],
    'SCHEMA3': ['TABLE6', 'TABLE7', 'TABLE8']
}
```

## 🚀 Uso

### Execução Manual
1. Acesse Airflow Web UI
2. Ative o DAG `oracle_to_snowflake_pipeline`
3. Trigger manual execution

### Execução Programada
```python
# Configuração no DAG
schedule_interval='0 2 * * *'  # Diário às 2h AM
```

### Testes
```bash
# Executar testes unitários
python -m pytest tests/

# Executar DAG de testes no Airflow
# Trigger: oracle_snowflake_tests
```

## 📊 Monitoramento

### Métricas CloudWatch
- Pipeline execution time
- Data volume processed
- Success/failure rates
- Resource utilization

### Alertas
- **Email**: Configurado via SNS
- **Slack**: Webhook notifications
- **CloudWatch Alarms**: Threshold-based alerts

### Logs
```bash
# MWAA Logs
aws logs describe-log-groups --log-group-name-prefix airflow

# Task-specific logs via Airflow UI
```

## 🔧 Troubleshooting

### Problemas Comuns

#### Conexão Oracle
```bash
# Testar conectividade
telnet your-oracle-endpoint.amazonaws.com 1521

# Verificar security groups
aws ec2 describe-security-groups --group-ids sg-xxxxx
```

#### Performance Lenta
```sql
-- Criar índices Oracle
CREATE INDEX idx_table_updated_at ON schema.table(updated_at);

-- Ajustar warehouse Snowflake
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'LARGE';
```

Ver [Guia Completo de Troubleshooting](docs/TROUBLESHOOTING_GUIDE.md)

## 📁 Estrutura do Projeto

```
oracle-snowflake-pipeline/
├── dags/                      # DAGs Airflow
│   ├── oracle_to_snowflake_dag.py
│   ├── oracle_extractor.py
│   └── snowflake_loader.py
├── deployment/                # Scripts deploy
├── docs/                      # Documentação
├── tests/                     # Testes
└── sql/                       # Scripts SQL
```

## 🧪 Testes

### Executar Testes
```bash
# Testes unitários
python -m pytest tests/unit/

# Testes integração
python -m pytest tests/integration/

# Coverage
python -m pytest --cov=dags tests/
```

### Validação de Deploy
```bash
# Executar checklist
./scripts/validate_environment.py

# Testar conexões
python tests/integration/test_connections.py
```

## 🔄 Atualizações

### Adicionar Nova Tabela
1. Editar `SCHEMAS_CONFIG` no DAG
2. Commit e push para atualizar MWAA
3. Testar extração manualmente

### Modificar Schedule
```python
# No DAG principal
schedule_interval='0 */6 * * *'  # A cada 6 horas
```

## 📈 Performance

### Otimizações Implementadas
- **Extração Incremental**: Apenas dados novos/modificados
- **Formato Parquet**: Compressão e performance
- **Pools de Conexão**: Controle de paralelismo
- **Staging S3**: Decoupling entre Oracle e Snowflake
- **Auto-scaling**: Warehouse Snowflake se ajusta automaticamente

### Benchmarks Típicos
- **Small datasets** (< 1M rows): 15-30 min
- **Medium datasets** (1-10M rows): 30-90 min  
- **Large datasets** (> 10M rows): 1-3 hours

## 🤝 Contribuição

1. Fork o projeto
2. Crie feature branch (`git checkout -b feature/nova-funcionalidade`)
3. Commit mudanças (`git commit -am 'Adiciona nova funcionalidade'`)
4. Push branch (`git push origin feature/nova-funcionalidade`)
5. Abra Pull Request

### Guidelines
- Seguir PEP 8 para Python
- Adicionar testes para novas funcionalidades
- Atualizar documentação
- Usar conventional commits

## 📄 Licença

Este projeto está licenciado sob a MIT License - veja o arquivo [LICENSE](LICENSE) para detalhes.

## 📞 Suporte

### Contatos
- **Técnico**: data-team@company.com
- **Infraestrutura**: infra-team@company.com

### Issues
Para reportar bugs ou solicitar features, use [GitHub Issues](https://github.com/seu-usuario/oracle-snowflake-pipeline/issues).

### Documentação Adicional
- [Deployment Checklist](docs/DEPLOYMENT_CHECKLIST.md)
- [Troubleshooting Guide](docs/TROUBLESHOOTING_GUIDE.md)
- [Architecture Documentation](docs/ARCHITECTURE.md)

---

**Desenvolvido com ❤️ pela equipe de Data Engineering**