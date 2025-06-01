# ✅ Checklist de Deploy - Pipeline Oracle → Snowflake

## 📋 Pré-requisitos

### Infraestrutura AWS
- [ ] MWAA Environment criado e funcionando
- [ ] Oracle RDS acessível e configurado
- [ ] S3 Bucket para staging criado
- [ ] IAM Roles com permissões adequadas
- [ ] VPC/Security Groups configurados para conectividade

### Credenciais e Acessos
- [ ] Usuário Oracle com permissões SELECT nas tabelas necessárias
- [ ] Usuário Snowflake com permissões CREATE SCHEMA/TABLE
- [ ] AWS credentials configuradas para MWAA
- [ ] Slack webhook configurado (opcional)

## 🚀 Deploy Steps

### 1. Upload de Arquivos
- [ ] `oracle_to_snowflake_dag.py` → S3 MWAA dags/
- [ ] `oracle_extractor.py` → S3 MWAA dags/
- [ ] `snowflake_loader.py` → S3 MWAA dags/
- [ ] `monitoring_and_alerts.py` → S3 MWAA dags/
- [ ] `requirements.txt` → S3 MWAA root/

### 2. Configuração Airflow
- [ ] Conexão `oracle_conn` criada e testada
- [ ] Conexão `snowflake_conn` criada e testada
- [ ] Conexão `aws_s3_conn` criada (se necessário)
- [ ] Pools configurados:
  - [ ] `oracle_pool` (5 slots)
  - [ ] `snowflake_pool` (10 slots)
  - [ ] `s3_pool` (15 slots)

### 3. Configuração de Variáveis
- [ ] Variável `oracle_snowflake_config` criada no Airflow
- [ ] S3 bucket e prefix definidos corretamente
- [ ] Schemas e tabelas configurados no DAG
- [ ] Emails de alerta configurados

### 4. Testes Iniciais
- [ ] DAG aparece na UI do Airflow sem erros
- [ ] Execução manual do DAG de testes passa
- [ ] Conexões validadas através do teste
- [ ] Dry run de extração executado com sucesso

## 🔍 Verificações Pós-Deploy

### Primeira Execução
- [ ] DAG principal executado manualmente
- [ ] Dados extraídos para S3 corretamente
- [ ] Schemas criados no Snowflake
- [ ] Tabelas criadas com estrutura correta
- [ ] Dados carregados no Snowflake
- [ ] Métricas CloudWatch publicadas

### Validação de Dados
- [ ] Contagem de registros Oracle vs Snowflake confere
- [ ] Tipos de dados mapeados corretamente
- [ ] Dados incrementais funcionando (próxima execução)
- [ ] Metadados de auditoria (_EXTRACTION_TIMESTAMP, _SOURCE_FILE) presentes

### Monitoramento
- [ ] Logs do MWAA acessíveis e informativos
- [ ] Alarmes CloudWatch configurados
- [ ] Notificações Slack funcionando (se configurado)
- [ ] SNS tópicos configurados para alertas

## 📅 Schedule de Execução

### Configuração Recomendada
```python
# Produção
schedule_interval='0 2 * * *'  # Diário às 2h

# Desenvolvimento/Teste  
schedule_interval=None  # Manual apenas
```

### Janelas de Execução
- [ ] Horário definido para não conflitar com outros jobs
- [ ] Janela de manutenção Oracle considerada
- [ ] Warehouse Snowflake dimensionado adequadamente
- [ ] Timeout configurado adequadamente (4-6 horas)

## 🔧 Configurações de Performance

### Oracle RDS
- [ ] Índices criados nas colunas de timestamp incremental
- [ ] Connection pooling configurado
- [ ] Timeout de conexão ajustado (60s)
- [ ] Tamanho do RDS adequado para carga

### Snowflake
- [ ] Warehouse AUTO_RESUME habilitado
- [ ] Warehouse dimensionado adequadamente (SMALL → LARGE)
- [ ] File format otimizado (PARQUET com SNAPPY)
- [ ] Stage configurado corretamente

### MWAA
- [ ] Environment class adequado (mw1.small → mw1.medium)
- [ ] Max workers configurado (10-25)
- [ ] Paralelismo de DAG controlado
- [ ] Pools dimensionados corretamente

## 📊 Métricas para Acompanhar

### Diárias
- [ ] Volume de dados extraído (GB)
- [ ] Tempo total de execução (minutos)
- [ ] Taxa de sucesso das tasks (%)
- [ ] Número de registros processados

### Semanais
- [ ] Crescimento do volume de dados
- [ ] Performance trends (tempo por tabela)
- [ ] Utilização do warehouse Snowflake
- [ ] Custos AWS (MWAA + S3 + RDS)

### Mensais
- [ ] Review das configurações de performance
- [ ] Otimização de queries Oracle
- [ ] Limpeza de arquivos antigos no S3
- [ ] Review de alertas e falsos positivos

## 🚨 Plano de Contingência

### Falha Completa do Pipeline
1. [ ] Verificar status dos serviços (MWAA, RDS, Snowflake)
2. [ ] Validar conectividade de rede
3. [ ] Executar DAG de testes para diagnóstico
4. [ ] Escalar para equipe de infraestrutura se necessário

### Falha Parcial (Algumas Tabelas)
1. [ ] Identificar tabelas falhando nos logs
2. [ ] Executar extração manual via CLI se necessário
3. [ ] Investigar problemas específicos da tabela
4. [ ] Re-executar tasks específicas

### Dados Corrompidos/Duplicados
1. [ ] Parar execuções automáticas
2. [ ] Identificar janela temporal do problema
3. [ ] Executar queries de limpeza no Snowflake
4. [ ] Re-processar dados do período afetado

## 📝 Documentação Final

### Para Desenvolvedores
- [ ] README.md atualizado com instruções
- [ ] Código comentado adequadamente
- [ ] Testes unitários documentados
- [ ] Guia de troubleshooting acessível

### Para Operações
- [ ] Runbook de operação diária
- [ ] Procedimentos de escalação definidos
- [ ] Contatos de suporte atualizados
- [ ] SLAs definidos para resolução de problemas

### Para Business Users
- [ ] Documentação das tabelas disponíveis
- [ ] Horários de atualização comunicados
- [ ] SLA de disponibilidade dos dados
- [ ] Processo para solicitar novas tabelas

## 🎯 Critérios de Sucesso

### Técnicos
- [ ] SLA 99.5% de disponibilidade mensal
- [ ] Latência máxima 4 horas (extração → disponibilidade)
- [ ] Zero perda de dados
- [ ] Tempo de execução < 2 horas para carga completa

### Operacionais
- [ ] Alertas efetivos (poucos falsos positivos)
- [ ] Recovery time < 1 hora para problemas comuns
- [ ] Documentação completa e atualizada
- [ ] Time treinado nos procedimentos

### Negócio
- [ ] Dados disponíveis no horário acordado
- [ ] Qualidade dos dados mantida
- [ ] Novos schemas/tabelas podem ser adicionados facilmente
- [ ] Custos dentro do orçamento aprovado

---

## 📞 Contatos de Suporte

| Componente | Contato | SLA |
|------------|---------|-----|
| MWAA/AWS | AWS Support | 4h |
| Oracle RDS | AWS Support | 4h |
| Snowflake | Snowflake Support | 2h |
| Aplicação | data-team@company.com | 1h |
| Infraestrutura | infra-team@company.com | 2h |

---

**Data do Deploy:** _______________  
**Aprovado por:** _______________  
**Responsável Técnico:** _______________  
**Próxima Revisão:** _______________