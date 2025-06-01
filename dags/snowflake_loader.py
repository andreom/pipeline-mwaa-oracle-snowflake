from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging
from datetime import datetime

class SnowflakeLoader:
    
    def __init__(self, connection_id):
        self.connection_id = connection_id
        self.snowflake_hook = SnowflakeHook(snowflake_conn_id=connection_id)
        self.logger = logging.getLogger(__name__)
    
    def create_schema_if_not_exists(self, schema_name):
        """Cria schema no Snowflake se não existir"""
        
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        
        self.snowflake_hook.run(create_schema_sql)
        self.logger.info(f"Schema {schema_name} created/verified")
    
    def create_stage_if_not_exists(self, stage_name, s3_bucket, s3_prefix):
        """Cria stage para S3 se não existir"""
        
        create_stage_sql = f"""
        CREATE STAGE IF NOT EXISTS {stage_name}
        URL = 's3://{s3_bucket}/{s3_prefix}/'
        FILE_FORMAT = (
            TYPE = PARQUET
            COMPRESSION = SNAPPY
        )
        """
        
        self.snowflake_hook.run(create_stage_sql)
        self.logger.info(f"Stage {stage_name} created/verified")
    
    def create_table_from_parquet(self, schema_name, table_name, s3_file_path):
        """Cria tabela no Snowflake baseada na estrutura do arquivo Parquet"""
        
        # Usa INFER_SCHEMA para detectar estrutura do Parquet
        infer_sql = f"""
        SELECT *
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '{s3_file_path}',
                FILE_FORMAT => 'parquet'
            )
        )
        """
        
        schema_info = self.snowflake_hook.get_records(infer_sql)
        
        # Constrói DDL da tabela
        columns = []
        for row in schema_info:
            col_name = row[0]
            col_type = self._map_parquet_to_snowflake_type(row[1])
            columns.append(f"{col_name} {col_type}")
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            {', '.join(columns)},
            _EXTRACTION_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            _SOURCE_FILE STRING
        )
        """
        
        self.snowflake_hook.run(create_table_sql)
        self.logger.info(f"Table {schema_name}.{table_name} created/verified")
    
    def copy_from_s3(self, s3_path, target_schema, target_table, file_format='PARQUET'):
        """Carrega dados do S3 para Snowflake usando COPY INTO"""
        
        # Garante que schema existe
        self.create_schema_if_not_exists(target_schema)
        
        # Cria tabela se não existir (baseada no primeiro arquivo)
        try:
            self.create_table_from_parquet(target_schema, target_table, s3_path)
        except Exception as e:
            self.logger.warning(f"Could not auto-create table: {e}")
        
        # Comando COPY INTO
        copy_sql = f"""
        COPY INTO {target_schema}.{target_table}
        FROM '{s3_path}'
        FILE_FORMAT = (TYPE = {file_format})
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE'
        """
        
        # Adiciona metadados de auditoria
        copy_sql_with_metadata = f"""
        COPY INTO {target_schema}.{target_table}
        FROM (
            SELECT 
                *,
                CURRENT_TIMESTAMP() as _EXTRACTION_TIMESTAMP,
                METADATA$FILENAME as _SOURCE_FILE
            FROM '{s3_path}'
        )
        FILE_FORMAT = (TYPE = {file_format})
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE'
        """
        
        try:
            result = self.snowflake_hook.run(copy_sql_with_metadata, autocommit=True)
            self.logger.info(f"Data loaded successfully to {target_schema}.{target_table}")
            return result
        except Exception as e:
            # Fallback para COPY simples se metadados falharem
            self.logger.warning(f"Metadata COPY failed, trying simple COPY: {e}")
            result = self.snowflake_hook.run(copy_sql, autocommit=True)
            return result
    
    def merge_incremental_data(self, target_schema, target_table, source_table, merge_keys):
        """Executa MERGE para dados incrementais"""
        
        merge_sql = f"""
        MERGE INTO {target_schema}.{target_table} AS target
        USING {source_table} AS source
        ON {' AND '.join([f'target.{key} = source.{key}' for key in merge_keys])}
        WHEN MATCHED THEN
            UPDATE SET {', '.join([f'{col} = source.{col}' for col in self._get_table_columns(target_schema, target_table) if col not in merge_keys])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(self._get_table_columns(target_schema, target_table))})
            VALUES ({', '.join([f'source.{col}' for col in self._get_table_columns(target_schema, target_table)])})
        """
        
        result = self.snowflake_hook.run(merge_sql, autocommit=True)
        self.logger.info(f"MERGE completed for {target_schema}.{target_table}")
        return result
    
    def _map_parquet_to_snowflake_type(self, parquet_type):
        """Mapeia tipos Parquet para tipos Snowflake"""
        
        type_mapping = {
            'BIGINT': 'NUMBER(19,0)',
            'INT': 'NUMBER(10,0)', 
            'DOUBLE': 'FLOAT',
            'FLOAT': 'FLOAT',
            'BOOLEAN': 'BOOLEAN',
            'STRING': 'VARCHAR(16777216)',
            'BINARY': 'BINARY',
            'DATE': 'DATE',
            'TIMESTAMP': 'TIMESTAMP_NTZ'
        }
        
        return type_mapping.get(parquet_type.upper(), 'VARCHAR(16777216)')
    
    def _get_table_columns(self, schema_name, table_name):
        """Recupera colunas de uma tabela"""
        
        columns_sql = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{schema_name.upper()}' 
        AND table_name = '{table_name.upper()}'
        ORDER BY ordinal_position
        """
        
        result = self.snowflake_hook.get_records(columns_sql)
        return [row[0] for row in result]
    
    def validate_data_quality(self, schema_name, table_name, expected_min_rows=0):
        """Valida qualidade dos dados carregados"""
        
        validation_sql = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT _SOURCE_FILE) as source_files,
            MAX(_EXTRACTION_TIMESTAMP) as last_update
        FROM {schema_name}.{table_name}
        WHERE DATE(_EXTRACTION_TIMESTAMP) = CURRENT_DATE()
        """
        
        result = self.snowflake_hook.get_first(validation_sql)
        
        if result[0] < expected_min_rows:
            raise ValueError(f"Data quality check failed: only {result[0]} rows loaded, expected at least {expected_min_rows}")
        
        self.logger.info(f"Data quality validation passed: {result[0]} rows loaded from {result[1]} files")
        return result