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
            TYPE = CSV
            FIELD_DELIMITER = ';'
            RECORD_DELIMITER = '\\n'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            ESCAPE_UNENCLOSED_FIELD = NONE
            COMPRESSION = GZIP
            ENCODING = 'UTF-8'
        )
        """
        
        self.snowflake_hook.run(create_stage_sql)
        self.logger.info(f"Stage {stage_name} created/verified for CSV with gzip compression")
    
    def create_file_format_csv(self, format_name='CSV_SEMICOLON_GZIP'):
        """Cria file format específico para CSV com ponto e vírgula e gzip"""
        
        create_format_sql = f"""
        CREATE OR REPLACE FILE FORMAT {format_name}
        TYPE = CSV
        FIELD_DELIMITER = ';'
        RECORD_DELIMITER = '\\n'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        TRIM_SPACE = TRUE
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        ESCAPE_UNENCLOSED_FIELD = NONE
        COMPRESSION = GZIP
        ENCODING = 'UTF-8'
        NULL_IF = ('', 'NULL', 'null')
        """
        
        self.snowflake_hook.run(create_format_sql)
        self.logger.info(f"File format {format_name} created/updated")
        return format_name
    
    def infer_schema_from_csv(self, s3_file_path, file_format='CSV_SEMICOLON_GZIP'):
        """Infere schema de arquivo CSV no S3"""
        
        try:
            infer_sql = f"""
            SELECT *
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '{s3_file_path}',
                    FILE_FORMAT => '{file_format}'
                )
            )
            """
            
            schema_info = self.snowflake_hook.get_records(infer_sql)
            return schema_info
            
        except Exception as e:
            self.logger.warning(f"Could not infer schema from CSV: {e}")
            return None
    
    def create_table_from_csv_schema(self, schema_name, table_name, s3_file_path):
        """Cria tabela baseada na estrutura inferida do CSV"""
        
        # Cria file format se não existir
        file_format = self.create_file_format_csv()
        
        # Tenta inferir schema
        schema_info = self.infer_schema_from_csv(s3_file_path, file_format)
        
        if schema_info:
            # Constrói DDL da tabela baseado no schema inferido
            columns = []
            for row in schema_info:
                col_name = row[0]
                col_type = self._map_csv_to_snowflake_type(row[1])
                columns.append(f'"{col_name}" {col_type}')
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                {', '.join(columns)},
                "_EXTRACTION_TIMESTAMP" TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                "_SOURCE_FILE" STRING
            )
            """
            
        else:
            # Fallback: cria tabela genérica
            self.logger.warning(f"Using generic table structure for {schema_name}.{table_name}")
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                data VARIANT,
                "_EXTRACTION_TIMESTAMP" TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                "_SOURCE_FILE" STRING
            )
            """
        
        self.snowflake_hook.run(create_table_sql)
        self.logger.info(f"Table {schema_name}.{table_name} created/verified")
    
    def copy_csv_from_s3(self, s3_path, target_schema, target_table, file_format='CSV_SEMICOLON_GZIP'):
        """Carrega dados CSV do S3 para Snowflake usando COPY INTO"""
        
        # Garante que schema existe
        self.create_schema_if_not_exists(target_schema)
        
        # Cria file format se não existir
        if file_format == 'CSV_SEMICOLON_GZIP':
            file_format = self.create_file_format_csv()
        
        # Cria tabela se não existir
        try:
            self.create_table_from_csv_schema(target_schema, target_table, s3_path)
        except Exception as e:
            self.logger.warning(f"Could not auto-create table: {e}")
        
        # Comando COPY INTO para CSV
        copy_sql = f"""
        COPY INTO {target_schema}.{target_table}
        FROM (
            SELECT 
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                CURRENT_TIMESTAMP() as "_PULLEDDATETIME",
                METADATA$FILENAME as "_SOURCE_FILE"
            FROM '{s3_path}'
        )
        FILE_FORMAT = '{file_format}'
        ON_ERROR = 'CONTINUE'
        """
        
        # Versão simplificada do COPY (auto-detecção de colunas)
        copy_sql_simple = f"""
        COPY INTO {target_schema}.{target_table}
        FROM '{s3_path}'
        FILE_FORMAT = '{file_format}'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE'
        """
        
        try:
            # Tenta primeiro o COPY com auto-detecção
            result = self.snowflake_hook.run(copy_sql_simple, autocommit=True)
            self.logger.info(f"CSV data loaded successfully to {target_schema}.{target_table}")
            return result
            
        except Exception as e:
            self.logger.warning(f"Auto-detection COPY failed, trying manual mapping: {e}")
            
            try:
                # Fallback para COPY manual
                result = self.snowflake_hook.run(copy_sql, autocommit=True)
                return result
            except Exception as e2:
                self.logger.error(f"Both COPY methods failed: {e2}")
                raise
    
    def copy_multiple_csv_files(self, s3_path_pattern, target_schema, target_table):
        """Carrega múltiplos arquivos CSV (útil para chunks)"""
        
        # Para padrões de arquivo (exemplo: s3://bucket/path/file_*.csv.gz)
        copy_sql = f"""
        COPY INTO {target_schema}.{target_table}
        FROM '{s3_path_pattern}'
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_DELIMITER = ';'
            RECORD_DELIMITER = '\\n'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            COMPRESSION = GZIP
            ENCODING = 'UTF-8'
        )
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE'
        """
        
        result = self.snowflake_hook.run(copy_sql, autocommit=True)
        self.logger.info(f"Multiple CSV files loaded to {target_schema}.{target_table}")
        return result
    
    def _map_csv_to_snowflake_type(self, csv_type):
        """Mapeia tipos CSV inferidos para tipos Snowflake"""
        
        type_mapping = {
            'NUMBER': 'NUMBER',
            'DECIMAL': 'NUMBER(38,10)',
            'INTEGER': 'NUMBER(38,0)',
            'BIGINT': 'NUMBER(38,0)',
            'FLOAT': 'FLOAT',
            'DOUBLE': 'FLOAT',
            'BOOLEAN': 'BOOLEAN',
            'TEXT': 'VARCHAR(16777216)',
            'STRING': 'VARCHAR(16777216)',
            'VARCHAR': 'VARCHAR(16777216)',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'TIMESTAMP_NTZ',
            'DATETIME': 'TIMESTAMP_NTZ'
        }
        
        return type_mapping.get(csv_type.upper(), 'VARCHAR(16777216)')
    
    def validate_csv_data_quality(self, schema_name, table_name, expected_min_rows=0):
        """Valida qualidade dos dados CSV carregados"""
        
        validation_sql = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT "_SOURCE_FILE") as source_files,
            MAX("_EXTRACTION_TIMESTAMP") as last_update,
            COUNT(*) - COUNT(CASE WHEN "_SOURCE_FILE" IS NULL THEN 1 END) as rows_with_source
        FROM {schema_name}.{table_name}
        WHERE DATE("_EXTRACTION_TIMESTAMP") = CURRENT_DATE()
        """
        
        result = self.snowflake_hook.get_first(validation_sql)
        
        if result[0] < expected_min_rows:
            raise ValueError(f"Data quality check failed: only {result[0]} rows loaded, expected at least {expected_min_rows}")
        
        self.logger.info(f"CSV data quality validation passed: {result[0]} rows loaded from {result[1]} files")
        return result
    
    def get_copy_history(self, table_name, hours_back=24):
        """Verifica histórico de COPY para debugging"""
        
        history_sql = f"""
        SELECT 
            FILE_NAME,
            FILE_SIZE,
            ROW_COUNT,
            ROW_PARSED,
            FIRST_ERROR_MESSAGE,
            FIRST_ERROR_LINE_NUMBER,
            FIRST_ERROR_CHARACTER_POSITION,
            LAST_LOAD_TIME
        FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => '{table_name}',
            START_TIME => DATEADD(hours, -{hours_back}, CURRENT_TIMESTAMP())
        ))
        ORDER BY LAST_LOAD_TIME DESC
        """
        
        try:
            result = self.snowflake_hook.get_records(history_sql)
            self.logger.info(f"Retrieved COPY history for {table_name}: {len(result)} operations")
            return result
        except Exception as e:
            self.logger.warning(f"Could not retrieve COPY history: {e}")
            return []