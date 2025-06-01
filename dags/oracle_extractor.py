import pandas as pd
import boto3
from datetime import datetime, timedelta
from airflow.providers.oracle.hooks.oracle import OracleHook
import logging
import os

class OracleExtractor:
    
    def __init__(self, connection_id, s3_bucket, s3_prefix):
        self.connection_id = connection_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.oracle_hook = OracleHook(oracle_conn_id=connection_id)
        self.s3_client = boto3.client('s3')
        self.logger = logging.getLogger(__name__)
    
    def get_table_metadata(self, schema, table):
        """Recupera metadados da tabela para extração incremental"""
        
        metadata_query = """
        SELECT 
            column_name,
            data_type,
            data_length
        FROM all_tab_columns 
        WHERE owner = :schema 
        AND table_name = :table
        ORDER BY column_id
        """
        
        result = self.oracle_hook.get_records(
            sql=metadata_query,
            parameters={'schema': schema, 'table': table}
        )
        
        return result
    
    def get_incremental_column(self, schema, table):
        """Identifica coluna para extração incremental"""
        
        # Procura por colunas de timestamp/data
        timestamp_query = """
        SELECT column_name 
        FROM all_tab_columns 
        WHERE owner = :schema 
        AND table_name = :table 
        AND data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
        AND column_name IN ('UPDATED_AT', 'MODIFIED_DATE', 'LAST_MODIFIED', 'CREATE_DATE')
        ORDER BY 
            CASE column_name 
                WHEN 'UPDATED_AT' THEN 1
                WHEN 'MODIFIED_DATE' THEN 2  
                WHEN 'LAST_MODIFIED' THEN 3
                WHEN 'CREATE_DATE' THEN 4
                ELSE 5
            END
        """
        
        result = self.oracle_hook.get_first(
            sql=timestamp_query,
            parameters={'schema': schema, 'table': table}
        )
        
        return result[0] if result else None
    
    def get_last_extraction_timestamp(self, schema, table):
        """Recupera último timestamp de extração do S3 metadata"""
        
        metadata_key = f"{self.s3_prefix}/metadata/{schema}/{table}/last_extraction.txt"
        
        try:
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=metadata_key
            )
            return response['Body'].read().decode('utf-8').strip()
        except:
            # Se não existe, retorna data padrão (últimos 7 dias)
            return (datetime.now() - timedelta(days=7)).isoformat()
    
    def save_extraction_timestamp(self, schema, table, timestamp):
        """Salva timestamp da extração atual"""
        
        metadata_key = f"{self.s3_prefix}/metadata/{schema}/{table}/last_extraction.txt"
        
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=metadata_key,
            Body=timestamp
        )
    
    def extract_table_incremental(self, schema, table, execution_date):
        """Extrai dados incrementais de uma tabela"""
        
        self.logger.info(f"Starting extraction for {schema}.{table}")
        
        # Identifica coluna incremental
        incremental_col = self.get_incremental_column(schema, table)
        
        if incremental_col:
            # Extração incremental
            last_timestamp = self.get_last_extraction_timestamp(schema, table)
            
            query = f"""
            SELECT * FROM {schema}.{table}
            WHERE {incremental_col} > TO_TIMESTAMP(:last_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS')
            """
            
            df = pd.read_sql(
                sql=query,
                con=self.oracle_hook.get_conn(),
                params={'last_timestamp': last_timestamp}
            )
            
            extraction_type = 'incremental'
            
        else:
            # Extração completa se não há coluna incremental
            self.logger.warning(f"No incremental column found for {schema}.{table}. Performing full extract.")
            
            query = f"SELECT * FROM {schema}.{table}"
            df = pd.read_sql(sql=query, con=self.oracle_hook.get_conn())
            extraction_type = 'full'
        
        # Processa dados
        if not df.empty:
            # Converte tipos de dados problemáticos
            df = self._process_dataframe(df)
            
            # Salva no S3 em formato Parquet
            file_path = self._save_to_s3(df, schema, table, execution_date, extraction_type)
            
            # Atualiza timestamp se foi extração incremental
            if incremental_col:
                current_timestamp = execution_date.isoformat()
                self.save_extraction_timestamp(schema, table, current_timestamp)
            
            self.logger.info(f"Extracted {len(df)} rows from {schema}.{table}")
            return file_path
        
        else:
            self.logger.info(f"No new data found for {schema}.{table}")
            return None
    
    def _process_dataframe(self, df):
        """Processa DataFrame para compatibilidade com Parquet/Snowflake"""
        
        # Converte CLOB/BLOB para string
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    # Tenta converter LOBs para string
                    df[col] = df[col].astype(str)
                except:
                    pass
        
        # Trata valores nulos em colunas numéricas
        numeric_columns = df.select_dtypes(include=['number']).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
        
        # Trata valores nulos em colunas de texto
        text_columns = df.select_dtypes(include=['object']).columns
        df[text_columns] = df[text_columns].fillna('')
        
        return df
    
    def _save_to_s3(self, df, schema, table, execution_date, extraction_type):
        """Salva DataFrame no S3 em formato Parquet"""
        
        # Define caminho do arquivo
        date_partition = execution_date.strftime('%Y/%m/%d')
        timestamp = execution_date.strftime('%Y%m%d_%H%M%S')
        
        file_key = f"{self.s3_prefix}/data/{schema.lower()}/{table.lower()}/{date_partition}/{extraction_type}_{timestamp}.parquet"
        
        # Salva localmente primeiro
        local_file = f"/tmp/{schema}_{table}_{timestamp}.parquet"
        df.to_parquet(local_file, index=False, engine='pyarrow')
        
        # Upload para S3
        self.s3_client.upload_file(
            Filename=local_file,
            Bucket=self.s3_bucket,
            Key=file_key
        )
        
        # Remove arquivo local
        os.remove(local_file)
        
        s3_path = f"s3://{self.s3_bucket}/{file_key}"
        self.logger.info(f"Data saved to {s3_path}")
        
        return s3_path