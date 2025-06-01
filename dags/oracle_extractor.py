import pandas as pd
import boto3
from datetime import datetime, timedelta
from airflow.providers.oracle.hooks.oracle import OracleHook
import logging
import os
import csv
import gzip

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
            
            # Salva no S3 em formato CSV compactado
            file_path = self._save_to_s3_csv(df, schema, table, execution_date, extraction_type)
            
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
        """Processa DataFrame para compatibilidade com CSV/Snowflake"""
        
        # Converte CLOB/BLOB para string
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    # Tenta converter LOBs para string
                    df[col] = df[col].astype(str)
                    # Remove quebras de linha e caracteres problemáticos para CSV
                    df[col] = df[col].str.replace('\n', ' ', regex=False)
                    df[col] = df[col].str.replace('\r', ' ', regex=False)
                    df[col] = df[col].str.replace('\t', ' ', regex=False)
                except:
                    pass
        
        # Trata valores nulos em colunas numéricas
        numeric_columns = df.select_dtypes(include=['number']).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
        
        # Trata valores nulos em colunas de texto
        text_columns = df.select_dtypes(include=['object']).columns
        df[text_columns] = df[text_columns].fillna('')
        
        # Converte datas para string formatada
        date_columns = df.select_dtypes(include=['datetime64[ns]']).columns
        for col in date_columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        return df
    
    def _save_to_s3_csv(self, df, schema, table, execution_date, extraction_type):
        """Salva DataFrame no S3 em formato CSV compactado com gzip"""
        
        # Define caminho do arquivo
        date_partition = execution_date.strftime('%Y/%m/%d')
        timestamp = execution_date.strftime('%Y%m%d_%H%M%S')
        
        file_key = f"{self.s3_prefix}/data/{schema.lower()}/{table.lower()}/{date_partition}/{extraction_type}_{timestamp}.csv.gz"
        
        # Salva localmente primeiro
        local_file = f"/tmp/{schema}_{table}_{timestamp}.csv"
        local_file_gz = f"/tmp/{schema}_{table}_{timestamp}.csv.gz"
        
        # Salva CSV com configurações específicas:
        # - Separador: ponto e vírgula (;)
        # - Aspas duplas em todas as strings (QUOTE_ALL)
        # - Encoding UTF-8
        df.to_csv(
            local_file,
            sep=';',                      # Separador ponto e vírgula
            index=False,                  # Não incluir índice
            header=True,                  # Incluir cabeçalho
            quoting=csv.QUOTE_ALL,        # Aspas duplas em todos os campos
            quotechar='"',                # Caractere de aspas duplas
            doublequote=True,             # Escape de aspas duplas
            encoding='utf-8',             # Encoding UTF-8
            na_rep='',                    # Representação de valores nulos
            lineterminator='\n',          # Terminador de linha
            date_format='%Y-%m-%d %H:%M:%S'  # Formato de data
        )
        
        # Compacta o arquivo com gzip
        with open(local_file, 'rb') as f_in:
            with gzip.open(local_file_gz, 'wb') as f_out:
                f_out.writelines(f_in)
        
        # Upload para S3
        self.s3_client.upload_file(
            Filename=local_file_gz,
            Bucket=self.s3_bucket,
            Key=file_key,
            ExtraArgs={
                'ContentType': 'application/gzip',
                'ContentEncoding': 'gzip'
            }
        )
        
        # Remove arquivos locais
        if os.path.exists(local_file):
            os.remove(local_file)
        if os.path.exists(local_file_gz):
            os.remove(local_file_gz)
        
        s3_path = f"s3://{self.s3_bucket}/{file_key}"
        self.logger.info(f"Data saved to {s3_path} (CSV format with gzip compression)")
        
        return s3_path
    
    def extract_table_chunked(self, schema, table, execution_date, chunk_size=100000):
        """Extrai tabelas grandes em chunks para economizar memória"""
        
        self.logger.info(f"Starting chunked extraction for {schema}.{table} (chunk_size={chunk_size})")
        
        # Identifica coluna incremental
        incremental_col = self.get_incremental_column(schema, table)
        
        if incremental_col:
            # Extração incremental
            last_timestamp = self.get_last_extraction_timestamp(schema, table)
            
            query = f"""
            SELECT * FROM {schema}.{table}
            WHERE {incremental_col} > TO_TIMESTAMP(:last_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS')
            ORDER BY {incremental_col}
            """
            
            params = {'last_timestamp': last_timestamp}
            extraction_type = 'incremental'
            
        else:
            # Extração completa
            self.logger.warning(f"No incremental column found for {schema}.{table}. Performing full extract.")
            query = f"SELECT * FROM {schema}.{table}"
            params = {}
            extraction_type = 'full'
        
        file_paths = []
        chunk_number = 1
        
        # Processa em chunks
        try:
            for chunk_df in pd.read_sql(
                sql=query,
                con=self.oracle_hook.get_conn(),
                params=params,
                chunksize=chunk_size
            ):
                if not chunk_df.empty:
                    # Processa chunk
                    chunk_df = self._process_dataframe(chunk_df)
                    
                    # Salva chunk individual
                    chunk_file_path = self._save_chunk_to_s3_csv(
                        chunk_df, schema, table, execution_date, 
                        extraction_type, chunk_number
                    )
                    
                    file_paths.append(chunk_file_path)
                    chunk_number += 1
                    
                    self.logger.info(f"Processed chunk {chunk_number-1}: {len(chunk_df)} rows")
            
            # Atualiza timestamp se foi extração incremental
            if incremental_col:
                current_timestamp = execution_date.isoformat()
                self.save_extraction_timestamp(schema, table, current_timestamp)
            
            total_chunks = len(file_paths)
            self.logger.info(f"Chunked extraction completed: {total_chunks} chunks saved")
            
            return file_paths
            
        except Exception as e:
            self.logger.error(f"Error during chunked extraction: {str(e)}")
            raise
    
    def _save_chunk_to_s3_csv(self, df, schema, table, execution_date, extraction_type, chunk_number):
        """Salva um chunk individual no S3"""
        
        # Define caminho do arquivo com número do chunk
        date_partition = execution_date.strftime('%Y/%m/%d')
        timestamp = execution_date.strftime('%Y%m%d_%H%M%S')
        
        file_key = f"{self.s3_prefix}/data/{schema.lower()}/{table.lower()}/{date_partition}/{extraction_type}_{timestamp}_chunk_{chunk_number:04d}.csv.gz"
        
        # Salva localmente primeiro
        local_file = f"/tmp/{schema}_{table}_{timestamp}_chunk_{chunk_number}.csv"
        local_file_gz = f"/tmp/{schema}_{table}_{timestamp}_chunk_{chunk_number}.csv.gz"
        
        # Salva CSV (sem cabeçalho para chunks após o primeiro)
        include_header = chunk_number == 1
        
        df.to_csv(
            local_file,
            sep=';',
            index=False,
            header=include_header,
            quoting=csv.QUOTE_ALL,
            quotechar='"',
            doublequote=True,
            encoding='utf-8',
            na_rep='',
            lineterminator='\n',
            date_format='%Y-%m-%d %H:%M:%S'
        )
        
        # Compacta com gzip
        with open(local_file, 'rb') as f_in:
            with gzip.open(local_file_gz, 'wb') as f_out:
                f_out.writelines(f_in)
        
        # Upload para S3
        self.s3_client.upload_file(
            Filename=local_file_gz,
            Bucket=self.s3_bucket,
            Key=file_key,
            ExtraArgs={
                'ContentType': 'application/gzip',
                'ContentEncoding': 'gzip'
            }
        )
        
        # Remove arquivos locais
        if os.path.exists(local_file):
            os.remove(local_file)
        if os.path.exists(local_file_gz):
            os.remove(local_file_gz)
        
        s3_path = f"s3://{self.s3_bucket}/{file_key}"
        return s3_path