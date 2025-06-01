import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime
import sys
import os

# Adiciona o diretório dos DAGs ao path
sys.path.append('/opt/airflow/dags')

from oracle_extractor import OracleExtractor
from snowflake_loader import SnowflakeLoader

class TestOracleExtractor(unittest.TestCase):
    
    def setUp(self):
        """Setup para testes do OracleExtractor"""
        self.extractor = OracleExtractor(
            connection_id='test_oracle_conn',
            s3_bucket='test-bucket',
            s3_prefix='test-prefix'
        )
    
    @patch('oracle_extractor.OracleHook')
    @patch('oracle_extractor.boto3.client')
    def test_get_incremental_column(self, mock_s3, mock_oracle_hook):
        """Testa identificação de coluna incremental"""
        
        # Mock do retorno do Oracle
        mock_oracle_hook.return_value.get_first.return_value = ['UPDATED_AT']
        
        result = self.extractor.get_incremental_column('TEST_SCHEMA', 'TEST_TABLE')
        
        self.assertEqual(result, 'UPDATED_AT')
        mock_oracle_hook.return_value.get_first.assert_called_once()
    
    @patch('oracle_extractor.pd.read_sql')
    @patch('oracle_extractor.OracleHook')
    @patch('oracle_extractor.boto3.client')
    def test_extract_table_incremental(self, mock_s3, mock_oracle_hook, mock_read_sql):
        """Testa extração incremental"""
        
        # Mock dos dados de teste
        test_data = pd.DataFrame({
            'ID': [1, 2, 3],
            'NAME': ['Test1', 'Test2', 'Test3'],
            'UPDATED_AT': [datetime.now(), datetime.now(), datetime.now()]
        })
        
        mock_read_sql.return_value = test_data
        mock_oracle_hook.return_value.get_first.return_value = ['UPDATED_AT']
        
        # Mock S3 operations
        mock_s3.return_value.get_object.side_effect = Exception("No previous extraction")
        mock_s3.return_value.upload_file.return_value = None
        mock_s3.return_value.put_object.return_value = None
        
        execution_date = datetime.now()
        
        with patch.object(self.extractor, '_save_to_s3') as mock_save:
            mock_save.return_value = 's3://test-bucket/test-file.parquet'
            
            result = self.extractor.extract_table_incremental(
                'TEST_SCHEMA', 'TEST_TABLE', execution_date
            )
            
            self.assertIsNotNone(result)
            mock_save.assert_called_once()
    
    def test_process_dataframe(self):
        """Testa processamento do DataFrame"""
        
        # DataFrame com dados problemáticos
        test_df = pd.DataFrame({
            'ID': [1, 2, None],
            'NAME': ['Test1', None, 'Test3'],
            'VALUE': [10.5, None, 20.0]
        })
        
        processed_df = self.extractor._process_dataframe(test_df)
        
        # Verifica se valores nulos foram tratados
        self.assertEqual(processed_df['ID'].iloc[2], 0)  # Numérico → 0
        self.assertEqual(processed_df['NAME'].iloc[1], '')  # String → ''
        self.assertEqual(processed_df['VALUE'].iloc[1], 0)  # Float → 0.0

class TestSnowflakeLoader(unittest.TestCase):
    
    def setUp(self):
        """Setup para testes do SnowflakeLoader"""
        self.loader = SnowflakeLoader(connection_id='test_snowflake_conn')
    
    @patch('snowflake_loader.SnowflakeHook')
    def test_create_schema_if_not_exists(self, mock_snowflake_hook):
        """Testa criação de schema"""
        
        self.loader.create_schema_if_not_exists('test_schema')
        
        mock_snowflake_hook.return_value.run.assert_called_with(
            "CREATE SCHEMA IF NOT EXISTS test_schema"
        )
    
    @patch('snowflake_loader.SnowflakeHook')
    def test_copy_from_s3(self, mock_snowflake_hook):
        """Testa COPY FROM S3"""
        
        mock_snowflake_hook.return_value.run.return_value = "Success"
        mock_snowflake_hook.return_value.get_records.return_value = [
            ['ID', 'NUMBER'], ['NAME', 'STRING']
        ]
        
        result = self.loader.copy_from_s3(
            's3://test-bucket/test-file.parquet',
            'test_schema',
            'test_table'
        )
        
        self.assertEqual(result, "Success")
    
    def test_map_parquet_to_snowflake_type(self):
        """Testa mapeamento de tipos"""
        
        # Testa mapeamentos conhecidos
        self.assertEqual(
            self.loader._map_parquet_to_snowflake_type('BIGINT'),
            'NUMBER(19,0)'
        )
        self.assertEqual(
            self.loader._map_parquet_to_snowflake_type('STRING'),
            'VARCHAR(16777216)'
        )
        # Testa tipo desconhecido
        self.assertEqual(
            self.loader._map_parquet_to_snowflake_type('UNKNOWN'),
            'VARCHAR(16777216)'
        )

class TestDataValidation(unittest.TestCase):
    """Testes para validação de dados"""
    
    def test_data_quality_validation(self):
        """Testa validação de qualidade dos dados"""
        
        # Simula dados válidos
        valid_data = pd.DataFrame({
            'ID': range(1000),  # 1000 registros
            'STATUS': ['ACTIVE'] * 1000
        })
        
        # Validações básicas
        self.assertGreater(len(valid_data), 100)  # Mínimo de registros
        self.assertFalse(valid_data['ID'].duplicated().any())  # Sem duplicatas
        self.assertFalse(valid_data.isnull().all().any())  # Sem colunas completamente nulas
    
    def test_incremental_logic(self):
        """Testa lógica de extração incremental"""
        
        # Simula timestamps
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        last_extraction = datetime(2024, 1, 1, 10, 0, 0)
        
        # Dados que devem ser extraídos (após last_extraction)
        new_data = pd.DataFrame({
            'ID': [1, 2, 3],
            'UPDATED_AT': [
                datetime(2024, 1, 1, 11, 0, 0),  # Deve ser incluído
                datetime(2024, 1, 1, 9, 0, 0),   # Não deve ser incluído
                datetime(2024, 1, 1, 13, 0, 0)   # Deve ser incluído
            ]
        })
        
        # Filtra dados incremental
        incremental_data = new_data[new_data['UPDATED_AT'] > last_extraction]
        
        self.assertEqual(len(incremental_data), 2)  # Apenas 2 registros novos

class TestEndToEndIntegration(unittest.TestCase):
    """Testes de integração end-to-end"""
    
    @patch('oracle_extractor.OracleHook')
    @patch('oracle_extractor.boto3.client')
    @patch('snowflake_loader.SnowflakeHook')
    def test_full_pipeline_flow(self, mock_snowflake_hook, mock_s3, mock_oracle_hook):
        """Testa fluxo completo do pipeline"""
        
        # Setup mocks
        test_data = pd.DataFrame({
            'ID': [1, 2, 3],
            'NAME': ['A', 'B', 'C'],
            'UPDATED_AT': [datetime.now()] * 3
        })
        
        # Mock Oracle extraction
        with patch('oracle_extractor.pd.read_sql') as mock_read_sql:
            mock_read_sql.return_value = test_data
            mock_oracle_hook.return_value.get_first.return_value = ['UPDATED_AT']
            
            # Mock S3 operations
            mock_s3.return_value.get_object.side_effect = Exception("No previous")
            mock_s3.return_value.upload_file.return_value = None
            mock_s3.return_value.put_object.return_value = None
            
            # Mock Snowflake load
            mock_snowflake_hook.return_value.run.return_value = "Success"
            mock_snowflake_hook.return_value.get_records.return_value = [
                ['ID', 'NUMBER'], ['NAME', 'STRING']
            ]
            
            # Execute pipeline steps
            extractor = OracleExtractor('oracle_conn', 'test-bucket', 'test-prefix')
            loader = SnowflakeLoader('snowflake_conn')
            
            # Extract
            with patch.object(extractor, '_save_to_s3') as mock_save:
                mock_save.return_value = 's3://test-bucket/test.parquet'
                
                s3_path = extractor.extract_table_incremental(
                    'TEST_SCHEMA', 'TEST_TABLE', datetime.now()
                )
                
                # Load
                result = loader.copy_from_s3(s3_path, 'test_schema', 'test_table')
                
                self.assertIsNotNone(s3_path)
                self.assertEqual(result, "Success")

def run_pipeline_tests():
    """Executa todos os testes do pipeline"""
    
    # Configuração de logging para testes
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # Suite de testes
    test_suite = unittest.TestSuite()
    
    # Adiciona testes
    test_suite.addTest(unittest.makeSuite(TestOracleExtractor))
    test_suite.addTest(unittest.makeSuite(TestSnowflakeLoader))
    test_suite.addTest(unittest.makeSuite(TestDataValidation))
    test_suite.addTest(unittest.makeSuite(TestEndToEndIntegration))
    
    # Executa testes
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Retorna status
    return result.wasSuccessful()

# DAG para executar testes no Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

test_dag = DAG(
    'oracle_snowflake_tests',
    default_args={
        'owner': 'data-team',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=2)
    },
    description='Test Oracle to Snowflake pipeline',
    schedule_interval=None,  # Manual execution only
    catchup=False,
    tags=['testing', 'oracle', 'snowflake']
)

def run_unit_tests(**context):
    """Task para executar testes unitários"""
    
    success = run_pipeline_tests()
    
    if not success:
        raise Exception("Unit tests failed!")
    
    return "All tests passed!"

def validate_connections(**context):
    """Task para validar conexões"""
    
    from airflow.providers.oracle.hooks.oracle import OracleHook
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    try:
        # Testa conexão Oracle
        oracle_hook = OracleHook(oracle_conn_id='oracle_conn')
        oracle_conn = oracle_hook.get_conn()
        oracle_conn.close()
        
        # Testa conexão Snowflake
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        snowflake_hook.run("SELECT 1")
        
        return "All connections validated successfully!"
        
    except Exception as e:
        raise Exception(f"Connection validation failed: {str(e)}")

def dry_run_extraction(**context):
    """Task para executar dry run da extração"""
    
    from oracle_extractor import OracleExtractor
    
    # Testa apenas metadados, sem extrair dados
    extractor = OracleExtractor('oracle_conn', 'test-bucket', 'test-prefix')
    
    test_schema = 'YOUR_TEST_SCHEMA'
    test_table = 'YOUR_TEST_TABLE'
    
    try:
        # Testa busca de metadados
        metadata = extractor.get_table_metadata(test_schema, test_table)
        incremental_col = extractor.get_incremental_column(test_schema, test_table)
        
        context['task_instance'].xcom_push(
            key='test_results',
            value={
                'schema': test_schema,
                'table': test_table,
                'columns_count': len(metadata),
                'incremental_column': incremental_col
            }
        )
        
        return f"Dry run successful: {len(metadata)} columns found, incremental column: {incremental_col}"
        
    except Exception as e:
        raise Exception(f"Dry run failed: {str(e)}")

# Tasks do DAG de teste
unit_tests_task = PythonOperator(
    task_id='run_unit_tests',
    python_callable=run_unit_tests,
    dag=test_dag
)

validate_connections_task = PythonOperator(
    task_id='validate_connections',
    python_callable=validate_connections,
    dag=test_dag
)

dry_run_task = PythonOperator(
    task_id='dry_run_extraction',
    python_callable=dry_run_extraction,
    dag=test_dag
)

# Fluxo dos testes
unit_tests_task >> validate_connections_task >> dry_run_task

if __name__ == '__main__':
    # Permite executar testes localmente
    run_pipeline_tests()