import os
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import json
from lambda_function import categorize_invoices, process_batch, process_batches_in_parallel, identify_trigger, handle_jump
from llm_client import LLMClient
from config import Config
from data_processor import DataProcessor as dp


class TestProcessBatch:
    """Test suite for the process_batch function"""
    
    @pytest.fixture
    def mock_llm_client(self):
        """Create a mock LLM client for testing"""
        mock_client = Mock(spec=LLMClient)
        # Configure the actual methods that exist in LLMClient
        mock_client.create_user_prompt = Mock()
        mock_client.get_response = Mock()
        return mock_client
    
    @pytest.fixture
    def sample_batch(self):
        """Create a sample batch DataFrame for testing"""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'RAGIONE_SOCIALE': ['ENEL ENERGIA', 'FORNITORE ALIMENTARI SRL', 'MECCANICO ROSSI'],
            'DESCRIZIONE': ['Fornitura energia elettrica', 'Acquisto prodotti freschi', 'Riparazione automobile'],
            'CATEGORIA': [None, None, None]  # Initially empty categories
        }).set_index('id')
    
    @pytest.fixture
    def sample_llm_response(self):
        """Sample LLM response in expected format"""
        return [
            {'id': 1, 'CATEGORIA': 'Energia Elettrica'},
            {'id': 2, 'CATEGORIA': 'Food'}, 
            {'id': 3, 'CATEGORIA': 'Automezzi'}
        ]

    def test_process_batch_success(self, mock_llm_client, sample_batch, sample_llm_response):
        """Test successful batch processing"""
        # Setup mocks
        mock_llm_client.create_user_prompt.return_value = "Test prompt"
        mock_llm_client.get_response.return_value = sample_llm_response
        
        # Execute function
        result = process_batch(sample_batch, mock_llm_client)
        
        # Assertions
        assert isinstance(result, pd.DataFrame)
        assert result.loc[1, 'CATEGORIA'] == 'Energia Elettrica'
        assert result.loc[2, 'CATEGORIA'] == 'Food'
        assert result.loc[3, 'CATEGORIA'] == 'Automezzi'
        
        # Verify LLM client calls
        mock_llm_client.create_user_prompt.assert_called_once_with(sample_batch)
        mock_llm_client.get_response.assert_called_once_with("Test prompt", Config.SYSTEM_PROMPT)
        
        print("✅ Successfully processed batch with LLM categorization")

    def test_process_batch_with_retries(self, mock_llm_client, sample_batch, sample_llm_response):
        """Test batch processing with LLM retries on failure"""
        # Setup mock to fail twice then succeed
        mock_llm_client.create_user_prompt.return_value = "Test prompt"
        mock_llm_client.get_response.side_effect = [
            Exception("API Error 1"),
            Exception("API Error 2"), 
            sample_llm_response  # Success on 3rd try
        ]
        
        # Execute function
        result = process_batch(sample_batch, mock_llm_client)
        
        # Assertions
        assert isinstance(result, pd.DataFrame)
        assert result.loc[1, 'CATEGORIA'] == 'Energia Elettrica'
        
        # Verify retry behavior (should be called 3 times)
        assert mock_llm_client.get_response.call_count == 3
        
        print("✅ Successfully handled LLM retries")

    def test_process_batch_exhausted_retries(self, mock_llm_client, sample_batch):
        """Test batch processing when all retries are exhausted"""
        # Setup mock to always fail
        mock_llm_client.create_user_prompt.return_value = "Test prompt"
        mock_llm_client.get_response.side_effect = Exception("Persistent API Error")
        
        # Execute and expect exception
        with pytest.raises(Exception, match="Persistent API Error"):
            process_batch(sample_batch, mock_llm_client)
        
        # Verify all retries were attempted (Config.LLM_Tries = 3)
        assert mock_llm_client.get_response.call_count == Config.LLM_Tries
        
        print("✅ Correctly handled exhausted retries")

    def test_process_batch_partial_response(self, mock_llm_client, sample_batch):
        """Test batch processing with partial LLM response (missing some IDs)"""
        # Setup partial response (missing id=2)
        partial_response = [
            {'id': 1, 'CATEGORIA': 'Energia Elettrica'},
            {'id': 3, 'CATEGORIA': 'Automezzi'}
        ]
        
        mock_llm_client.create_user_prompt.return_value = "Test prompt"
        mock_llm_client.get_response.return_value = partial_response
        
        # Execute function
        result = process_batch(sample_batch, mock_llm_client)
        
        # Assertions
        assert result.loc[1, 'CATEGORIA'] == 'Energia Elettrica'
        assert pd.isna(result.loc[2, 'CATEGORIA'])  # Should remain None/NaN
        assert result.loc[3, 'CATEGORIA'] == 'Automezzi'
        
        print("✅ Correctly handled partial LLM response")

    def test_process_batch_empty_batch(self, mock_llm_client):
        """Test processing an empty batch"""
        empty_batch = pd.DataFrame({
            'RAGIONE_SOCIALE': [],
            'DESCRIZIONE': [], 
            'CATEGORIA': []
        })
        
        mock_llm_client.create_user_prompt.return_value = "Test prompt"
        mock_llm_client.get_response.return_value = []
        
        # Execute function
        result = process_batch(empty_batch, mock_llm_client)
        
        # Assertions
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        
        print("✅ Correctly handled empty batch")

    @patch('lambda_function.Config')
    def test_process_batch_config_usage(self, mock_config, mock_llm_client, sample_batch, sample_llm_response):
        """Test that process_batch uses Config values correctly"""
        # Setup config mock
        mock_config.SYSTEM_PROMPT = "Test system prompt"
        mock_config.LLM_Tries = 2
        
        mock_llm_client.create_user_prompt.return_value = "Test prompt"
        mock_llm_client.get_response.return_value = sample_llm_response
        
        # Execute function
        result = process_batch(sample_batch, mock_llm_client)
        
        # Verify config usage
        mock_llm_client.get_response.assert_called_with("Test prompt", "Test system prompt")
        
        print("✅ Correctly used Config values")


class TestCategorizeAndSaveToS3:
    """Test suite for the categorize_and_save_to_s3 function"""
    
    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client for testing"""
        mock_s3 = Mock()
        mock_s3.get_latest_parquet_file_key = Mock()
        mock_s3.read_parquet_to_dataframe = Mock()
        mock_s3.write_df_to_parquet = Mock()
        return mock_s3
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create sample invoice DataFrame"""
        return pd.DataFrame({
            'P_IVA': ['12345678901', '98765432109'],
            'RAGIONE_SOCIALE': ['ENEL ENERGIA', 'FORNITORE SRL'],
            'DESCRIZIONE': ['Fornitura energia elettrica', 'Prodotti alimentari'],
            'IMPORTO_TOTALE_DOCUMENTO': [150.00, 75.50]
        })

    @patch('lambda_function.LLMClient')
    @patch('lambda_function.dp')
    def test_categorize_and_save_to_s3_error_handling(self, mock_dp, mock_llm_class, mock_s3_client, sample_dataframe):
        """Test error handling when categorize_invoices fails"""
        
        # Setup S3 mock to return sample data
        mock_s3_client.get_latest_parquet_file_key.return_value = "test/path/file.parquet"
        mock_s3_client.read_parquet_to_dataframe.return_value = sample_dataframe
        
        # Setup data processor mocks
        clustered_data = sample_dataframe.copy()
        clustered_data['cluster'] = [1, 2]
        mock_dp.sequential_cluster.return_value = clustered_data
        mock_dp.representatives.return_value = sample_dataframe
        
        # Setup LLM client mock to raise an exception during categorization
        mock_llm_instance = Mock()
        mock_llm_class.return_value = mock_llm_instance
        
        # Mock categorize_invoices to raise an exception
        with patch('lambda_function.categorize_invoices') as mock_categorize:
            mock_categorize.side_effect = Exception("LLM API connection failed")
            
            # Execute function
            from lambda_function import categorize_and_save_to_s3
            result = categorize_and_save_to_s3("test_company", "2025-01-01", mock_s3_client)
        
        # Assertions
        assert result["statusCode"] == 500
        assert result["body"] == "Error occurred while processing"
        
        # Verify that S3 operations were called but write was not (due to error)
        mock_s3_client.get_latest_parquet_file_key.assert_called_once_with(
            "we-are-soda-datalake", "test_company", "2025-01-01"
        )
        mock_s3_client.read_parquet_to_dataframe.assert_called_once()
        mock_s3_client.write_df_to_parquet.assert_not_called()  # Should not reach write due to error
        
        # Verify categorize_invoices was attempted
        mock_categorize.assert_called_once()
        
        print("✅ Successfully handled categorization error")

    @patch('lambda_function.LLMClient')
    @patch('lambda_function.dp')
    def test_categorize_and_save_to_s3_success_case(self, mock_dp, mock_llm_class, mock_s3_client, sample_dataframe):
        """Test successful categorization and save to S3"""
        
        # Setup S3 mock to return sample data
        mock_s3_client.get_latest_parquet_file_key.return_value = "test/path/original_file.parquet"
        mock_s3_client.read_parquet_to_dataframe.return_value = sample_dataframe
        
        # Setup data processor mocks
        clustered_data = sample_dataframe.copy()
        clustered_data['cluster'] = [1, 2]
        mock_dp.sequential_cluster.return_value = clustered_data
        mock_dp.representatives.return_value = sample_dataframe
        
        # Setup successful categorization
        categorized_data = sample_dataframe.copy()
        categorized_data['CATEGORIA'] = ['Energia Elettrica', 'Food']
        categorized_data['cluster'] = [1, 2]
        
        # Setup LLM client mock
        mock_llm_instance = Mock()
        mock_llm_class.return_value = mock_llm_instance
        
        # Mock successful categorize_invoices
        with patch('lambda_function.categorize_invoices') as mock_categorize:
            mock_categorize.return_value = categorized_data
            
            # Execute function
            from lambda_function import categorize_and_save_to_s3
            result = categorize_and_save_to_s3("test_company", "2025-01-01", mock_s3_client)
        
        # Assertions for successful execution
        assert result["statusCode"] == 200
        assert "original_shape" in result
        assert "reduced_shape" in result
        assert "body" in result
        
        # Verify S3 write was called with correct parameters
        mock_s3_client.write_df_to_parquet.assert_called_once()
        write_call_args = mock_s3_client.write_df_to_parquet.call_args
        
        # Check that the file key follows expected pattern
        expected_key = "test_company/silver/estratto_fatture_categorizzato/PARTITION_DATE=2025-01-01/original_file_cat.parquet"
        assert write_call_args[0][1] == "we-are-soda-datalake"  # bucket
        assert write_call_args[0][2] == expected_key  # key
        
        print("✅ Successfully tested successful categorization flow")

def read_parquet():
    test_data_path = os.path.join(os.path.dirname(__file__), 'mocks', 'data')
    parquet_files = [f for f in os.listdir(test_data_path) if f.endswith('.parquet')]
     # Read all parquet files and concatenate them
    dataframes = []
    for file in parquet_files:
        df = pd.read_parquet(os.path.join(test_data_path, file))
        dataframes.append(df)
        
    # Combine all dataframes
    combined_df = pd.concat(dataframes, ignore_index=True)        
    return combined_df
    
    

def test_categorize():
    """Placeholder test for categorize_invoices function"""
    # Read test data from mocks/data directory
    test_data_path = os.path.join(os.path.dirname(__file__), 'mocks', 'data')
    df = read_parquet()

    # Basic test that df was loaded successfully
    assert not df.empty, "Test dataframe should not be empty"
    assert 'RAGIONE_SOCIALE' in df.columns, "Expected column RAGIONE_SOCIALE not found"
    print(f"✅ Successfully loaded test data with {len(df)} rows")

     # Process invoices 
    clustered_fatture = dp.sequential_cluster(df, threshold=0.8)
    reduced_row = dp.representatives(clustered_fatture)
    llm_client = LLMClient(Config.SECRET_NAME)
    company = "test_company"

    try:
        reduced_row_categ = categorize_invoices(reduced_row, company, llm_client)

    except Exception as e:
        print(f"Error occurred while categorizing invoices: {e}")
        return {"statusCode": 500, "body": "Error occurred while processing"}   

     # propagate categories to clustered_fatture
    clustered_with_cat = clustered_fatture.drop(
        columns=["CATEGORIA"], errors='ignore'
        ).merge(
        reduced_row_categ[["P_IVA", "cluster", "CATEGORIA"]],
        on=["P_IVA", "cluster"],
        how="left"
                )
    
    clustered_with_cat = clustered_with_cat.drop(columns=["cluster"], errors='ignore')
    print("final df shape:", clustered_with_cat.shape)
    clustered_with_cat.to_csv("categorized_output.csv", index=False)

    pass

def test_categorize_100():
    """Placeholder test for categorize_invoices function"""
    # Read test data from mocks/data directory
    test_data_path = os.path.join(os.path.dirname(__file__), 'mocks', 'data')
    df = read_parquet()

    # Basic test that df was loaded successfully
    assert not df.empty, "Test dataframe should not be empty"
    assert 'RAGIONE_SOCIALE' in df.columns, "Expected column RAGIONE_SOCIALE not found"
    print(f"✅ Successfully loaded test data with {len(df)} rows")

    df = df.sample(100)
    print(f"Sampled df shape: {df.shape}")

     # Process invoices 
    clustered_fatture = dp.sequential_cluster(df, threshold=0.8)
    reduced_row = dp.representatives(clustered_fatture)
    llm_client = LLMClient(Config.SECRET_NAME)
    company = "test_company"

    try:
        
        reduced_row_categ = categorize_invoices(reduced_row, company, llm_client)

    except Exception as e:
        import traceback
        print(f"Error occurred while categorizing invoices: {e}")
        print(f"Error type: {type(e).__name__}")
        print("Full traceback:")
        traceback.print_exc()
        return {"statusCode": 500, "body": "Error occurred while processing"}   

     # propagate categories to clustered_fatture
    clustered_with_cat = clustered_fatture.drop(
        columns=["CATEGORIA"], errors='ignore'
        ).merge(
        reduced_row_categ[["P_IVA", "cluster", "CATEGORIA"]],
        on=["P_IVA", "cluster"],
        how="left"
                )
    
    clustered_with_cat = clustered_with_cat.drop(columns=["cluster"], errors='ignore')
    print("final df shape:", clustered_with_cat.shape)
    result_data = clustered_with_cat.to_dict(orient='records')
    print("✅ Successfully processed and saved categorized data")
    print(result_data)
    pass

def test_jump_trigger_identification():
    """
    Test that identify_trigger correctly identifies a 'jump' event
    """
    # Create a jump event structure
    jump_event = {
        'headers': {
            'Content-Type': 'application/json',
            'User-Agent': 'test-agent'
        },
        'body': {
            'destination': 'we-are-soda-datalake/meh/silver/estratto_fatture/2025-01-01/fatture_cat_IA.json',
            'source': 'we-are-soda-datalake/meh/silver/estratto_fatture/2025-01-01'
        }
    }
    
    # Test the identify_trigger function
    trigger_type = identify_trigger(jump_event)
    
    # Assert that it correctly identifies as 'jump'
    assert trigger_type == 'jump', f"Expected 'jump' but got '{trigger_type}'"
    print("✅ Successfully identified jump trigger")



