import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import json
from lambda_function import categorize_invoices, process_batch, process_batches_in_parallel, identify_trigger, handle_jump
from llm_client import LLMClient
from config import Config


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


def test_categorize():
    """Placeholder test for categorize_invoices function"""
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



