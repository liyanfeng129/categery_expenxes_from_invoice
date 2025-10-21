import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from llm_client import LLMClient
import pandas as pd
import pytest

def test_initialization():
    secret_name = "GPT5_nano_api"
    llm_client = LLMClient(secret_name)
    assert llm_client.client is not None
    assert llm_client.client is not None
    # Test actual connection to OpenAI with a simple request
    try:
            test_response = llm_client.client.chat.completions.create(
                model="gpt-5-nano",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": "Say 'Hello, test!' and nothing else."}
                ]
                
               
            )
            
            # Verify we got a response
            assert test_response is not None
            assert test_response.choices is not None
            assert len(test_response.choices) > 0
            assert test_response.choices[0].message is not None
            assert test_response.choices[0].message.content is not None
            
            # Verify the response content
            response_content = test_response.choices[0].message.content.strip()
            print(f"Response content: {response_content}")
            assert "Hello, test!" in response_content
            
            print(f"✓ OpenAI connection successful. Response: {response_content}")
            print(f"✓ Tokens used: {test_response.usage.total_tokens}")
            
            
    except Exception as e:
        pytest.fail(f"OpenAI connection test failed: {str(e)}")    


def test_create_user_prompt():
    secret_name = "GPT5_nano_api"
    llm_client = LLMClient(secret_name)

    # Create a sample batch DataFrame
    sample_data = {
        "RAGIONE_SOCIALE": ["Company A", "Company B"],
        "DESCRIZIONE": ["Invoice for services rendered", "Invoice for goods sold"]
    }
    batch = pd.DataFrame(sample_data)

    # Generate user prompt
    user_prompt = llm_client.create_user_prompt(batch)
    print(f"User prompt:\n{user_prompt}")

    # Validate the user prompt format
    assert "id" in user_prompt
    assert "Ragione sociale" in user_prompt
    assert "Descrizione" in user_prompt

if __name__ == "__main__":
    test_initialization()
    test_create_user_prompt()