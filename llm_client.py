
import boto3
from botocore.exceptions import ClientError
import json
from typing import Dict, Any, Optional, List
from openai import OpenAI
import tiktoken
from config import Config
def get_secret(secret_name: str):

    #secret_name = "GPT5_nano_api"
    region_name = "eu-south-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    
    # Parse the JSON secret and extract the API key
    secret_dict = json.loads(secret)
    
    # Extract the API key from the JSON (assuming the key name matches the secret_name)
    if secret_name in secret_dict:
        return secret_dict[secret_name]
    else:
        # If exact match not found, try to get the first value (common case)
        return list(secret_dict.values())[0]

class LLMClient:
    """
    Client for interacting with Large Language Models (OpenAI GPT).
    Handles prompt creation, API calls, and response validation.
    """
    
    def __init__(self, secret_name: str):
        """
        Initialize the LLM client.
        
        Args:
            config: Configuration object containing API keys and settings
        """
        llm_api = get_secret(secret_name)
        if(Config.OPENAI_MODEL=="deepseek-chat"):
            self.client = OpenAI(api_key=llm_api, base_url="https://api.deepseek.com")
        else:
            self.client = OpenAI(api_key=llm_api)
    
    def create_user_prompt(self, batch) -> str:
        """
        Create a user prompt for invoice categorization.
        
        Args:
            batch: DataFrame row containing invoice information

        Returns:
            Formatted prompt string
        """
        user_prompt = "\n".join(
    f"id {id} | Ragione sociale: {row['RAGIONE_SOCIALE']} | Descrizione: {row['DESCRIZIONE']}"
    for id, row in batch.iterrows()
)

        return user_prompt

    def count_tokens(self, text: str, model: str = "gpt-4") -> int:
        """
        Count tokens in a text string.
        
        Args:
            text: Text to count tokens for
            model: Model name for tokenizer (default: gpt-4)
            
        Returns:
            Number of tokens
        """
        if tiktoken:
            # Use tiktoken for accurate token counting
            try:
                encoding = tiktoken.encoding_for_model(model)
                return len(encoding.encode(text))
            except KeyError:
                # Fallback to cl100k_base encoding if model not found
                encoding = tiktoken.get_encoding("cl100k_base")
                return len(encoding.encode(text))
        else:
            # Rough approximation: ~4 characters per token
            return len(text) // 4
    
    def estimate_request_tokens(self, user_prompt: str, system_prompt: str, model: str = "gpt-4") -> dict:
        """
        Estimate total tokens for a request before sending.
        
        Args:
            user_prompt: User message content
            system_prompt: System message content  
            model: Model name
            
        Returns:
            Dictionary with token counts and estimates
        """
        system_tokens = self.count_tokens(system_prompt, model)
        user_tokens = self.count_tokens(user_prompt, model)
        
        # Add overhead for message structure (role, content keys, etc.)
        message_overhead = 10  # Approximate overhead per message
        total_input_tokens = system_tokens + user_tokens + message_overhead
        
        # Estimate output tokens based on batch size (rough estimate)
        batch_lines = user_prompt.count('\n') + 1
        estimated_output_tokens = batch_lines * 15  # ~15 tokens per categorization response
        
        return {
            "system_tokens": system_tokens,
            "user_tokens": user_tokens, 
            "total_input_tokens": total_input_tokens,
            "estimated_output_tokens": estimated_output_tokens,
            "estimated_total_tokens": total_input_tokens + estimated_output_tokens
        }
    
    def check_rate_limit(self, user_prompt: str, system_prompt: str, 
                        tokens_per_minute: int = 10000, model: str = "gpt-4") -> bool:
        """
        Check if request would exceed rate limit.
        
        Args:
            user_prompt: User message
            system_prompt: System message
            tokens_per_minute: Rate limit (default 10k TPM)
            model: Model name
            
        Returns:
            True if request is within limits, False otherwise
        """
        estimate = self.estimate_request_tokens(user_prompt, system_prompt, model)
        return estimate["estimated_total_tokens"] <= tokens_per_minute

    def get_response(self, user_prompt: str, 
                    system_prompt: str):
        """
        Get response from the LLM.
        """
        response = self.client.chat.completions.create(
                model=Config.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={ "type": "json_object" }   # force valid JSON output
                
            )
        try: 
            validated_response = self.validate_response(response)

        except ValueError as e:
            print(f"❌ Response validation error: {e}")
            print(f"Response content: {response.choices[0].message.content}")
            raise e    
        return validated_response
        


    def validate_response(self, response) -> List[Dict[str, Any]]:
        """
        Validate the LLM response format and content.
        
        Args:
            response: Raw response from the LLM
            
        Returns:
            Validated responses as a list of dictionaries
        Raises:
            ValueError: If the response format is invalid   
        """
        response_content = response.choices[0].message.content
        result = json.loads(response_content)
         # Validate top-level JSON object
        if not isinstance(result, dict) and not isinstance(result, list):
         raise ValueError("Top-level response is not a JSON object")
    
        # Try common keys for response data
        response_data = None
        for key in ['response', 'data', 'output', 'results','Response content']:
            if key in result and isinstance(result[key], list):
                response_data = result[key]
                break
    
        if response_data is None:
            # Fallback: check if the object itself is a list of dicts with the required keys
            if isinstance(result, list) and all(
                isinstance(item, dict) and 
                'id' in item and 
                'CATEGORIA' in item
                for item in result
            ):
                response_data = result
            else:
                print(response_data)
                raise ValueError("Could not locate valid response data in JSON")
                
        
        # Validate each item in response
        validated_items = []
        for item in response_data:
            if not isinstance(item, dict):
                continue
            if 'id' not in item or 'CATEGORIA' not in item:
                continue
            validated_items.append({
                'id': int(item['id']),
                'CATEGORIA': str(item['CATEGORIA'])
            })
        
        if not validated_items:
            raise ValueError("No valid items found in response")
        return validated_items
        


if __name__ == "__main__":
    secret = get_secret("deepseek_api")
    print(secret)
