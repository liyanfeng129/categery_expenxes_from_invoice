import json
from s3_client import S3_Client
from llm_client import LLMClient
from data_processor import DataProcessor as dp
from company_cache import CompanyCache
import boto3
from botocore.exceptions import ClientError, BotoCoreError, NoCredentialsError
import numpy as np
import threading
import pandas as pd
from config import Config
import traceback
def lambda_handler(event, context):
    """
    Main handler that routes events from different triggers
    """
    
    # Identify the trigger source
    trigger_source = identify_trigger(event)
    
    # Route to appropriate handler
    if trigger_source == 'api_gateway':
        return handle_api_gateway(event, context)
    elif trigger_source == 's3':
        return handle_s3(event, context)
    elif trigger_source == 'jump':
        return handle_jump(event, context)
    else:
        return handle_unknown(event, context)
    
def handle_jump(event, context):
    """
    Handle Jump trigger
    """
    print("Handling Jump event")
    print(event)
    print(context)
    body = event['body']
    destination = body.get('destination')
    source = body.get('source')
    
    # Extract company and partition from source path
    # Source format: 'we-are-soda-datalake/meh/silver/estratto_fatture/2025-01-01'
    path_parts = source.split('/')
    company = path_parts[1]  # 'meh' - second part after bucket
    partition_date = path_parts[-1]  # '2025-01-01' - last part
    
    # Continue with processing using extracted values
    s3 = S3_Client(boto3.client('s3'))
    return [categorize_and_return_to_jump(company, partition_date, s3)]

def categorize_and_return_to_jump(company, partition, s3: S3_Client):
    """
    Categorize invoices and return results for Jump integration.
    """
    bucket = "we-are-soda-datalake"
    latest_file = s3.get_latest_parquet_file_key(bucket, company, partition)
    df = s3.read_parquet_to_dataframe(bucket, latest_file)
    # Process invoices
    clustered_fatture = dp.sequential_cluster(df, threshold=0.8)
    reduced_row = dp.representatives(clustered_fatture)
    llm_client = LLMClient(Config.SECRET_NAME)

    try:
        reduced_row_categ = categorize_invoices(reduced_row, company, llm_client)

    except Exception as e:
        
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
    
    """
    # Extract just the filename from the full S3 key path
    import os
    original_filename = os.path.basename(latest_file)
    
    if original_filename.endswith('.parquet'):
        filename_with_cat = original_filename[:-8] + '_cat.parquet'  # Remove .parquet, add _cat.parquet
    else:
        filename_with_cat = f"{original_filename}_cat"

    # Create new file key with categorized data structure
    new_file_key = f"{company}/silver/estratto_fatture_categorizzato/PARTITION_DATE={partition}/{filename_with_cat}"
    
    s3.write_df_to_parquet(clustered_with_cat, bucket, new_file_key) 
    """   
    
    return clustered_with_cat.to_dict(orient='records')
    

def handle_api_gateway(event, context):
    """
    Handle API Gateway trigger
    """
    print("Handling API Gateway event")
    print(event)
    print(context)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from API Gateway!')
    }
    


def handle_s3(event, context):
    """
    Handle S3 trigger
    """
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'File {key} uploaded to bucket {bucket}')
    }        

def handle_unknown(event, context):
    """
    Handle unknown trigger
    """
    print(event)
    print(context)
    return ["Jump dumb string"]

def identify_trigger(event):
    """
    Identify the source of the trigger based on event structure
    """
    if 'httpMethod' in event or 'requestContext' in event and 'apiId' in event['requestContext']:
        return 'api_gateway'
    elif 'Records' in event:
        first_record = event['Records'][0]
        if 's3' in first_record:
            return 's3'
        elif 'Sns' in first_record:
            return 'sns'
    elif 'trigger_source' in event:  # Custom trigger identifier
        return event['trigger_source']
    elif 'headers' in event and 'body' in event:
        body = event['body']
        if 'destination' in body and 'source' in body:
            return 'jump'
    return 'unknown'

def process_batches_in_parallel(batches, process_batch, llm_client: LLMClient) -> pd.DataFrame:
    """
    Process all batches in parallel using threads.

    Args:
        batches (list[pd.DataFrame]): list of batches (DataFrames).
        process_batch (callable): function that takes a batch (DataFrame) 
                                 and returns the updated batch (with categories).

    Returns:
        pd.DataFrame: concatenated DataFrame of all processed batches.
    """
    results = {}
    threads = []
    def worker(batch, idx):
        updated = process_batch(batch, llm_client)  # call LLM or custom function
        results[idx] = updated

    # Create and start threads
    for idx, batch in enumerate(batches):
        t = threading.Thread(target=worker, args=(batch, idx))
        t.start()
        threads.append(t)
    
    # Wait for all threads to finish
    for t in threads:
        t.join()    

     # Combine results in original order

    assert results, "empty results after processing"
    all_processed = pd.concat([results[i] for i in sorted(results.keys())], ignore_index=True)
    print("All processed:", all_processed.sample(min(100, len(all_processed))))
    return all_processed

def process_batch(batch, llm_client: LLMClient) -> pd.DataFrame:
    """
    Process a batch of data by sending it to an LLM for categorization.
    Args:
        batch: DataFrame containing the data to be categorized
            llm_client: Client object for interacting with the LLM API
            
        Returns:
            DataFrame: Updated batch with categorization results
    """
    system_prompt = Config.SYSTEM_PROMPT
    user_prompt = llm_client.create_user_prompt(batch)
    tries = Config.LLM_Tries
    # LLM could fail intermittently, so we retry a few times
    while(tries > 0):
        try:    
            response = llm_client.get_response(user_prompt, system_prompt)
            break  # Exit loop if successful
        except Exception as e:
            print(f"Error getting response from LLM: {e}")
            tries -= 1
            if tries == 0:
                raise e  # Re-raise exception if out of tries
    # If we reach here, it means we got a response
    # Update the batch with the response
    for item in response:
        batch.loc[item['id'], 'CATEGORIA'] = item['CATEGORIA']
        
    return batch

def categorize_invoices(processed_fatture, company_name, llm_client: LLMClient):
    """
    Categorize invoices based on supplier data.
    
    Parameters:
    - processed_fatture: DataFrame containing preprocessed invoice data
        it is repetition free
        -- has these colunm:
                            SourceName  
                            CODICE_COMMITTENTE  
                            ALIQUOTA_IVA       
                            DATA       
                            P_IVA 
                            UNITAMISURA  
                            IMPORTO_TOTALE_DOCUMENTO
                            NUMERO TIPO_DOCUMENTO                                
                            RAGIONE_SOCIALE 
                            SEDE_INDIRIZZO_CESSIONARIO  
                            QUANTITA                    
                            DESCRIZIONE  
                            IMPONIBILE_IMPORTO  
                            PREZZO_UNITARIO
                            cluster
    - company_cache: string
    
    Returns:
    - categorized_fatture: DataFrame with categorized invoices
    """
    #company_cache = CompanyCache(company_cache)
    batch_size = Config.BATCH_SIZE  # Adjust batch size as needed
    categorized_fatture = processed_fatture.copy()
    categorized_fatture['CATEGORIA'] = 'No categorizzato'  # Default value
    """
    cache_hit = 0  # Counter for cache hits
    for _, row in categorized_fatture.iterrows():
        cluster = row['cluster']
        piva = row['P_IVA']
        descrizione = row['DESCRIZIONE']
        
        # Check cache first
        if company_cache.has_category(piva, descrizione):
            categorized_fatture.loc[(categorized_fatture['cluster'] == cluster) & 
                                     (categorized_fatture['P_IVA'] == piva) & 
                                     (categorized_fatture['DESCRIZIONE'] == descrizione),
                                       'CATEGORIA'] = company_cache.get_category(piva, descrizione)
            cache_hit += 1
    print(f"Cache hits so far: {cache_hit}")
    """
    not_categorized = categorized_fatture[categorized_fatture["CATEGORIA"] == 'No categorizzato' ]

    if len(not_categorized) > 0:
        print(f"Not categorized items: {not_categorized.shape[0]}")
        batches = dp.split_into_batches(not_categorized, batch_size=batch_size)
        print(f"Total batches to process: {len(batches)}")

        # Process batches in parallel
        all_processed = process_batches_in_parallel(batches, process_batch, llm_client)
        print("all_processed columns:", all_processed.columns.tolist())
        """
        # Save categories to cache
        for _, row in all_processed.iterrows():
            piva = row['P_IVA']
            descrizione = row['DESCRIZIONE']
            categoria = row['CATEGORIA']
            company_cache.set_category(piva, descrizione, categoria)
        company_cache.save_cache()
        """    
        # Merge categories based on company + cluster
        # now categorized_fatture has temporary colunm Categoria_new
        categorized_fatture = categorized_fatture.merge(
        all_processed[["P_IVA", "cluster", "CATEGORIA"]],
        on=["P_IVA", "cluster"],
        how="left",
        suffixes=('', '_new')  # to avoid overwriting existing column yet
    )
        print("Merged columns:", categorized_fatture.columns.tolist())
        # If CATEGORIA_new is not NaN → use it
        # Else → keep old CATEGORIA (which is guaranteed to not be "No categorizzato")
        categorized_fatture['CATEGORIA'] = np.where(
        categorized_fatture['CATEGORIA_new'].notna(),
        categorized_fatture['CATEGORIA_new'],
        categorized_fatture['CATEGORIA']
    )

        # Drop temporary column
        categorized_fatture.drop(columns=['CATEGORIA_new'], inplace=True)
        not_categorized = categorized_fatture[categorized_fatture["CATEGORIA"] == 'No categorizzato' ]
        if len(not_categorized) > 0:
            raise ValueError("Some items were not categorized after LLM processing.")
    return categorized_fatture 
