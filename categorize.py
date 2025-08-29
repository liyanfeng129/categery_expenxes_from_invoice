
import argparse
import pandas as pd
from CompanyCache import CompanyCache
import json
from openai import OpenAI
from DataProcessor import DataProcessor
import threading
import numpy as np

client = OpenAI(api_key="sk-eb4783b9dade435585d47bfc0945cc92", base_url="https://api.deepseek.com")

def process_batch(batch):
    """Send a batch to DeepSeek API and parse response."""
    SYSTEM_PROMPT = """
        Sei un assistente specializzato nella classificazione di spese aziendali.
        Ricevi in input un batch di righe, ognuna con:
        1. id (un identificativo univoco per ogni riga)
        2. Ragione sociale
        3. Descrizione

        
        Il tuo compito è restituire esclusivamente una delle categorie definite qui sotto,
        seguendo le regole speciali e utilizzando sia ragione sociale sia descrizione per dedurre la categoria.

       
        Food – Prodotti alimentari per cucina e ristorazione, freschi o confezionati.
        Beverage – Bevande alcoliche (vino, birra, liquori) e analcoliche (acqua, succhi, bibite).
        Altre Forniture – Materiale di consumo vario (cancelleria, detergenti, accessori).
        Energia Elettrica – Fornitura e consumo di energia elettrica.
        Carburante – Benzina, diesel o altri combustibili per veicoli e macchinari.
        Automezzi – Acquisto, leasing o manutenzione di veicoli aziendali.
        Spese Amministrative – Costi di gestione ufficio, segreteria e pratiche burocratiche. Include licenze obbligatorie come SIAE e diritti connessi.
        Attrezzature Sala – Arredi e strumenti per la sala clienti (tavoli, sedie, posateria).
        Commercialista – Servizi di consulenza e gestione contabile/fiscale.
        Marketing – Pubblicità, promozioni, campagne online e offline.
        Altre Spese – Costi vari non riconducibili ad altre categorie.
        Musica – Licenze musicali, impianti audio o intrattenimento musicale (non obblighi amministrativi).
        Spese Bancarie – Commissioni, canoni e oneri bancari.
        Noleggi – Affitto temporaneo di attrezzature, veicoli o locali.
        Software – Licenze e abbonamenti a programmi e applicazioni.
        Manutenzione Verde – Cura di giardini, piante e spazi esterni.
        Rappresentanza – Spese per eventi, meeting e relazioni con clienti.
        Caffè – Fornitura di caffè e prodotti correlati.
        Manutenzione Generica – Riparazioni e manutenzioni non specialistiche. Include interventi su serrature, porte e servizi di pulizia/disinfestazione.
        Telefono – Costi di telefonia fissa e mobile, abbonamenti internet e VoIP.
        Consulente Lavoro – Servizi per gestione buste paga e pratiche del personale.
        Consulenza – Consulenze professionali in vari ambiti (strategia, legale, tecnico).
        Professionisti – Prestazioni di lavoratori autonomi specializzati.
        Abiti lavoro – Divise, abbigliamento professionale e DPI.
        Manutenzione Macchinari – Riparazione e assistenza macchinari di lavoro.
        Materiali di Cucina – Utensili, pentole e materiali per preparazioni culinarie.
        Manutenzione Impianti – Assistenza su impianti elettrici, idraulici o di climatizzazione.
        Altre - In caso di spese che non rientrano in nessuna delle categorie sopra elencate, utilizzare questa categoria.

        Regole speciali:


        Esempi:
        1. Ragione sociale: MALINVERNI PAOLO ENRICO | Descrizione: Realizzazione sottobicchieri in cartone pressato con logo → Marketing
        2. Ragione sociale: LR SERRATURE S.N.C. | Descrizione: Sostituzione chiudiporta Cisa con fermo a giorno → Manutenzione Generica
        3. Ragione sociale: Flawless Living s.r.l. | Descrizione: Partnership FLAWLESS.life → Marketing

        Il tuo output deve essere **solo un array JSON di oggetti**, uno per riga, con i campi:
        - "id"
        - "CATEGORIA"
        
        Esempio:
            {'response': [
                {'id': 1, 'CATEGORIA': 'Carburante'},
                {'id': 35, 'CATEGORIA': 'Food'},
                {'id': 78, 'CATEGORIA': 'Food'}
            ]}
        """

    user_prompt = "\n".join(
    f"id {id} | Ragione sociale: {row['RAGIONE_SOCIALE']} | Descrizione: {row['DESCRIZIONE']}"
    for id, row in batch.iterrows()
)
    
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ],
        response_format={"type": "json_object"},
        temperature=0.3  # Less randomness for consistent categories
    )
    
    response_content = response.choices[0].message.content
    result = json.loads(response_content)
    
    # Validate top-level JSON object
    if not isinstance(result, dict):
        raise ValueError("Top-level response is not a JSON object")
    
    # Try common keys for response data
    response_data = None
    for key in ['response', 'data', 'output', 'results']:
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

    for item in validated_items:
        batch.loc[item['id'], 'CATEGORIA'] = item['CATEGORIA']

    return batch

def process_batches_in_parallel(batches, process_batch):
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
        updated = process_batch(batch)  # call LLM or custom function
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

    return all_processed


def data_preprocessing(fatture):
   clustered_data = DataProcessor.sequential_cluster(fatture, threshold=0.8)
   
   return clustered_data

def categorize_invoices(processed_fatture, company_cache):
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
    company_cache = CompanyCache(company_cache)
    batch_size = 20  # Adjust batch size as needed
    categorized_fatture = processed_fatture.copy()
    categorized_fatture['CATEGORIA'] = 'No categorizzato'  # Default value
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
    not_categorized = categorized_fatture[categorized_fatture["CATEGORIA"] == 'No categorizzato' ]

    if len(not_categorized) > 0:
        print(f"Not categorized items: {not_categorized.shape[0]}")
        batches = DataProcessor.split_into_batches(not_categorized, batch_size=batch_size)
        print(f"Total batches to process: {len(batches)}")

        # Process batches in parallel
        all_processed = process_batches_in_parallel(batches, process_batch)
        print("all_processed columns:", all_processed.columns.tolist())
        # Save categories to cache
        for _, row in all_processed.iterrows():
            piva = row['P_IVA']
            descrizione = row['DESCRIZIONE']
            categoria = row['CATEGORIA']
            company_cache.set_category(piva, descrizione, categoria)
        company_cache.save_cache()    
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

    return categorized_fatture  


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Categorize invoices based on supplier data.")
    parser.add_argument("input_csv", help="Path to input CSV containing invoices")
    parser.add_argument("cache_json", help="Path to JSON cache with supplier data")
    parser.add_argument("output_csv", help="Path to save categorized invoices CSV")
    args = parser.parse_args()

    # Load input data
    df = pd.read_csv(args.input_csv)
    print("input shape is", df.shape)
    # Process invoices
    clustered_fatture = DataProcessor.sequential_cluster(df, threshold=0.8)
    reduced_row = DataProcessor.representatives(clustered_fatture)
    print("reduced_input is", reduced_row.shape)
    reduced_row_categ = categorize_invoices(reduced_row, args.cache_json)
    print("reduced_row_categ columns:", reduced_row_categ.columns.tolist())
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
    

    # Save results
    clustered_with_cat.to_csv(args.output_csv, index=False, encoding="utf-8-sig")

