
import argparse
import pandas as pd
from CompanyCache import CompanyCache
import json
from openai import OpenAI
from DataProcessor import DataProcessor

client = OpenAI(api_key="sk-eb4783b9dade435585d47bfc0945cc92", base_url="https://api.deepseek.com")

def process_batch(batch):
    """Send a batch to DeepSeek API and parse response."""
    SYSTEM_PROMPT = """
        Sei un assistente specializzato nella classificazione di spese aziendali.
        Ricevi in input un batch di righe, ognuna con:
        1. Index (numero identificativo)
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

        L'output deve essere SOLO un array JSON di oggetti, uno per riga, esempio:
            {'response': [
                {'index': 2543, 'categoria': 'Carburante'},
                {'index': 5580, 'categoria': 'Food'},
                {'index': 2885, 'categoria': 'Food'}
            ]}
        """

    user_prompt = "\n".join(
        f"{row['index']}. Ragione sociale: {row['ragione_sociale']} | Descrizione: {row['descrizione']}"
        for row in batch
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
    # Strict JSON parsing with validation
    response_content = response.choices[0].message.content
    result = json.loads(response_content)
    
    # Validate JSON structure
    if not isinstance(result, dict):
        raise ValueError("Top-level response is not a JSON object")
    
    # Check for response data in common key locations
    response_data = None
    for key in ['response', 'data', 'output', 'results']:
        if key in result and isinstance(result[key], list):
            response_data = result[key]
            break
    if response_data is None:
         # If no standard key found, check if the object itself is the response
        if all(isinstance(item, dict) and 'index' in item and 'categoria' in item 
            for item in result.values() if isinstance(item, dict)):
                response_data = list(result.values())
        else:
            raise ValueError("Could not locate valid response data in JSON")
    # Validate each item in response
    validated_items = []
    for item in response_data:
        if not isinstance(item, dict):
            continue
        if 'index' not in item or 'categoria' not in item:
            continue
        validated_items.append({
                'index': int(item['index']),
                'categoria': str(item['categoria'])
            })
            
    if not validated_items:
        raise ValueError("No valid items found in response")
        print("response data:"+response_data)
            
    return {'response': validated_items}



def data_preprocessing(fatture):
   clustered_data = DataProcessor.sequential_cluster(fatture, threshold=0.8)
   
   return clustered_data

def categorize_invoices(processed_fatture, company_cache):
    """
    Categorize invoices based on supplier data.
    
    Parameters:
    - processed_fatture: DataFrame containing preprocessed invoice data
        -- has these colunm:
                            Index
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
    - fornitori: DataFrame containing supplier data
    - company_cache: string
    
    Returns:
    - categorized_fatture: DataFrame with categorized invoices
    """
    company_cache = CompanyCache(company_cache)
    batch_size = 20  # Adjust batch size as needed
    batch = []
    batch_count = 0
    categorized_fatture = processed_fatture.copy()
    categorized_fatture['CATEGORIA'] = 'No categorizzato'  # Default value
    cache_hit = 0  # Counter for cache hits
    for _, row in processed_fatture.iterrows():
        index = row['Index']
        ragione_sociale = row['RAGIONE_SOCIALE']
        descrizione = row['DESCRIZIONE']
        
        # Check cache first
        if company_cache.has_category(ragione_sociale, descrizione):
            categorized_fatture.loc[categorized_fatture['Index'] == index, 'CATEGORIA'] = company_cache.get_category(ragione_sociale, descrizione)
            cache_hit += 1
        else:
            # If not cached, categorize using the model
            item = {"index": index, "ragione_sociale": ragione_sociale, "descrizione": descrizione}
            batch.append(item)
        if len(batch) == batch_size:
            # Process the batch, call deepseek API to categorize bach_size items, 
            # result will be a list of dictionaries with 'index' and 'categoria'
            batch_count += 1
            results = process_batch(batch) 

            print(f"batch {batch_count} processed, content:") 
            

            for result in results['response']:
                index = result.get('index', 'N/A')
                categoria =  result.get('categoria', 'N/A')
                ragione_sociale = next(
                    (item['ragione_sociale'] for item in batch if item['index'] == index), None)

                descrizione = next(
                    (item['descrizione'] for item in batch if item['index'] == index), None)
                
                # Update cache and DataFrame
                company_cache.set_category(ragione_sociale, descrizione, categoria)
                categorized_fatture.loc[categorized_fatture['Index'] == index, 'CATEGORIA'] = categoria
            
            batch = []

    
    # Process any remaining items in the last batch
    if batch:
        results = process_batch(batch)
        print(f"batch {batch_count} processed, content:") 
       
        for result in results['response']:
            index = result.get('index', 'N/A')
            categoria = result.get('categoria', 'N/A')
            ragione_sociale = next(
                (item['ragione_sociale'] for item in batch if item['index'] == index), None)

            descrizione = next(
                (item['descrizione'] for item in batch if item['index'] == index), None)
            
            # Update cache and DataFrame
            company_cache.set_category(ragione_sociale, descrizione, categoria)
            categorized_fatture.loc[categorized_fatture['Index'] == index, 'CATEGORIA'] = categoria
    # Save the cache to file
    company_cache.save_cache()
    print(f"Cache hits: {cache_hit} out of {len(processed_fatture)} total items")
    return categorized_fatture        


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Categorize invoices based on supplier data.")
    parser.add_argument("input_csv", help="Path to input CSV containing invoices")
    parser.add_argument("cache_json", help="Path to JSON cache with supplier data")
    parser.add_argument("output_csv", help="Path to save categorized invoices CSV")
    args = parser.parse_args()

    # Load input data
    df = pd.read_csv(args.input_csv)

    # Process invoices
    clustered_fatture = DataProcessor.sequential_cluster(df, threshold=0.8)
    reduced_row = DataProcessor.representatives(clustered_fatture)

    reduced_row_categ = categorize_invoices(reduced_row, args.cache_json)

     # Step 1: propagate categories to clustered_fatture
    clustered_with_cat = clustered_fatture.drop(
        columns=["CATEGORIA"], errors='ignore'
        ).merge(
        reduced_row_categ[["RAGIONE_SOCIALE", "cluster", "CATEGORIA"]],
        on=["RAGIONE_SOCIALE", "cluster"],
        how="left"
                )
    # Step 2: use (RAGIONE_SOCIALE, DESCRIZIONE) to bring CATEGORIA back to original df
    df_with_cat = df.merge(
        clustered_with_cat[["RAGIONE_SOCIALE", "DESCRIZIONE", "CATEGORIA"]],
        on=["RAGIONE_SOCIALE", "DESCRIZIONE"],
        how="left"
    )    

    # Show preview
    print(df_with_cat.info())
    print(df_with_cat.head())

    # Save results
    df_with_cat.to_csv(args.output_csv, index=False, encoding="utf-8-sig")

