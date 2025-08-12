
from turtle import pd

import json
from openai import OpenAI

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

        L'output deve essere SOLO un array JSON di oggetti, uno per riga, con:
        {"index": X, "categoria": "..."}


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
        1. Ragione sociale: S.I.A.E. | Descrizione: MUSICA D'AMBIENTE - Diritti connessi → Spese Amministrative
        2. Ragione sociale: ENEGAN S.P.A. | Descrizione: Canone fibra ottica e traffico telefonico → Telefono
        3. Ragione sociale: DISINFECTA SPA | Descrizione: Contratto annuale di disinfestazione → Manutenzione Generica
        4. Ragione sociale: CAMMISCIA SRL | Descrizione: Drogheria → Food
        5. Ragione sociale: MALINVERNI PAOLO ENRICO | Descrizione: Realizzazione sottobicchieri in cartone pressato con logo → Marketing
        6. Ragione sociale: LR SERRATURE S.N.C. | Descrizione: Sostituzione chiudiporta Cisa con fermo a giorno → Manutenzione Generica
        7. Ragione sociale: Flawless Living s.r.l. | Descrizione: Partnership FLAWLESS.life → Marketing

        Rispondi sempre e solo con un array JSON valido e nessun testo extra.
        """

    user_prompt = "\n".join(
        f"{row['index']}. {row['ragione_sociale']} | {row['descrizione']}"
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
    return json.loads(response.choices[0].message.content)


def data_preprocessing(fatture):
    raise NotImplementedError

def categorize_invoices(processed_fatture, fornitori, info=None):
    """
    Categorize invoices based on supplier data.
    
    Parameters:
    - processed_fatture: DataFrame containing preprocessed invoice data
    - fornitori: DataFrame containing supplier data
    - info: Additional information for categorization (optional)
    
    Returns:
    - categorized_fatture: DataFrame with categorized invoices
    """
    raise NotImplementedError


def main(fatture, fornitori, info=None):
    """
    Main function to categorize invoices based on supplier data.
    
    Parameters:
    - fatture: DataFrame containing invoice data
    - fornitori: DataFrame containing supplier data
    
    Returns:
    - fatture: DataFrame with categorized invoices
    """
    processed_fatture = data_preprocessing(fatture)
    result = categorize_invoices(processed_fatture, fornitori, info)

    return result

