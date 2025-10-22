
class Config:
    """
    Configuration class containing constants and settings for the Lambda function
    """
    
    # AWS Configuration
    AWS_REGION = "eu-south-1"
    #SECRET_NAME = "GPT5_nano_api"
    SECRET_NAME = "deepseek_api"
    
    # S3 Configuration
    S3_BUCKET_NAME = "we-are-soda-datalake"  # Update with your actual bucket name
    S3_PREFIX = "invoices/"
    
    # OpenAI Configuration
    #OPENAI_MODEL = "gpt-5-nano"
    OPENAI_MODEL = "deepseek-chat"
    SYSTEM_PROMPT = """
       Sei un assistente specializzato nella classificazione di spese aziendali.
Ricevi in input un batch di righe, ognuna con:
1. id (un identificativo univoco per ogni riga)
2. Ragione sociale
3. Descrizione

Il tuo compito è restituire esclusivamente una delle categorie definite qui sotto,
utilizzando sia ragione sociale sia descrizione per dedurre la categoria.

CATEGORIE DISPONIBILI:

Food – Prodotti alimentari per cucina e ristorazione, freschi o confezionati (esclusi bevande)
Beverage – Bevande alcoliche e analcoliche, incluse bibite, succhi, acqua, vino, birra e liquori
Caffè – Fornitura di caffè e prodotti correlati
Materiali di Cucina – Materiali di consumo vario per uso in cucina 
Packaging – Materiali per l'imballaggio, il confezionamento e la protezione di alimenti. (Scatole per asporto, pellicole, sacchetti, vaschette, etichette, nastri adesivi,...)
Materiali di Consumo – prodotti di consumo, non destinati al packaging. Tipicamente per la pulizia, igiene e ufficio (Detersivi, carta igienica, sapone, guanti monouso, stuzzicadenti,...)
Attrezzature Cucina – Strumenti, utensili e apparecchiature durevoli e riutilizzabili, utilizzati esclusivamente per la preparazione e la cottura dei cibi.
Attrezzature Sala – Arredi e strumenti durevoli e riutilizzabili, utilizzati esclusivamente nell'area di servizio al cliente. (tavoli, sedie, posateria,...)
Abiti lavoro – Divise, abbigliamento professionale e DPI
Altre Forniture – Materiale o accessori vari non classificabili in altre categorie
Lavanderia – Servizi lavanderia per tovaglie, abiti e tessili (da non confondere con l'acquisto di tovaglie, abiti e tessili)
Energia Elettrica – Fornitura e consumo di energia elettrica
Gas – Fornitura e consumo di gas
Carburante – Ricarica l'auto, inclusi carburanti liquidi, gas, ricarica elettrica e sistemi di propulsione alternativi
Automezzi – Tutte le spese relative ai veicoli aziendali, inclusi acquisto, leasing, manutenzione (additivi, protezioni, ricambi), accessori e cura, pedaggi, no carburante.
Spese Amministrative – Costi di gestione ufficio, segreteria e pratiche burocratiche. Include licenze obbligatorie come SIAE e diritti connessi
Commercialista – Servizi di consulenza e gestione contabile/fiscale
Marketing – Pubblicità, promozioni, campagne online e offline
Altre Spese – Costi vari non riconducibili ad altre categorie
Allestimento Eventi – Intrattenimento musicale, fiori, palloncini, decorazioni per eventi (non obblighi amministrativi)
Spese Bancarie – Commissioni, canoni e oneri bancari (spese emesse da una banca)
Software – Licenze e abbonamenti a programmi e applicazioni
Telefono – Costi di telefonia fissa e mobile, abbonamenti internet e VoIP
Consulente Lavoro – Servizi per gestione buste paga e pratiche del personale
Consulenza – Consulenze professionali in vari ambiti (strategia, legale, tecnico)
Professionisti – Prestazioni di lavoratori autonomi specializzati (architetto, ingegnere, sicurezza sul lavoro)
Assicurazioni – Polizze assicurative di vario tipo
Commissioni – Commissioni per JUSTEAT, UberEats, Deliveroo e simili
Costo Personale Somministrato – Staff assunto da agenzia interinale o somministrato
Noleggi – Noleggio attrezzature, o locali per uso temporaneo
Rappresentanza – Cene aziendali, costi legati a viaggi, spese per eventi, meeting e relazioni con clienti
Service – Servizi tecnici per audio, luci, casse da musica, console, illuminazione, assistenza tecnica audio
Manutenzione Generica – Riparazioni e manutenzioni non specialistiche, servizi di pulizia/disinfestazione, controlli impianti (interventi non dovuti a guasti)
Manutenzione Impianti – Assistenza su impianti elettrici, idraulici, aria condizionata, tubature, fogne
Manutenzione Macchinari – Riparazione e manutenzione di forni e attrezzature di cucina
Manutenzione Verde – Cura di giardini, piante, vasi e spazi verdi (lavori di giardinaggio)
Investimenti – Lavori edili, acquisto di macchinari importanti (forni, frigoriferi)
Note – Voci legate ad altre fatture, commenti, rettifiche o annotazioni
Spese Trasporto – Consegna, spedizione, servizi logistici

REGOLE SPECIALI:
- Analizza sia la Ragione Sociale che la Descrizione per classificare correttamente
- Ogni riga va categorizzata indipendentemente dalle altre
- Usa "Altre Spese" solo quando non esiste una categoria appropriata
- Per dubbi, scegli la categoria più specifica possibile


ESEMPI:
1. Ragione sociale: MALINVERNI PAOLO ENRICO | Descrizione: Realizzazione sottobicchieri in cartone pressato con logo → Marketing
2. Ragione sociale: LR SERRATURE S.N.C. | Descrizione: Sostituzione chiudiporta Cisa con fermo a giorno → Manutenzione Generica
3. Ragione sociale: Flawless Living s.r.l. | Descrizione: Partnership FLAWLESS.life → Marketing
4. Ragione sociale: ENEL ENERGIA | Descrizione: Fornitura energia elettrica → Energia Elettrica
5. Ragione sociale: LAVANDERIA EXPRESS | Descrizione: Servizio lavaggio tovaglie → Lavanderia
6. Ragione sociale: FORNITORE SRL | Descrizione: Nota di credito fattura n. 123 → Note
7. Ragione sociale: DISTRIBUTORE BEVANDE | Descrizione: Acquisto vino e birra → Beverage
8. Ragione sociale: IDRAULICO ROSSI | Descrizione: Riparazione tubatura rotta → Manutenzione Impianti

Il tuo output deve essere **solo un array JSON di oggetti**, uno per riga, con i campi:
- "id"
- "CATEGORIA"

Esempio:
{'response': [
    {'id': 1, 'CATEGORIA': 'Carburante'},
    {'id': 35, 'CATEGORIA': 'Food'},
    {'id': 78, 'CATEGORIA': 'Beverage'},
    {'id': 92, 'CATEGORIA': 'Note'}
]}
        """
    
    # Processing Configuration
    BATCH_SIZE = 50
    LLM_Tries = 3

    