import os
import logging
from typing import List, Dict, Set, Optional, Tuple
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.o_data_errors.o_data_error import ODataError
from azure.data.tables import TableServiceClient
import datetime
from .create_planner import create_planner_for_team, list_available_plans


# Konstanter for tabellnavn og partisjonsnøkler
TABLE_NAME = "processedteams"
PARTITION_KEY = "teams"

async def get_processed_team_ids(table_client: TableServiceClient) -> Set[str]:
    """
    Henter alle prosesserte team-IDer fra Azure Table Storage.
    
    Args:
        table_client: Azure Table Storage klient
        
    Returns:
        Set[str]: Mengde med prosesserte team-IDer
    """
    try:
        query = f"PartitionKey eq '{PARTITION_KEY}'"
        processed_teams = {entity['RowKey'] for entity in table_client.query_entities(query)}
        logging.info(f"Fant {len(processed_teams)} prosesserte teams i tabellen")
        return processed_teams
    except Exception as e:
        logging.error(f"Feil ved henting av prosesserte team-IDer: {str(e)}")
        return set()

async def initialize_table_client() -> Optional[TableServiceClient]:
    """
    Initialiserer og returnerer en Table Storage klient.
    
    Returns:
        Optional[TableServiceClient]: Table Storage klient eller None ved feil
    """
    try:
        storage_connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if not storage_connection_string:
            raise ValueError("Mangler AZURE_STORAGE_CONNECTION_STRING miljøvariabel")
            
        table_service_client = TableServiceClient.from_connection_string(storage_connection_string)
        
        try:
            table_client = table_service_client.get_table_client(TABLE_NAME)
            logging.info(f"Koblet til tabell: {TABLE_NAME}")
        except Exception:
            logging.info(f"Oppretter ny tabell: {TABLE_NAME}")
            table_client = table_service_client.create_table(TABLE_NAME)
            
        return table_client
    except Exception as e:
        logging.error(f"Feil ved initialisering av Table Storage: {str(e)}")
        return None

async def initialize_graph_client() -> Tuple[GraphServiceClient, Set[str]]:
    """
    Initialiserer Graph API klient og henter prosesserte teams.
    
    Returns:
        Tuple[GraphServiceClient, Set[str]]: Graph klient og mengde med prosesserte team-IDer
    """
    # Hent påkrevde miljøvariabler
    tenant_id = os.getenv('AZURE_TENANT_ID')
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    
    if not all([tenant_id, client_id, client_secret]):
        raise ValueError("Mangler påkrevde miljøvariabler: TENANT_ID, CLIENT_ID, eller CLIENT_SECRET")
    
    # Initialiser Graph API klient
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )
    client = GraphServiceClient(credentials=credential, scopes=['https://graph.microsoft.com/.default'])
    
    # Initialiser Table Storage og hent prosesserte teams
    table_client = await initialize_table_client()
    if not table_client:
        raise Exception("Kunne ikke initialisere Table Storage")
        
    processed_team_ids = await get_processed_team_ids(table_client)
    
    return client, processed_team_ids


async def process_team(team, client, processed_team_ids: Set[str]) -> Optional[Dict]:
    """
    Prosesserer et enkelt team og markerer det som prosessert hvis det er nytt.
    
    Args:
        team: Team objekt fra Graph API
        client: Graph API klient
        processed_team_ids: Mengde med allerede prosesserte team-IDer
        
    Returns:
        Optional[Dict]: Team data hvis teamet er nytt, None hvis det er allerede prosessert
    """
    # Hopp over arkiverte teams og allerede prosesserte teams
    if team.is_archived or team.id in processed_team_ids:
        return None
    
    logging.info(f"Fant nytt team: {team.id} ({team.display_name})")
    
    # Lag en ny planner for teamet og marker team som prosessert
    try:
        planner_result = True # await create_planner_for_team(client, team.id)

        if planner_result:
            processed_team_ids.add(team.id)
            logging.info(f"Opprettet planner og markerte team {team.id} som prosessert")
        else:
            logging.error(f"Kunne ikke opprette planner for team {team.id}")
            return None
    except Exception as e:
        logging.error(f"Kunne ikke prosessere team {team.id}: {str(e)}")
        return None
    
    return {
        'id': team.id,
        'displayName': team.display_name,
        'visibility': team.visibility,
        'isProcessed': True,
        #'plannerId': planner_result.get('planId') if planner_result else None
    }


async def get_teams_async() -> Dict:
    """
    Hovedfunksjon som henter alle Microsoft Teams fra organisasjonen.
    Filtrerer bort arkiverte teams og returnerer kun teams som ikke er prosessert.
    
    Returns:
        Dict: Dictionary med nye teams og metadata
    """
    try:
        # Initialiser klienter og hent prosesserte teams
        client, processed_team_ids = await initialize_graph_client()
        
        # Hent og prosesser teams
        new_teams_list = []
        teams_response = await client.teams.get()
        
        while teams_response:
            if teams_response.value:
                for team in teams_response.value:
                    team_data = await process_team(team, client, processed_team_ids)
                    if team_data:
                        new_teams_list.append(team_data)
            
            # Håndter paginering
            if teams_response.odata_next_link:
                teams_response = await client.teams.with_url(teams_response.odata_next_link).get()
            else:
                break
        
        return {
            "teams": new_teams_list,
            "count": len(new_teams_list),
            "status": "success",
            "message": f"Fant {len(new_teams_list)} nye teams som trenger prosessering"
        }
        
    except ODataError as e:
        error_message = e.error.message if e.error else str(e)
        logging.error(f"Microsoft Graph API feil: {error_message}")
        raise Exception(f"Microsoft Graph API feil: {error_message}")
        
    except ValueError as e:
        logging.error(f"Konfigurasjonsfeil: {str(e)}")
        raise e
        
    except Exception as e:
        logging.error(f"Uventet feil: {str(e)}")
        raise Exception(f"Uventet feil: {str(e)}")

async def mark_team_as_processed(team_id: str) -> bool:
    """
    Marker et team som prosessert i Table Storage.
    
    Args:
        team_id: ID til teamet som skal markeres
        
    Returns:
        bool: True hvis operasjonen var vellykket
    """
    try:
        table_client = await initialize_table_client()
        if not table_client:
            raise ValueError("Kunne ikke initialisere Table Storage")
        
        # Opprett entity for prosessert team
        entity = {
            'PartitionKey': PARTITION_KEY,
            'RowKey': team_id,
            'ProcessedAt': datetime.datetime.now(datetime.UTC).isoformat()
        }
        
        logging.info(f"Forsøker å upsertte entity: {entity}")
        table_client.upsert_entity(entity=entity)
        logging.info(f"Vellykket upsert av entity for team {team_id}")
        return True
        
    except Exception as e:
        logging.error(f"Feil ved markering av team som prosessert: {str(e)}")
        raise Exception(f"Feil ved markering av team som prosessert: {str(e)}")
