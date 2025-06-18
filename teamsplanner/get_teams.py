import os
import logging
from typing import List, Dict, Set, Optional, Tuple
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.o_data_errors.o_data_error import ODataError
from azure.data.tables import TableServiceClient
import datetime
from .create_planner import PlannerTemplateManager

class TeamsProcessor:
    """
    Klasse for å prosessere Microsoft Teams og håndtere deres status i Azure Table Storage.
    """
    
    def __init__(self):
        # Konstanter
        self.TABLE_NAME = "processedteams"
        self.PARTITION_KEY = "teams"
        
        # Klienter som vil bli initialisert
        self.credentials: Optional[ClientSecretCredential] = None
        self.graph_client: Optional[GraphServiceClient] = None
        self.table_client: Optional[TableServiceClient] = None
        self.planner_manager: Optional[PlannerTemplateManager] = None
        
        # State
        self.processed_team_ids: Set[str] = set()
        self.is_initialized = False
    
    async def initialize(self) -> None:
        """
        Initialiserer Graph API klient, Table Storage og henter prosesserte teams.
        """
        if self.is_initialized:
            return
            
        # Initialiser Graph API klient
        await self._initialize_graph_client()
        
        # Initialiser Table Storage
        await self._initialize_table_client()
        
        # Hent prosesserte teams
        if self.table_client:
            self.processed_team_ids = await self._get_processed_team_ids()
        
        self.is_initialized = True
        logging.info("TeamsProcessor er initialisert")
    
    async def _initialize_graph_client(self) -> None:
        """
        Initialiserer Graph API klient med miljøvariabler.
        """
        tenant_id = os.getenv('AZURE_TENANT_ID')
        client_id = os.getenv('AZURE_CLIENT_ID')
        client_secret = os.getenv('AZURE_CLIENT_SECRET')
        
        if not all([tenant_id, client_id, client_secret]):
            raise ValueError("Mangler påkrevde miljøvariabler: TENANT_ID, CLIENT_ID, eller CLIENT_SECRET")
        
        self.credentials = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        self.graph_client = GraphServiceClient(
            credentials=self.credentials, 
            scopes=['https://graph.microsoft.com/.default']
        )

        self.planner_manager = PlannerTemplateManager(self.graph_client, self.credentials)
        logging.info("Graph API klient initialisert")
    
    async def _initialize_table_client(self) -> None:
        """
        Initialiserer Table Storage klient.
        """
        try:
            storage_connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
            if not storage_connection_string:
                raise ValueError("Mangler AZURE_STORAGE_CONNECTION_STRING miljøvariabel")
                
            table_service_client = TableServiceClient.from_connection_string(storage_connection_string)
            
            try:
                self.table_client = table_service_client.get_table_client(self.TABLE_NAME)
                logging.info(f"Koblet til tabell: {self.TABLE_NAME}")
            except Exception:
                logging.info(f"Oppretter ny tabell: {self.TABLE_NAME}")
                self.table_client = table_service_client.create_table(self.TABLE_NAME)
                
        except Exception as e:
            logging.error(f"Feil ved initialisering av Table Storage: {str(e)}")
            raise
    
    async def _get_processed_team_ids(self) -> Set[str]:
        """
        Henter alle prosesserte team-IDer fra Azure Table Storage.
        
        Returns:
            Set[str]: Mengde med prosesserte team-IDer
        """
        if not self.table_client:
            logging.error("Table client er ikke initialisert")
            return set()
            
        try:
            query = f"PartitionKey eq '{self.PARTITION_KEY}'"
            processed_teams = {entity['RowKey'] for entity in self.table_client.query_entities(query)}
            logging.info(f"Fant {len(processed_teams)} prosesserte teams i tabellen")
            return processed_teams
        except Exception as e:
            logging.error(f"Feil ved henting av prosesserte team-IDer: {str(e)}")
            return set()
    
    async def process_team(self, team) -> Optional[Dict]:
        """
        Prosesserer et enkelt team og markerer det som prosessert hvis det er nytt.
        
        Args:
            team: Team objekt fra Graph API
            
        Returns:
            Optional[Dict]: Team data hvis teamet er nytt, None hvis det er allerede prosessert
        """
        if not self.is_initialized:
            raise RuntimeError("TeamsProcessor er ikke initialisert. Kall initialize() først.")
        
        # Hopp over arkiverte teams og allerede prosesserte teams
        if team.is_archived or team.id in self.processed_team_ids:
            return None
        
        logging.info(f"Fant nytt team: {team.id} ({team.display_name})")
        
        # Lag en ny planner for teamet og marker team som prosessert
        try:
            planner_result = await self.planner_manager.create_planner_for_team(team.id, team.display_name)

            if planner_result:
                await self.mark_team_as_processed(team.id)
                logging.info(f"Opprettet planner og markerte team {team.id} som prosessert")
            else:
                logging.error(f"Kunne ikke opprette planner for team {team.id}")
            
            self.processed_team_ids.add(team.id)

        except Exception as e:
            logging.error(f"Kunne ikke prosessere team {team.id}: {str(e)}")
            return None
        
        return {
            'id': team.id,
            'displayName': team.display_name,
            'visibility': team.visibility,
            'isProcessed': planner_result,
        }
    
    async def get_teams_async(self) -> Dict:
        """
        Hovedfunksjon som henter alle Microsoft Teams fra organisasjonen.
        Filtrerer bort arkiverte teams og returnerer kun teams som ikke er prosessert.
        
        Returns:
            Dict: Dictionary med nye teams og metadata
        """
        if not self.is_initialized:
            await self.initialize()
        
        try:
            # Hent og prosesser teams
            new_teams_list = []
            teams_response = await self.graph_client.teams.get()
            
            while teams_response:
                if teams_response.value:
                    for team in teams_response.value:
                        team_data = await self.process_team(team)
                        if team_data:
                            new_teams_list.append(team_data)
                
                # Håndter paginering
                if teams_response.odata_next_link:
                    teams_response = await self.graph_client.teams.with_url(teams_response.odata_next_link).get()
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
    
    async def mark_team_as_processed(self, team_id: str) -> bool:
        """
        Marker et team som prosessert i Table Storage.
        
        Args:
            team_id: ID til teamet som skal markeres
            
        Returns:
            bool: True hvis operasjonen var vellykket
        """
        if not self.table_client:
            raise RuntimeError("Table client er ikke initialisert")
        
        try:
            # Opprett entity for prosessert team
            entity = {
                'PartitionKey': self.PARTITION_KEY,
                'RowKey': team_id,
                'ProcessedAt': datetime.datetime.now(datetime.UTC).isoformat()
            }
            
            logging.info(f"Forsøker å upsertte entity: {entity}")
            self.table_client.upsert_entity(entity=entity)
            logging.info(f"Vellykket upsert av entity for team {team_id}")
            return True
            
        except Exception as e:
            logging.error(f"Feil ved markering av team som prosessert: {str(e)}")
            raise Exception(f"Feil ved markering av team som prosessert: {str(e)}")
        
    
    def is_team_processed(self, team_id: str) -> bool:
        """
        Sjekker om et team allerede er prosessert.
        
        Args:
            team_id: ID til teamet som skal sjekkes
            
        Returns:
            bool: True hvis teamet er prosessert
        """
        return team_id in self.processed_team_ids
    
    def get_processed_teams_count(self) -> int:
        """
        Returnerer antall prosesserte teams.
        
        Returns:
            int: Antall prosesserte teams
        """
        return len(self.processed_team_ids)
    
    def reset_processed_teams(self) -> None:
        """
        Tømmer cache av prosesserte teams (nyttig for testing).
        """
        self.processed_team_ids.clear()
        logging.info("Cache av prosesserte teams er tømt")
