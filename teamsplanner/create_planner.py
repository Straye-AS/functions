import logging
import os
import uuid
from typing import Dict, Optional
import asyncio
import aiohttp
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.planner_plan import PlannerPlan
from msgraph.generated.models.planner_task import PlannerTask

class PlannerTemplateManager:
    """
    Klasse for å håndtere kopiering av Planner-templates til nye teams.
    Kopierer malen nøyaktig som den er uten å endre noen states.
    """
    
    def __init__(self, graph_client: GraphServiceClient, credentials: ClientSecretCredential):
        # Klienter og credentials
        self.graph_client = graph_client
        self.credentials = credentials
        
        # Konfigurasjon
        self.template_planner_id = os.getenv("TEAMS_PLANNER_TEMPLATE_ID")
        
        # Cache
        self._template_plan: Optional[PlannerPlan] = None
        self._access_token: Optional[str] = None
        
        # Timing konfigurasjon
        self.bucket_delay = 0.1  # Sekunder mellom bucket-opprettelser
        self.task_delay = 0.1   # Sekunder mellom task-opprettelser
        self.details_delay = 0.1 # Sekunder mellom details-oppdateringer
        
        # Validering
        self._validate_configuration()
        self.get_access_token()
    
    def _validate_configuration(self) -> None:
        """Validerer påkrevde miljøvariabler."""
        if not self.template_planner_id:
            raise ValueError("Mangler påkrevd miljøvariabel: TEAMS_PLANNER_TEMPLATE_ID")
    
    def get_access_token(self) -> Optional[str]:
        """
        Henter access token fra credentials.
        
        Returns:
            Optional[str]: Access token eller None ved feil
        """
        try:
            token =  self.credentials.get_token('https://graph.microsoft.com/.default')
            self._access_token = token.token
            return self._access_token
        except Exception as e:
            logging.error(f"Feil ved henting av access token: {str(e)}")
            return None
    
    async def get_template_planner(self) -> Optional[PlannerPlan]:
        """
        Henter og cacher mal-planneren.
        
        Returns:
            Optional[PlannerPlan]: Mal-planneren eller None ved feil
        """
        if self._template_plan:
            return self._template_plan
        
        try:
            plan = await self.graph_client.planner.plans.by_planner_plan_id(self.template_planner_id).get()
            if not plan:
                raise ValueError(f"Fant ikke mal-planneren med ID: {self.template_planner_id}")
            
            self._template_plan = plan
            logging.info(f"Cachet mal-planner: {plan.title}")
            return plan
        
        except Exception as e:
            logging.error(f"Feil ved henting av mal-planner: {str(e)}")
            return None
    
    async def create_planner_for_team(self, team_id: str, team_name: str) -> Optional[Dict]:
        """
        Oppretter en ny planner for et team ved å kopiere eksisterende mal.
        Ny algoritme: fullfører hver bucket komplett før neste.
        
        Args:
            team_id: ID til teamet som skal få en ny planner
            team_name: Navn på teamet
            
        Returns:
            Optional[Dict]: Informasjon om den nye planneren hvis vellykket, None ved feil
        """
        try:
            # Hent mal-planner
            template_plan = await self.get_template_planner()
            if not template_plan:
                logging.error("Kunne ikke hente mal-planner")
                return None
            
            logging.info(f"Hentet mal-planner: {template_plan.title}")
            
            # Sjekk om team allerede har planner
            if await self.team_has_existing_planners(team_id):
                logging.info(f"Team {team_id} har allerede planner")
                return True
            
            # Opprett ny plan
            created_plan = await self.create_new_plan(template_plan, team_id, team_name)
            if not created_plan:
                return None
            
            # Kopier buckets og tasks sekvensielt (ny algoritme)
            success = await self.copy_template_sequentially(template_plan.id, created_plan.id)
            if not success:
                logging.error("Kopiering av template feilet")
                return None
            
            # Legg til Planner tab i General kanal
            tab_success = await self.add_planner_tab_to_team(team_id, created_plan.id, created_plan.title)
            if  tab_success:
                logging.info(f"Planner tab lagt til i Generelt kanal for team {team_id}")
            else:
                logging.error(f"Kunne ikke legge til Planner tab i Generelt kanal for team {team_id}")
        
            
            logging.info(f"Kopiering av mal fullført! Ny plan ID: {created_plan.id}")

            
            return {
                "id": created_plan.id,
                "title": created_plan.title,
                "owner": created_plan.owner,
                "created_date_time": created_plan.created_date_time,
                "tab_added": tab_success
            }
            
        except Exception as e:
            logging.error(f"Feil ved opprettelse av planner for team {team_id}: {str(e)}")
            return None
    
    async def copy_template_sequentially(self, template_plan_id: str, new_plan_id: str) -> bool:
        """
        Kopier template sekvensielt bucket-for-bucket med ASCII-basert sortering.
        
        Args:
            template_plan_id: ID til mal-planen
            new_plan_id: ID til den nye planen
            
        Returns:
            bool: True hvis vellykket
        """
        try:
            from msgraph.generated.models.planner_task import PlannerTask
            # Hent alle buckets fra template
            template_buckets = await self.graph_client.planner.plans.by_planner_plan_id(template_plan_id).buckets.get()
            if not template_buckets or not template_buckets.value:
                logging.info("Ingen buckets å kopiere")
                return True
            
            # Hent alle tasks fra template 
            template_tasks = await self.graph_client.planner.plans.by_planner_plan_id(template_plan_id).tasks.get()
            if not template_tasks or not template_tasks.value:
                logging.info("Ingen tasks å kopiere")
                # Kopier bare tomme buckets
                return await self.copy_empty_buckets(template_buckets.value, new_plan_id)
            
            # Grupper tasks per bucket
            tasks_by_bucket = {}
            for task in template_tasks.value:
                if task.bucket_id not in tasks_by_bucket:
                    tasks_by_bucket[task.bucket_id] = []
                tasks_by_bucket[task.bucket_id].append(task)
            
            # ASCII-basert sortering av tasks innenfor hver bucket
            def pure_ascii_task_sort_key(item: PlannerTask):
                """Garanterer ren ASCII-basert sammenligning."""
                if item.order_hint:
                    return item.order_hint
                else:
                    return item.title
                        
            # Sorter tasks innenfor hver bucket med ASCII-basert sammenligning
            for bucket_id in tasks_by_bucket:
                # Først prøv normal ASCII sortering
                sorted_tasks = sorted(tasks_by_bucket[bucket_id], key=pure_ascii_task_sort_key)
                tasks_by_bucket[bucket_id] = sorted_tasks
            
            # Log tasks i sortert rekkefölge for debugging
            logging.info("Tasks sortert med ASCII-basert orderHint sammenligning:")
            for bucket_id, tasks in tasks_by_bucket.items():
                bucket_name = next((b.name for b in template_buckets.value if b.id == bucket_id), "Unknown")
                logging.info(f"  Bucket '{bucket_name}': {len(tasks)} tasks")
                for i, task in enumerate(tasks, 1):
                    logging.info(f"    {i}. {task.title} (orderHint: '{task.order_hint}')")
            
            # Sorter buckets etter order hint (buckets sorting fungerer korrekt)
            def ascii_bucket_sort_key(bucket):
                """Returnerer bucket orderHint for ASCII sammenligning."""
                return bucket.order_hint or ""
            
            sorted_buckets = sorted(template_buckets.value, key=ascii_bucket_sort_key, reverse=True)
            
            logging.info(f"Starter sekvensiell kopiering av {len(sorted_buckets)} buckets")
            logging.info("Bucket rekkefölge:")
            for i, bucket in enumerate(sorted_buckets, 1):
                logging.info(f"  {i}. {bucket.name} (orderHint: '{bucket.order_hint}')")
            
            # Prosesser hver bucket komplett
            total_buckets = len(sorted_buckets)
            total_tasks = 0
            successful_tasks = 0
            
            for bucket_index, bucket in enumerate(sorted_buckets, 1):
                logging.info(f"")
                logging.info(f"=== BUCKET {bucket_index}/{total_buckets}: {bucket.name} ===")
                
                # Steg 1: Opprett bucket
                new_bucket = await self.create_bucket_copy(bucket, new_plan_id)
                if not new_bucket:
                    logging.error(f"Kunne ikke opprette bucket: {bucket.name}")
                    continue
                
                # Steg 2: Kopier alle tasks i denne bucketen
                bucket_tasks = tasks_by_bucket.get(bucket.id, [])
                if not bucket_tasks:
                    logging.info(f"Ingen tasks å kopiere for bucket: {bucket.name}")
                    continue
                
                logging.info(f"Kopierer {len(bucket_tasks)} tasks for bucket: {bucket.name} (ASCII-sortert)")
                bucket_successful = 0
                
                for task_index, task in enumerate(bucket_tasks, 1):
                    total_tasks += 1
                    
                    try:
                        logging.info(f"  Task {task_index}/{len(bucket_tasks)}: {task.title}")
                        logging.debug(f"    Original orderHint: '{task.order_hint}'")
                        
                        # Steg 2a: Opprett task
                        created_task = await self.create_task_copy(task, new_plan_id, new_bucket.id)
                        if not created_task:
                            logging.error(f"    Kunne ikke opprette task: {task.title}")
                            continue
                        
                        # Pause etter task opprettelse
                        await asyncio.sleep(self.task_delay)
                        
                        # Steg 2b: Kopier task details
                        await self.copy_task_details(task.id, created_task.id, task.title)
                        
                        successful_tasks += 1
                        bucket_successful += 1
                        
                        # Pause mellom task details
                        await asyncio.sleep(self.details_delay)
                        
                    except Exception as task_error:
                        logging.error(f"    Feil ved kopiering av task '{task.title}': {str(task_error)}")
                        continue
                
                logging.info(f"Bucket '{bucket.name}' fullført: {bucket_successful}/{len(bucket_tasks)} tasks kopiert")
                
                # Pause mellom buckets
                if bucket_index < total_buckets:
                    logging.info(f"Venter {self.bucket_delay}s før neste bucket...")
                    await asyncio.sleep(self.bucket_delay)
            
            logging.info(f"")
            logging.info(f"=== KOPIERING FULLFØRT ===")
            logging.info(f"Totalt: {successful_tasks}/{total_tasks} tasks kopiert på tvers av {total_buckets} buckets")
            
            # If tasks don't appear in correct order, try changing line 45 to:
            # tasks_by_bucket[bucket_id].sort(key=ascii_task_sort_key, reverse=True)
            
            return True
                    
        except Exception as e:
            logging.error(f"Feil ved sekvensiell kopiering: {str(e)}")
            return False

    
    async def copy_empty_buckets(self, template_buckets, new_plan_id: str) -> bool:
        """Kopierer buckets uten tasks."""
        try:
            from msgraph.generated.models.planner_bucket import PlannerBucket
            
            for bucket in template_buckets:
                new_bucket = PlannerBucket()
                new_bucket.name = bucket.name
                new_bucket.plan_id = new_plan_id
                
                created_bucket = await self.graph_client.planner.buckets.post(new_bucket)
                if created_bucket:
                    logging.info(f"Opprettet tom bucket: {bucket.name}")
                
                await asyncio.sleep(self.bucket_delay)
            
            return True
        except Exception as e:
            logging.error(f"Feil ved kopiering av tomme buckets: {str(e)}")
            return False
    
    async def create_bucket_copy(self, template_bucket, new_plan_id: str):
        """
        Oppretter en ny bucket basert på template bucket.
        
        Args:
            template_bucket: Template bucket som skal kopieres
            new_plan_id: ID til den nye planen
            
        Returns:
            Den opprettede bucketen eller None ved feil
        """
        try:
            from msgraph.generated.models.planner_bucket import PlannerBucket
            
            new_bucket = PlannerBucket()
            new_bucket.name = template_bucket.name
            new_bucket.plan_id = new_plan_id
            
            created_bucket = await self.graph_client.planner.buckets.post(new_bucket)
            if created_bucket:
                logging.info(f"Opprettet bucket: {template_bucket.name}")
                return created_bucket
            else:
                logging.error(f"Kunne ikke opprette bucket: {template_bucket.name}")
                return None
                
        except Exception as e:
            logging.error(f"Feil ved opprettelse av bucket {template_bucket.name}: {str(e)}")
            return None
    
    async def team_has_existing_planners(self, team_id: str) -> bool:
        """
        Sjekker om et team allerede har planner.
        
        Args:
            team_id: ID til teamet
            
        Returns:
            bool: True hvis team har planner, False hvis ikke
        """
        try:
            existing_plans = await self.graph_client.groups.by_group_id(team_id).planner.plans.get()
            if existing_plans and existing_plans.value:
                logging.info(f"Team {team_id} har allerede {len(existing_plans.value)} planner(e)")
                logging.info(f"Eksisterende planners: {[it.title for it in existing_plans.value]}")
                return True
            return False
        except Exception as e:
            logging.error(f"Feil ved sjekk av eksisterende planner: {str(e)}")
            return False
    
    async def create_new_plan(self, template_plan: PlannerPlan, team_id: str, team_name: str) -> Optional[PlannerPlan]:
        """
        Oppretter en ny plan basert på mal.
        
        Args:
            template_plan: Mal-planen som skal kopieres
            team_id: ID til teamet som skal eie planen
            team_name: Navn på teamet
            
        Returns:
            Optional[PlannerPlan]: Den opprettede planen eller None ved feil
        """
        try:
            from msgraph.generated.models.planner_plan import PlannerPlan
            
            new_plan = PlannerPlan()
            new_plan.title = f"Prosjektplan {team_name}"
            new_plan.owner = team_id
            
            created_plan = await self.graph_client.planner.plans.post(new_plan)
            if created_plan:
                logging.info(f"Opprettet ny plan: {created_plan.id}")
                return created_plan
            else:
                logging.error("Kunne ikke opprette ny plan")
                return None
                
        except Exception as e:
            logging.error(f"Feil ved opprettelse av plan: {str(e)}")
            return None
    
    async def create_task_copy(self, template_task, new_plan_id: str, new_bucket_id: str) -> Optional[PlannerTask]:
        """
        Oppretter en ny task som kopi av mal-task.
        Lar Microsoft generere nye orderHints automatisk.
        """
        try:
            from msgraph.generated.models.planner_task import PlannerTask
            
            new_task = PlannerTask()
            new_task.title = template_task.title
            new_task.plan_id = new_plan_id
            new_task.bucket_id = new_bucket_id
            
            # Kopier original data fra mal (ikke endre states)
            new_task.percent_complete = template_task.percent_complete or 0
            new_task.priority = template_task.priority or 5
            
            # IKKE sett orderHint - la Microsoft generere automatisk
            # new_task.order_hint = ... (removed)
            
            # Kopier andre felter hvis de finnes
            if template_task.start_date_time:
                new_task.start_date_time = template_task.start_date_time
            if template_task.due_date_time:
                new_task.due_date_time = template_task.due_date_time
            
            created_task = await self.graph_client.planner.tasks.post(new_task)
            if created_task:
                logging.info(f"    Opprettet task: {template_task.title} (Microsoft genererer orderHint)")
                return created_task
            else:
                logging.error(f"    Kunne ikke opprette task: {template_task.title}")
                return None
                
        except Exception as e:
            logging.error(f"    Feil ved opprettelse av task {template_task.title}: {str(e)}")
            return None

    
    async def copy_task_details(self, template_task_id: str, new_task_id: str, task_title: str):
        """
        Kopierer task detaljer - sender separate PATCH requests for debugging.
        
        Args:
            template_task_id: ID til mal-tasken
            new_task_id: ID til den nye tasken
            task_title: Tittel på tasken (for logging)
        """
        try:
            # Bruk SDK for å hente template details (GET operasjon fungerer fint)
            template_details = await self.graph_client.planner.tasks.by_planner_task_id(template_task_id).details.get()
            if not template_details:
                logging.info(f"    Ingen detaljer å kopiere for task: {task_title}")
                return

            # Bruk SDK for å hente current details for ETag (GET operasjon fungerer fint)
            current_details = await self.graph_client.planner.tasks.by_planner_task_id(new_task_id).details.get()
            if not current_details:
                logging.error(f"    Kunne ikke hente detaljer for ny task: {task_title}")
                return

            # Hent ETag fra SDK objekt
            etag = self.get_etag_from_object(current_details)
            if not etag:
                logging.error(f"    Mangler ETag for task details: {task_title}")
                return

            # Operasjon 1: Kopier beskrivelse først (hvis den finnes)
            if template_details.description:
                description = self.extract_primitive_value(template_details.description)
                if description:
                    logging.info(f"    Kopierer beskrivelse for task: {task_title}")
                    patch_data = {'description': description}
                    
                    success, new_etag = await self.send_patch_http_with_etag(new_task_id, patch_data, etag, task_title, "beskrivelse")
                    if success and new_etag:
                        etag = new_etag
                        await asyncio.sleep(0.5)  # Pause mellom operasjoner
                    elif not success:
                        logging.error(f"    Beskrivelse PATCH feilet for: {task_title}")
                        return
            
            # Operasjon 2: Kopier checklist (hvis den finnes)
            if template_details.checklist:
                copied_checklist = self.copy_checklist_from_sdk(template_details.checklist)
                if copied_checklist:
                    logging.info(f"    Kopierer {len(copied_checklist)} checklist items for task: {task_title}")
                    patch_data = {'checklist': copied_checklist, 'previewType': 'checklist'}
                    
                    # Log første checklist item for debugging
                    first_key = next(iter(copied_checklist), None)
                    if first_key:
                        logging.debug(f"    Sample checklist item: {copied_checklist[first_key]}")
                    
                    success, new_etag = await self.send_patch_http_with_etag(new_task_id, patch_data, etag, task_title, "checklist")
                    if not success:
                        logging.error(f"    Checklist PATCH feilet for: {task_title}")
                        return

            logging.info(f"    Task detaljer kopiert for: {task_title}")
                
        except Exception as e:
            logging.error(f"    Feil ved kopiering av task detaljer for {task_title}: {str(e)}")

    async def send_patch_http_with_etag(self, task_id: str, patch_data: dict, etag: str, task_title: str, operation_type: str) -> tuple[bool, str]:
        """
        Sender PATCH request og returnerer success + ny ETag.
        
        Returns:
            tuple[bool, str]: (success, new_etag)
        """
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if not self._access_token:
                    self.get_access_token()
                    if not self._access_token:
                        logging.error("Kunne ikke hente access token")
                        return False, ""

                url = f"https://graph.microsoft.com/v1.0/planner/tasks/{task_id}/details"
                headers = {
                    "Authorization": f"Bearer {self._access_token}",
                    "Content-Type": "application/json",
                    "If-Match": etag,
                    "Prefer": "return=representation"  # Be om å få objektet tilbake
                }
                
                logging.info(f"    Sender {operation_type} PATCH til task: {task_title}")
                
                # Log patch data for debugging (men ikke hele checklist)
                if operation_type == "checklist":
                    logging.debug(f"    Patch data: checklist med {len(patch_data.get('checklist', {}))} items")
                else:
                    logging.debug(f"    Patch data: {patch_data}")
                
                async with aiohttp.ClientSession() as session:
                    async with session.patch(url, headers=headers, json=patch_data) as response:
                        response_text = await response.text()
                        
                        if response.status >= 200 and response.status < 300:
                            logging.info(f"    {operation_type.capitalize()} PATCH vellykket for: {task_title}")
                            
                            # Prøv å hente ny ETag fra response
                            new_etag = response.headers.get('ETag', '')
                            if not new_etag:
                                # Parse fra response body
                                try:
                                    import json
                                    response_json = json.loads(response_text)
                                    new_etag = response_json.get('@odata.etag', '')
                                except:
                                    new_etag = ''
                            
                            return True, new_etag
                            
                        elif response.status == 503:
                            wait_time = (2 ** attempt) + 1
                            logging.warning(f"    Service unavailable, retry {attempt+1}/{max_retries} om {wait_time}s")
                            await asyncio.sleep(wait_time)
                            continue
                            
                        elif response.status == 412:
                            # ETag mismatch - hent ny ETag via SDK
                            logging.warning(f"    ETag mismatch for {task_title}, henter ny ETag")
                            if attempt < max_retries - 1:
                                fresh_details = await self.graph_client.planner.tasks.by_planner_task_id(task_id).details.get()
                                new_etag = self.get_etag_from_object(fresh_details)
                                if new_etag:
                                    etag = new_etag
                                    await asyncio.sleep(1)
                                    continue
                            logging.error(f"    Kunne ikke hente ny ETag for {task_title}")
                            return False, ""
                                
                        elif response.status == 400:
                            logging.error(f"    400 Bad Request for {operation_type} på {task_title}")
                            logging.error(f"    Response: {response_text}")
                            logging.error(f"    ETag used: {etag}")
                            
                            # Detaljert logging for checklist
                            if operation_type == "checklist" and 'checklist' in patch_data:
                                checklist = patch_data['checklist']
                                logging.error(f"    Checklist keys: {list(checklist.keys())[:3]}...")  # Første 3 keys
                                first_key = next(iter(checklist), None)
                                if first_key:
                                    item = checklist[first_key]
                                    logging.error(f"    First item structure: {item}")
                                    logging.error(f"    First item title type: {type(item.get('title'))}")
                                    logging.error(f"    First item isChecked type: {type(item.get('isChecked'))}")
                            
                            return False, ""
                            
                        else:
                            logging.error(f"    {operation_type} PATCH feilet for {task_title}: {response.status} - {response_text}")
                            return False, ""
                            
            except aiohttp.ClientError as e:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 1
                    logging.warning(f"    Connection error, retry {attempt+1}/{max_retries} om {wait_time}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                else:
                    logging.error(f"    Max retries nådd for {operation_type} på {task_title}: {str(e)}")
                    return False, ""
            except Exception as e:
                logging.error(f"    Uventet feil ved {operation_type} PATCH for {task_title}: {str(e)}")
                return False, ""
        
        return False, ""

    def copy_checklist_from_sdk(self, original_checklist) -> dict:
        """
        Kopier checklist sortert korrekt uten orderHints - la Microsoft generere nye.
        """
        try:
            if not original_checklist:
                return {}
            
            # Ekstraher data
            checklist_data = None
            if hasattr(original_checklist, 'additional_data') and original_checklist.additional_data:
                checklist_data = original_checklist.additional_data
            elif isinstance(original_checklist, dict):
                checklist_data = original_checklist
            
            if not checklist_data:
                return {}
            
            # Samle items med order hint for sortering (kun for sorting, ikke sending)
            items_with_order = []
            
            for old_key, item in checklist_data.items():
                try:
                    title = ""
                    is_checked = False
                    original_order_hint = ""
                    
                    if isinstance(item, dict):
                        title = str(item.get("title", "")).strip()
                        is_checked = bool(item.get("isChecked", False))
                        original_order_hint = str(item.get("orderHint", ""))
                    elif hasattr(item, 'title'):
                        title = str(self.extract_primitive_value(getattr(item, 'title', ''))).strip()
                        is_checked = bool(getattr(item, 'is_checked', False) or getattr(item, 'isChecked', False))
                        original_order_hint = str(getattr(item, 'order_hint', '') or getattr(item, 'orderHint', ''))
                    
                    if not title:
                        logging.warning(f"Hopper over item uten title: {old_key}")
                        continue
                    
                    if len(title) > 255:
                        title = title[:255]
                        logging.warning(f"Kortet ned lang title: {title[:50]}...")
                    
                    items_with_order.append({
                        'title': title,
                        'isChecked': is_checked,
                        'orderHint': original_order_hint,  # Kun for sorting
                        'original_key': old_key
                    })
                    
                except Exception as item_error:
                    logging.warning(f"Kunne ikke prosessere checklist item {old_key}: {str(item_error)}")
                    continue
            
            # ASCII-basert sortering basert på original orderHint (reverse for UI match)
            def pure_ascii_sort_key(item):
                """Garanterer ren ASCII-basert sammenligning."""
                order_hint = item['orderHint'] or ""
                # Convert to ASCII bytes for guaranteed ASCII ordering
                ascii_bytes = order_hint.encode('ascii', errors='replace')
                return ascii_bytes
            
            items_with_order.sort(key=pure_ascii_sort_key, reverse=True)
            
            logging.info("Checklist items sortert basert på original orderHint (Microsoft genererer nye):")
            for i, item in enumerate(items_with_order, 1):
                logging.info(f"  {i}. '{item['title']}' (original orderHint: {item['orderHint']})")
            
            # Bygg result dict UTEN orderHint - la Microsoft generere
            result = {}
            for item in items_with_order:
                new_key = str(uuid.uuid4())
                
                item_data = {
                    "@odata.type": "microsoft.graph.plannerChecklistItem",
                    "title": item['title'],
                    "isChecked": item['isChecked']
                    # INGEN orderHint - Microsoft genererer automatisk
                }
                
                result[new_key] = item_data
            
            logging.info(f"Kopiert {len(result)} checklist items sortert korrekt (Microsoft genererer orderHints)")
            return result
            
        except Exception as e:
            logging.error(f"Feil ved kopiering av checklist: {str(e)}")
            return {}

    async def send_patch_http(self, task_id: str, patch_data: dict, etag: str, task_title: str) -> bool:
        """
        Sender PATCH request via direkte HTTP (SDK har serialiseringsproblemer her).
        
        Returns:
            bool: True hvis vellykket
        """
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if not self._access_token:
                    self.get_access_token()
                    if not self._access_token:
                        logging.error("Kunne ikke hente access token")
                        return False

                url = f"https://graph.microsoft.com/v1.0/planner/tasks/{task_id}/details"
                headers = {
                    "Authorization": f"Bearer {self._access_token}",
                    "Content-Type": "application/json",
                    "If-Match": etag
                }
                
                async with aiohttp.ClientSession() as session:
                    async with session.patch(url, headers=headers, json=patch_data) as response:
                        response_text = await response.text()
                        
                        if response.status >= 200 and response.status < 300:
                            logging.info(f"    PATCH vellykket for task: {task_title}")
                            return True
                            
                        elif response.status == 503:
                            wait_time = (2 ** attempt) + 1
                            logging.warning(f"    Service unavailable, retry {attempt+1}/{max_retries} om {wait_time}s")
                            await asyncio.sleep(wait_time)
                            continue
                            
                        elif response.status == 412:
                            # ETag mismatch - hent ny ETag via SDK
                            logging.warning(f"    ETag mismatch for {task_title}, henter ny ETag")
                            if attempt < max_retries - 1:
                                fresh_details = await self.graph_client.planner.tasks.by_planner_task_id(task_id).details.get()
                                new_etag = self.get_etag_from_object(fresh_details)
                                if new_etag:
                                    etag = new_etag
                                    await asyncio.sleep(1)
                                    continue
                            logging.error(f"    Kunne ikke hente ny ETag for {task_title}")
                            return False
                                
                        elif response.status == 400:
                            logging.error(f"    400 Bad Request for {task_title}")
                            logging.error(f"    Response: {response_text}")
                            return False
                            
                        else:
                            logging.error(f"    PATCH feilet for {task_title}: {response.status} - {response_text}")
                            return False
                            
            except aiohttp.ClientError as e:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 1
                    logging.warning(f"    Connection error, retry {attempt+1}/{max_retries} om {wait_time}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                else:
                    logging.error(f"    Max retries nådd for {task_title}: {str(e)}")
                    return False
            except Exception as e:
                logging.error(f"    Uventet feil ved PATCH for {task_title}: {str(e)}")
                return False
        
        return False

    def extract_primitive_value(self, value):
        """
        Ekstraherer primitive verdier fra Graph SDK objekter.
        
        Args:
            value: Verdi som kan være et Graph SDK objekt eller primitiv type
            
        Returns:
            Primitiv verdi som kan serialiseres til JSON
        """
        try:
            if value is None:
                return None
            
            # Hvis det allerede er en primitiv type
            if isinstance(value, (str, int, float, bool)):
                return value
            
            # Hvis det er en liste
            if isinstance(value, list):
                return [self.extract_primitive_value(item) for item in value]
            
            # Hvis det er en dict
            if isinstance(value, dict):
                return {k: self.extract_primitive_value(v) for k, v in value.items()}
            
            # For Graph SDK objekter - prøv å få verdien direkte
            if hasattr(value, '__str__') and not hasattr(value, '__dict__'):
                return str(value)
            
            # Hvis objektet har en verdi som kan ekstraheres
            if hasattr(value, 'value'):
                return self.extract_primitive_value(value.value)
            
            # Hvis det er et objekt med additional_data
            if hasattr(value, 'additional_data') and value.additional_data:
                return self.extract_primitive_value(value.additional_data)
            
            # Prøv å konvertere til string som siste utvei
            return str(value) if value else None
            
        except Exception as e:
            logging.warning(f"Kunne ikke ekstraherer primitiv verdi: {str(e)}")
            return None

    def get_etag_from_object(self, obj) -> str:
        """
        Henter ETag fra et Graph API objekt - prøver forskjellige mulige attributtnavn.
        
        Args:
            obj: Graph API objekt
            
        Returns:
            str: ETag verdi eller tom string hvis ikke funnet
        """
        try:
            # Prøv forskjellige mulige ETag attributter
            possible_etag_attrs = [
                'odata_etag',
                '@odata.etag', 
                'etag',
                '_etag',
                'e_tag'
            ]
            
            for attr in possible_etag_attrs:
                if hasattr(obj, attr):
                    etag = getattr(obj, attr)
                    if etag:
                        return etag
            
            # Sjekk additional_data hvis det finnes
            if hasattr(obj, 'additional_data') and obj.additional_data:
                for key in ['@odata.etag', 'odata.etag', 'etag']:
                    if key in obj.additional_data:
                        etag = obj.additional_data[key]
                        if etag:
                            return etag
            
            # Sjekk om objektet har en __dict__ med ETag info
            if hasattr(obj, '__dict__'):
                for key, value in obj.__dict__.items():
                    if 'etag' in key.lower() and value:
                        return value
            
            return ""
            
        except Exception as e:
            logging.error(f"Feil ved henting av ETag: {str(e)}")
            return ""
        
    async def add_planner_tab_to_team(self, team_id: str, plan_id: str, plan_title: str, channel_id: str = None) -> bool:
        """
        Legger til Planner tab med manuell HTTP request.
        """
        try:
            # If no channel_id provided, get the General channel
            if not channel_id:
                channel_id = await self.get_general_channel_id(team_id)
                if not channel_id:
                    logging.error("Kunne ikke finne General kanal")
                    return False

            tenant_id = os.getenv("AZURE_TENANT_ID")
            
            if not tenant_id:
                logging.error("Kunne ikke hente tenant ID")
                return False

            # Get access token from your graph client
            # This depends on how your graph client is set up
            
            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/tabs"
            
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json"
            }
            
            request_body = {
                "displayName": plan_title,
                "teamsApp@odata.bind": "https://graph.microsoft.com/v1.0/appCatalogs/teamsApps/com.microsoft.teamspace.tab.planner",
                "configuration": {
                    "entityId": plan_id,
                    "contentUrl": f"https://tasks.office.com/{tenant_id}/Home/PlannerFrame?page=7&planId={plan_id}",
                    "websiteUrl": f"https://tasks.office.com/{tenant_id}/Home/PlanViews/{plan_id}"
                }
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=request_body) as response:
                    if response.status == 201:
                        result = await response.json()
                        logging.info(f"Planner tab '{result['displayName']}' lagt til med ID: {result['id']}")
                        return True
                    else:
                        error_text = await response.text()
                        logging.error(f"HTTP {response.status}: {error_text}")
                        return False
                        
        except Exception as e:
            logging.error(f"Feil ved tillegging av Planner tab: {str(e)}")
            return False


    async def get_general_channel_id(self, team_id: str) -> str:
        """
        Henter ID til General kanalen i et team.
        
        Args:
            team_id: ID til teamet
            
        Returns:
            str: Channel ID eller tom string hvis ikke funnet
        """
        try:
            channels = await self.graph_client.teams.by_team_id(team_id).channels.get()
            
            if channels and channels.value:
                for channel in channels.value:
                    # Generelt kanal har spesielle egenskaper
                    if channel.display_name == "Generelt" or channel.membership_type == "standard":
                        return channel.id
            
            logging.warning(f"General kanal ikke funnet for team {team_id}")
            return ""
            
        except Exception as e:
            logging.error(f"Feil ved henting av General kanal: {str(e)}")
            return ""