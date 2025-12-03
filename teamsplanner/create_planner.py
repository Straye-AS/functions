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

    def __init__(
        self, graph_client: GraphServiceClient, credentials: ClientSecretCredential
    ):
        # Klienter og credentials
        self.graph_client = graph_client
        self.credentials = credentials

        # Konfigurasjon
        self.template_planner_id = os.getenv("TEAMS_PLANNER_TEMPLATE_ID")

        # Testing mode - sett til True for å kun prosessere "Testing" teams og kun legge til utviklere
        self.testing_mode = os.getenv("TESTING_MODE", "false").lower() == "true"

        # Medlemskonfigurasjon - lett å endre her!
        # Utviklere (alltid lagt til som team owners, både i testing og produksjon)
        self.developers = ["robot@straye.no"]

        # Admin channel owners (alltid kun utviklere)
        self.admin_channel_owners = self.developers

        # Company-specific owner mapping based on key members
        # Maps key member email to (company_name, list_of_owners)
        self.company_owner_mapping = {
            "henrik@straye.no": ("Tak", [
                "henrik@straye.no",
                "robert.russvoll@straye.no",
                "marek@straye.no",
                "kristoffer.lund@straye.no",
                "atle@straye.no",
                "dennis@straye.no",
                "andreas@straye.no"
            ]),
            "christer@straye.no": ("Hybridbygg", [
                "christer@straye.no",
                "julie@straye.no",
                "daniel@straye.no",
                "gjermund@straye.no",
                "arne@straye.no",
                "karoline@straye.no",
                "jenil@straye.no",
                "andreas@straye.no",
                "dennis@straye.no",
                "lg@straye.no"
            ]),
            "ali@straye.no": ("Stålbygg", [
                "daniel@straye.no",
                "jenil@straye.no",
                "dennis@straye.no",
                "ali@straye.no",
                "camilla@straye.no",
                "jan@straye.no",
                "trond@straye.no",
                "andreas@straye.no",
                "jacek.sztyler@straye.no",
                "fredrik@straye.no",
                "maksymilian@straye.no",
                "linus@straye.no",
                "christian@straye.no",
                "christian.quist@straye.no"
                "tommy@straye.no"
            ]),
            "sven@straye.no": ("Industri", [
                "sven@straye.no",
                "frode@straye.no",
                "jenil@straye.no",
                "daniel@straye.no",
                "dennis@straye.no",
                "ali@straye.no",
                "camilla@straye.no",
                "andreas@straye.no"
            ])
        }

        # Cache
        self._template_plan: Optional[PlannerPlan] = None
        self._access_token: Optional[str] = None

        # Timing konfigurasjon
        self.bucket_delay = 0.1  # Sekunder mellom bucket-opprettelser
        self.task_delay = 0.1  # Sekunder mellom task-opprettelser
        self.details_delay = 0.1  # Sekunder mellom details-oppdateringer

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
            token = self.credentials.get_token("https://graph.microsoft.com/.default")
            self._access_token = token.token
            return self._access_token
        except Exception as e:
            logging.error(f"Feil ved henting av access token: {str(e)}")
            return None

    async def _is_robot_already_owner(self, team_id: str, robot_email: str) -> bool:
        """
        Sjekker om robot@straye.no allerede er owner av teamet.

        Args:
            team_id: ID til teamet
            robot_email: Email til robot brukeren

        Returns:
            bool: True hvis robot er owner, False ellers
        """
        try:
            if not self._access_token:
                self._access_token = self.get_access_token()
                if not self._access_token:
                    logging.warning("Kunne ikke hente access token for å sjekke robot owner status")
                    return False

            check_url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/members"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(check_url, headers=headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        members = result.get("value", [])
                        for member in members:
                            member_email = member.get("email") or member.get("userPrincipalName")
                            if member_email and member_email.lower() == robot_email.lower():
                                member_roles = member.get("roles", [])
                                is_owner = "owner" in member_roles
                                if is_owner:
                                    return True
                        return False
                    else:
                        logging.warning(f"Kunne ikke hente medlemmer for team {team_id}, status: {response.status}")
                        return False

        except Exception as e:
            logging.warning(f"Feil ved sjekk om robot er owner: {str(e)}")
            return False

    async def get_template_planner(self) -> Optional[PlannerPlan]:
        """
        Henter og cacher mal-planneren.

        Returns:
            Optional[PlannerPlan]: Mal-planneren eller None ved feil
        """
        if self._template_plan:
            return self._template_plan

        try:
            plan = await self.graph_client.planner.plans.by_planner_plan_id(
                self.template_planner_id
            ).get()
            if not plan:
                raise ValueError(
                    f"Fant ikke mal-planneren med ID: {self.template_planner_id}"
                )

            self._template_plan = plan
            logging.info(f"Cachet mal-planner: {plan.title}")
            return plan

        except Exception as e:
            logging.error(f"Feil ved henting av mal-planner: {str(e)}")
            return None

    async def create_planner_for_team(
        self, team_id: str, team_name: str, visibility: str = None
    ) -> Optional[Dict]:
        """
        Oppretter en ny planner for et team ved å kopiere eksisterende mal.
        Idempotent: Sjekker hvert komponent individuelt og oppretter kun det som mangler.

        Args:
            team_id: ID til teamet som skal få en ny planner
            team_name: Navn på teamet
            visibility: Visibility av teamet ("private" eller "public")

        Returns:
            Optional[Dict]: Informasjon om den nye planneren hvis vellykket, None ved feil
        """
        try:
            # Log current mode
            mode_msg = "🧪 TESTING MODE" if self.testing_mode else "🚀 PRODUCTION MODE"
            logging.info(f"{mode_msg} - Processing team: {team_name}")

            # SKIP PUBLIC TEAMS: Public teams trenger ikke planner og spesialkanaler
            if visibility and visibility.lower() == "public":
                logging.info(
                    f"⏭️  SKIPPING team '{team_name}' - team is PUBLIC, only processing PRIVATE teams"
                )
                return True  # Returner True for å markere som prosessert

            # Hent mal-planner
            template_plan = await self.get_template_planner()
            if not template_plan:
                logging.error("Kunne ikke hente mal-planner")
                return None

            logging.info(f"Hentet mal-planner: {template_plan.title}")

            # SAFETY CHECK: In testing mode, only process teams with "Testing" in the name
            if self.testing_mode and "Testing" not in team_name:
                logging.info(
                    f"⏭️  SKIPPING team '{team_name}' - testing mode active and team name does not contain 'Testing'"
                )
                return None

            # Sjekk om robot@straye.no allerede er owner - hvis ja, hopp over all prosessering
            robot_email = self.developers[0]  # robot@straye.no
            if await self._is_robot_already_owner(team_id, robot_email):
                logging.info(
                    f"✅ Team {team_id} har allerede {robot_email} som owner - hopper over all prosessering"
                )
                return True  # Return success så teamet markeres som prosessert

            # Sikre at utviklere (robot) er team OWNERS
            logging.info(
                f"Sikrer at {len(self.developers)} utvikler(e) er team owners..."
            )
            for email in self.developers:
                await self.ensure_user_in_team(team_id, email, as_owner=True)

            # Bestem company-spesifikke owners og legg dem til som team owners
            if not self.testing_mode:
                company_name, company_owners = await self.determine_company_owners(team_id)

                if company_name and company_owners:
                    logging.info(
                        f"🏢 Legger til {len(company_owners)} {company_name}-spesifikke owners som team owners..."
                    )
                    for email in company_owners:
                        await self.ensure_user_in_team(team_id, email, as_owner=True)
                else:
                    logging.info(
                        "ℹ️ Ingen company-spesifikke owners funnet - kun utviklere er team owners"
                    )
            else:
                logging.info("Testing mode: Hopper over company-spesifikke team owners")

            # STEP 1: Check if planner exists, if not create it
            logging.info("📋 Sjekker om team har planner...")
            planner_exists = await self.team_has_existing_planners(team_id)
            created_plan = None

            if planner_exists:
                logging.info(
                    f"✅ Team {team_id} har allerede planner, hopper over opprettelse"
                )
                # Get existing planner ID for notification later
                try:
                    existing_plans = await self.graph_client.groups.by_group_id(
                        team_id
                    ).planner.plans.get()
                    if existing_plans and existing_plans.value:
                        created_plan = existing_plans.value[0]
                        logging.info(
                            f"Hentet eksisterende planner: {created_plan.title}"
                        )
                except Exception as e:
                    logging.warning(
                        f"Kunne ikke hente eksisterende planner detaljer: {str(e)}"
                    )
                    # This is OK - we just won't have plan details for the notification
            else:
                logging.info("🚀 Oppretter ny planner...")
                # Opprett ny plan
                created_plan = await self.create_new_plan(
                    template_plan, team_id, team_name
                )
                if not created_plan:
                    logging.error("Kunne ikke opprette planner")
                    return None

                # Kopier buckets og tasks sekvensielt
                success = await self.copy_template_sequentially(
                    template_plan.id, created_plan.id
                )
                if not success:
                    logging.error("Kopiering av template feilet")
                    return None

                # Wait a bit for the planner to be fully created and accessible
                logging.info(
                    "Venter 3 sekunder for at planner skal bli fullstendig tilgjengelig..."
                )
                await asyncio.sleep(3)

                # Legg til Planner tab i General kanal
                tab_success = await self.add_planner_tab_to_team(
                    team_id,
                    created_plan.id,
                    created_plan.title,
                )
                if tab_success:
                    logging.info(
                        f"✅ Planner tab lagt til i General kanal for team {team_id}"
                    )
                else:
                    logging.error(
                        f"❌ Kunne ikke legge til Planner tab i General kanal for team {team_id}"
                    )

            # STEP 2: Check and create admin channel if needed
            logging.info("🔑 Sjekker Administrasjon kanal...")
            admin_result = await self.get_or_create_channel(
                team_id,
                "Administrasjon 🔑",
                "For administrasjon og planlegging",
                is_private=True,
            )

            if not admin_result:
                logging.error("❌ Kunne ikke opprette Administrasjon kanal")
                return None

            admin_channel_id, admin_was_created = admin_result
            logging.info(
                f"{'✨ Opprettet' if admin_was_created else '✅ Fant eksisterende'} Administrasjon kanal: {admin_channel_id}"
            )

            # STEP 3: Check and create montasje channel if needed
            logging.info("🏗️ Sjekker Montasje kanal...")
            montasje_result = await self.get_or_create_channel(
                team_id,
                "Montasje 🏗️",
                "For montasje og utførelse",
                membership_type="shared",
            )

            montasje_channel_id = None
            montasje_was_created = False
            if montasje_result:
                montasje_channel_id, montasje_was_created = montasje_result
                logging.info(
                    f"{'✨ Opprettet' if montasje_was_created else '✅ Fant eksisterende'} Montasje kanal: {montasje_channel_id}"
                )
            else:
                logging.warning("⚠️ Kunne ikke opprette Montasje kanal, men fortsetter")

            # Track if we need to add members and send welcome messages (only if any channel was created)
            channels_were_created = admin_was_created or montasje_was_created

            # Track if we need to add tabs and send welcome messages (even if channels already exist)
            # This ensures proper setup even when channels exist but aren't configured correctly
            should_setup_channels = (
                admin_channel_id is not None or montasje_channel_id is not None
            )

            # STEP 4: Add members to newly created channels only
            if channels_were_created:
                logging.info("👥 Legger til medlemmer i nyopprettede kanaler...")

                # Wait for channels to be fully created
                logging.info(
                    "Venter 5 sekunder for at kanalene skal bli fullstendig opprettet..."
                )
                await asyncio.sleep(5)

                # Bestem company-spesifikke owners basert på teammedlemmer
                company_name, company_owners = await self.determine_company_owners(team_id)
                company_owners_lower = [email.lower() for email in company_owners]

                if company_name:
                    logging.info(
                        f"🏢 Bruker {company_name}-spesifikke owners: {len(company_owners)} personer"
                    )
                else:
                    logging.info(
                        "🏢 Ingen company-spesifikk konfigurasjon funnet, bruker kun admin owners"
                    )

                # Hent ALLE medlemmer i teamet med deres roller
                all_team_members = await self.get_all_team_members_with_roles(team_id)

                if all_team_members:
                    logging.info(
                        f"Fant {len(all_team_members)} medlemmer i teamet som skal legges til i kanalene"
                    )

                    # Legg til medlemmer i admin kanal hvis den ble opprettet
                    if admin_was_created and admin_channel_id:
                        logging.info(
                            "Legger til alle teammedlemmer i Administrasjon kanal..."
                        )
                        for member in all_team_members:
                            member_email = member["email"]
                            # Skip de som allerede er lagt til som channel owners ved opprettelse
                            if member_email.lower() not in [
                                e.lower() for e in self.admin_channel_owners
                            ]:
                                # Bestem om brukeren skal være owner basert på company-listen
                                should_be_owner = member_email.lower() in company_owners_lower
                                await self.add_member_to_channel(
                                    team_id,
                                    admin_channel_id,
                                    member_email,
                                    is_owner=should_be_owner,
                                )

                    # Legg til medlemmer i montasje kanal hvis den ble opprettet
                    if montasje_was_created and montasje_channel_id:
                        logging.info(
                            "Legger til alle teammedlemmer individuelt i Montasje kanal..."
                        )
                        for member in all_team_members:
                            member_email = member["email"]
                            # Skip de som allerede er lagt til som channel owners ved opprettelse
                            if member_email.lower() not in [
                                e.lower() for e in self.admin_channel_owners
                            ]:
                                # Bestem om brukeren skal være owner basert på company-listen
                                should_be_owner = member_email.lower() in company_owners_lower
                                await self.add_member_to_channel(
                                    team_id,
                                    montasje_channel_id,
                                    member_email,
                                    is_owner=should_be_owner,
                                )
                else:
                    logging.warning("Ingen teammedlemmer funnet å legge til i kanalene")
            else:
                logging.info(
                    "✅ Kanaler eksisterte allerede, hopper over medlemsopprettelse"
                )

            # STEP 5: Add SharePoint tab to admin channel (if admin channel exists)
            if admin_channel_id and channels_were_created:
                logging.info(
                    "⏳ Venter 2 minutter for at private kanal skal være klar for SharePoint..."
                )
                await asyncio.sleep(120)  # Wait 2 minutes initially

                # Get the SharePoint URL from the General channel's Files tab
                general_channel_id = await self.get_general_channel_id(team_id)
                sharepoint_url = await self.get_general_files_sharepoint_url(
                    team_id, general_channel_id
                )

                if sharepoint_url:
                    logging.info(f"Fant SharePoint URL: {sharepoint_url}")

                    # Try with extended retry logic (every 30 seconds for up to 5 more minutes)
                    sharepoint_tab_success = await self.add_sharepoint_tab_with_retry(
                        team_id,
                        admin_channel_id,
                        "Prosjektfiler 🗃️",
                        sharepoint_url,
                        max_retries=10,  # 10 retries * 30 seconds = 5 more minutes
                        retry_delay=30,
                    )
                    if sharepoint_tab_success:
                        logging.info(
                            f"✅ SharePoint tab 'Prosjektfiler 🗃️' lagt til i Administrasjon kanal for team {team_id}"
                        )
                    else:
                        logging.warning(
                            f"⚠️ Kunne ikke legge til SharePoint tab etter 10 minutter - kanal kanskje ikke klar ennå"
                        )
                else:
                    logging.warning(
                        "⚠️ Kunne ikke finne SharePoint URL fra General kanal"
                    )

            # STEP 6: Send welcome notification (if channels exist)
            if should_setup_channels:
                logging.info("📧 Sender velkomstmelding for kanaler...")
                general_channel_id = await self.get_general_channel_id(team_id)
                await self.send_channel_info_notification(
                    team_id=team_id,
                    team_name=team_name,
                    general_channel_id=general_channel_id,
                    admin_channel_id=admin_channel_id,
                    montasje_channel_id=montasje_channel_id,
                    planner_id=created_plan.id if created_plan else None,
                )
            else:
                logging.info(
                    "✅ Kanaler eksisterte allerede, hopper over velkomstmelding"
                )

            # All steps completed successfully!
            logging.info(f"✅ Team setup fullført for {team_name}!")

            return {
                "id": created_plan.id if created_plan else None,
                "title": created_plan.title if created_plan else None,
                "owner": created_plan.owner if created_plan else None,
                "created_date_time": (
                    created_plan.created_date_time if created_plan else None
                ),
                "admin_channel_id": admin_channel_id,
                "montasje_channel_id": montasje_channel_id,
            }

        except Exception as e:
            logging.error(
                f"Feil ved opprettelse av planner for team {team_id}: {str(e)}"
            )
            return None

    async def copy_template_sequentially(
        self, template_plan_id: str, new_plan_id: str
    ) -> bool:
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
            template_buckets = await self.graph_client.planner.plans.by_planner_plan_id(
                template_plan_id
            ).buckets.get()
            if not template_buckets or not template_buckets.value:
                logging.info("Ingen buckets å kopiere")
                return True

            # Hent alle tasks fra template
            template_tasks = await self.graph_client.planner.plans.by_planner_plan_id(
                template_plan_id
            ).tasks.get()
            if not template_tasks or not template_tasks.value:
                logging.info("Ingen tasks å kopiere")
                # Kopier bare tomme buckets
                return await self.copy_empty_buckets(
                    template_buckets.value, new_plan_id
                )

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
                sorted_tasks = sorted(
                    tasks_by_bucket[bucket_id], key=pure_ascii_task_sort_key
                )
                tasks_by_bucket[bucket_id] = sorted_tasks

            # Log tasks i sortert rekkefölge for debugging
            logging.info("Tasks sortert med ASCII-basert orderHint sammenligning:")
            for bucket_id, tasks in tasks_by_bucket.items():
                bucket_name = next(
                    (b.name for b in template_buckets.value if b.id == bucket_id),
                    "Unknown",
                )
                logging.info(f"  Bucket '{bucket_name}': {len(tasks)} tasks")
                for i, task in enumerate(tasks, 1):
                    logging.info(
                        f"    {i}. {task.title} (orderHint: '{task.order_hint}')"
                    )

            # Sorter buckets etter order hint (buckets sorting fungerer korrekt)
            def ascii_bucket_sort_key(bucket):
                """Returnerer bucket orderHint for ASCII sammenligning."""
                return bucket.order_hint or ""

            sorted_buckets = sorted(
                template_buckets.value, key=ascii_bucket_sort_key, reverse=True
            )

            logging.info(
                f"Starter sekvensiell kopiering av {len(sorted_buckets)} buckets"
            )
            logging.info("Bucket rekkefölge:")
            for i, bucket in enumerate(sorted_buckets, 1):
                logging.info(f"  {i}. {bucket.name} (orderHint: '{bucket.order_hint}')")

            # Prosesser hver bucket komplett
            total_buckets = len(sorted_buckets)
            total_tasks = 0
            successful_tasks = 0

            for bucket_index, bucket in enumerate(sorted_buckets, 1):
                logging.info(f"")
                logging.info(
                    f"=== BUCKET {bucket_index}/{total_buckets}: {bucket.name} ==="
                )

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

                logging.info(
                    f"Kopierer {len(bucket_tasks)} tasks for bucket: {bucket.name} (ASCII-sortert)"
                )
                bucket_successful = 0

                for task_index, task in enumerate(bucket_tasks, 1):
                    total_tasks += 1

                    try:
                        logging.info(
                            f"  Task {task_index}/{len(bucket_tasks)}: {task.title}"
                        )
                        logging.debug(f"    Original orderHint: '{task.order_hint}'")

                        # Steg 2a: Opprett task
                        created_task = await self.create_task_copy(
                            task, new_plan_id, new_bucket.id
                        )
                        if not created_task:
                            logging.error(f"    Kunne ikke opprette task: {task.title}")
                            continue

                        # Pause etter task opprettelse
                        await asyncio.sleep(self.task_delay)

                        # Steg 2b: Kopier task details
                        await self.copy_task_details(
                            task.id, created_task.id, task.title
                        )

                        successful_tasks += 1
                        bucket_successful += 1

                        # Pause mellom task details
                        await asyncio.sleep(self.details_delay)

                    except Exception as task_error:
                        logging.error(
                            f"    Feil ved kopiering av task '{task.title}': {str(task_error)}"
                        )
                        continue

                logging.info(
                    f"Bucket '{bucket.name}' fullført: {bucket_successful}/{len(bucket_tasks)} tasks kopiert"
                )

                # Pause mellom buckets
                if bucket_index < total_buckets:
                    logging.info(f"Venter {self.bucket_delay}s før neste bucket...")
                    await asyncio.sleep(self.bucket_delay)

            logging.info(f"")
            logging.info(f"=== KOPIERING FULLFØRT ===")
            logging.info(
                f"Totalt: {successful_tasks}/{total_tasks} tasks kopiert på tvers av {total_buckets} buckets"
            )

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

                created_bucket = await self.graph_client.planner.buckets.post(
                    new_bucket
                )
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
            logging.error(
                f"Feil ved opprettelse av bucket {template_bucket.name}: {str(e)}"
            )
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
            existing_plans = await self.graph_client.groups.by_group_id(
                team_id
            ).planner.plans.get()
            if existing_plans and existing_plans.value:
                logging.info(
                    f"Team {team_id} har allerede {len(existing_plans.value)} planner(e)"
                )
                logging.info(
                    f"Eksisterende planners: {[it.title for it in existing_plans.value]}"
                )
                return True
            return False
        except Exception as e:
            logging.error(f"Feil ved sjekk av eksisterende planner: {str(e)}")
            return False

    async def create_new_plan(
        self, template_plan: PlannerPlan, team_id: str, team_name: str
    ) -> Optional[PlannerPlan]:
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

    async def create_task_copy(
        self, template_task, new_plan_id: str, new_bucket_id: str
    ) -> Optional[PlannerTask]:
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
                logging.info(
                    f"    Opprettet task: {template_task.title} (Microsoft genererer orderHint)"
                )
                return created_task
            else:
                logging.error(f"    Kunne ikke opprette task: {template_task.title}")
                return None

        except Exception as e:
            logging.error(
                f"    Feil ved opprettelse av task {template_task.title}: {str(e)}"
            )
            return None

    async def copy_task_details(
        self, template_task_id: str, new_task_id: str, task_title: str
    ):
        """
        Kopierer task detaljer - sender separate PATCH requests for debugging.

        Args:
            template_task_id: ID til mal-tasken
            new_task_id: ID til den nye tasken
            task_title: Tittel på tasken (for logging)
        """
        try:
            # Bruk SDK for å hente template details (GET operasjon fungerer fint)
            template_details = await self.graph_client.planner.tasks.by_planner_task_id(
                template_task_id
            ).details.get()
            if not template_details:
                logging.info(f"    Ingen detaljer å kopiere for task: {task_title}")
                return

            # Bruk SDK for å hente current details for ETag (GET operasjon fungerer fint)
            current_details = await self.graph_client.planner.tasks.by_planner_task_id(
                new_task_id
            ).details.get()
            if not current_details:
                logging.error(
                    f"    Kunne ikke hente detaljer for ny task: {task_title}"
                )
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
                    patch_data = {"description": description}

                    success, new_etag = await self.send_patch_http_with_etag(
                        new_task_id, patch_data, etag, task_title, "beskrivelse"
                    )
                    if success and new_etag:
                        etag = new_etag
                        await asyncio.sleep(0.5)  # Pause mellom operasjoner
                    elif not success:
                        logging.error(f"    Beskrivelse PATCH feilet for: {task_title}")
                        return

            # Operasjon 2: Kopier checklist (hvis den finnes)
            if template_details.checklist:
                copied_checklist = self.copy_checklist_from_sdk(
                    template_details.checklist
                )
                if copied_checklist:
                    logging.info(
                        f"    Kopierer {len(copied_checklist)} checklist items for task: {task_title}"
                    )
                    patch_data = {
                        "checklist": copied_checklist,
                        "previewType": "checklist",
                    }

                    # Log første checklist item for debugging
                    first_key = next(iter(copied_checklist), None)
                    if first_key:
                        logging.debug(
                            f"    Sample checklist item: {copied_checklist[first_key]}"
                        )

                    success, new_etag = await self.send_patch_http_with_etag(
                        new_task_id, patch_data, etag, task_title, "checklist"
                    )
                    if not success:
                        logging.error(f"    Checklist PATCH feilet for: {task_title}")
                        return

            logging.info(f"    Task detaljer kopiert for: {task_title}")

        except Exception as e:
            logging.error(
                f"    Feil ved kopiering av task detaljer for {task_title}: {str(e)}"
            )

    async def send_patch_http_with_etag(
        self,
        task_id: str,
        patch_data: dict,
        etag: str,
        task_title: str,
        operation_type: str,
    ) -> tuple[bool, str]:
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

                url = (
                    f"https://graph.microsoft.com/v1.0/planner/tasks/{task_id}/details"
                )
                headers = {
                    "Authorization": f"Bearer {self._access_token}",
                    "Content-Type": "application/json",
                    "If-Match": etag,
                    "Prefer": "return=representation",  # Be om å få objektet tilbake
                }

                logging.info(
                    f"    Sender {operation_type} PATCH til task: {task_title}"
                )

                # Log patch data for debugging (men ikke hele checklist)
                if operation_type == "checklist":
                    logging.debug(
                        f"    Patch data: checklist med {len(patch_data.get('checklist', {}))} items"
                    )
                else:
                    logging.debug(f"    Patch data: {patch_data}")

                async with aiohttp.ClientSession() as session:
                    async with session.patch(
                        url, headers=headers, json=patch_data
                    ) as response:
                        response_text = await response.text()

                        if response.status >= 200 and response.status < 300:
                            logging.info(
                                f"    {operation_type.capitalize()} PATCH vellykket for: {task_title}"
                            )

                            # Prøv å hente ny ETag fra response
                            new_etag = response.headers.get("ETag", "")
                            if not new_etag:
                                # Parse fra response body
                                try:
                                    import json

                                    response_json = json.loads(response_text)
                                    new_etag = response_json.get("@odata.etag", "")
                                except:
                                    new_etag = ""

                            return True, new_etag

                        elif response.status == 503:
                            wait_time = (2**attempt) + 1
                            logging.warning(
                                f"    Service unavailable, retry {attempt+1}/{max_retries} om {wait_time}s"
                            )
                            await asyncio.sleep(wait_time)
                            continue

                        elif response.status == 412:
                            # ETag mismatch - hent ny ETag via SDK
                            logging.warning(
                                f"    ETag mismatch for {task_title}, henter ny ETag"
                            )
                            if attempt < max_retries - 1:
                                fresh_details = await self.graph_client.planner.tasks.by_planner_task_id(
                                    task_id
                                ).details.get()
                                new_etag = self.get_etag_from_object(fresh_details)
                                if new_etag:
                                    etag = new_etag
                                    await asyncio.sleep(1)
                                    continue
                            logging.error(
                                f"    Kunne ikke hente ny ETag for {task_title}"
                            )
                            return False, ""

                        elif response.status == 400:
                            logging.error(
                                f"    400 Bad Request for {operation_type} på {task_title}"
                            )
                            logging.error(f"    Response: {response_text}")
                            logging.error(f"    ETag used: {etag}")

                            # Detaljert logging for checklist
                            if (
                                operation_type == "checklist"
                                and "checklist" in patch_data
                            ):
                                checklist = patch_data["checklist"]
                                logging.error(
                                    f"    Checklist keys: {list(checklist.keys())[:3]}..."
                                )  # Første 3 keys
                                first_key = next(iter(checklist), None)
                                if first_key:
                                    item = checklist[first_key]
                                    logging.error(f"    First item structure: {item}")
                                    logging.error(
                                        f"    First item title type: {type(item.get('title'))}"
                                    )
                                    logging.error(
                                        f"    First item isChecked type: {type(item.get('isChecked'))}"
                                    )

                            return False, ""

                        else:
                            logging.error(
                                f"    {operation_type} PATCH feilet for {task_title}: {response.status} - {response_text}"
                            )
                            return False, ""

            except aiohttp.ClientError as e:
                if attempt < max_retries - 1:
                    wait_time = (2**attempt) + 1
                    logging.warning(
                        f"    Connection error, retry {attempt+1}/{max_retries} om {wait_time}s: {str(e)}"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logging.error(
                        f"    Max retries nådd for {operation_type} på {task_title}: {str(e)}"
                    )
                    return False, ""
            except Exception as e:
                logging.error(
                    f"    Uventet feil ved {operation_type} PATCH for {task_title}: {str(e)}"
                )
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
            if (
                hasattr(original_checklist, "additional_data")
                and original_checklist.additional_data
            ):
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
                    elif hasattr(item, "title"):
                        title = str(
                            self.extract_primitive_value(getattr(item, "title", ""))
                        ).strip()
                        is_checked = bool(
                            getattr(item, "is_checked", False)
                            or getattr(item, "isChecked", False)
                        )
                        original_order_hint = str(
                            getattr(item, "order_hint", "")
                            or getattr(item, "orderHint", "")
                        )

                    if not title:
                        logging.warning(f"Hopper over item uten title: {old_key}")
                        continue

                    if len(title) > 255:
                        title = title[:255]
                        logging.warning(f"Kortet ned lang title: {title[:50]}...")

                    items_with_order.append(
                        {
                            "title": title,
                            "isChecked": is_checked,
                            "orderHint": original_order_hint,  # Kun for sorting
                            "original_key": old_key,
                        }
                    )

                except Exception as item_error:
                    logging.warning(
                        f"Kunne ikke prosessere checklist item {old_key}: {str(item_error)}"
                    )
                    continue

            # ASCII-basert sortering basert på original orderHint (reverse for UI match)
            def pure_ascii_sort_key(item):
                """Garanterer ren ASCII-basert sammenligning."""
                order_hint = item["orderHint"] or ""
                # Convert to ASCII bytes for guaranteed ASCII ordering
                ascii_bytes = order_hint.encode("ascii", errors="replace")
                return ascii_bytes

            items_with_order.sort(key=pure_ascii_sort_key, reverse=True)

            logging.info(
                "Checklist items sortert basert på original orderHint (Microsoft genererer nye):"
            )
            for i, item in enumerate(items_with_order, 1):
                logging.info(
                    f"  {i}. '{item['title']}' (original orderHint: {item['orderHint']})"
                )

            # Bygg result dict UTEN orderHint - la Microsoft generere
            result = {}
            for item in items_with_order:
                new_key = str(uuid.uuid4())

                item_data = {
                    "@odata.type": "microsoft.graph.plannerChecklistItem",
                    "title": item["title"],
                    "isChecked": item["isChecked"],
                    # INGEN orderHint - Microsoft genererer automatisk
                }

                result[new_key] = item_data

            logging.info(
                f"Kopiert {len(result)} checklist items sortert korrekt (Microsoft genererer orderHints)"
            )
            return result

        except Exception as e:
            logging.error(f"Feil ved kopiering av checklist: {str(e)}")
            return {}

    async def send_patch_http(
        self, task_id: str, patch_data: dict, etag: str, task_title: str
    ) -> bool:
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

                url = (
                    f"https://graph.microsoft.com/v1.0/planner/tasks/{task_id}/details"
                )
                headers = {
                    "Authorization": f"Bearer {self._access_token}",
                    "Content-Type": "application/json",
                    "If-Match": etag,
                }

                async with aiohttp.ClientSession() as session:
                    async with session.patch(
                        url, headers=headers, json=patch_data
                    ) as response:
                        response_text = await response.text()

                        if response.status >= 200 and response.status < 300:
                            logging.info(f"    PATCH vellykket for task: {task_title}")
                            return True

                        elif response.status == 503:
                            wait_time = (2**attempt) + 1
                            logging.warning(
                                f"    Service unavailable, retry {attempt+1}/{max_retries} om {wait_time}s"
                            )
                            await asyncio.sleep(wait_time)
                            continue

                        elif response.status == 412:
                            # ETag mismatch - hent ny ETag via SDK
                            logging.warning(
                                f"    ETag mismatch for {task_title}, henter ny ETag"
                            )
                            if attempt < max_retries - 1:
                                fresh_details = await self.graph_client.planner.tasks.by_planner_task_id(
                                    task_id
                                ).details.get()
                                new_etag = self.get_etag_from_object(fresh_details)
                                if new_etag:
                                    etag = new_etag
                                    await asyncio.sleep(1)
                                    continue
                            logging.error(
                                f"    Kunne ikke hente ny ETag for {task_title}"
                            )
                            return False

                        elif response.status == 400:
                            logging.error(f"    400 Bad Request for {task_title}")
                            logging.error(f"    Response: {response_text}")
                            return False

                        else:
                            logging.error(
                                f"    PATCH feilet for {task_title}: {response.status} - {response_text}"
                            )
                            return False

            except aiohttp.ClientError as e:
                if attempt < max_retries - 1:
                    wait_time = (2**attempt) + 1
                    logging.warning(
                        f"    Connection error, retry {attempt+1}/{max_retries} om {wait_time}s: {str(e)}"
                    )
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
            if hasattr(value, "__str__") and not hasattr(value, "__dict__"):
                return str(value)

            # Hvis objektet har en verdi som kan ekstraheres
            if hasattr(value, "value"):
                return self.extract_primitive_value(value.value)

            # Hvis det er et objekt med additional_data
            if hasattr(value, "additional_data") and value.additional_data:
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
                "odata_etag",
                "@odata.etag",
                "etag",
                "_etag",
                "e_tag",
            ]

            for attr in possible_etag_attrs:
                if hasattr(obj, attr):
                    etag = getattr(obj, attr)
                    if etag:
                        return etag

            # Sjekk additional_data hvis det finnes
            if hasattr(obj, "additional_data") and obj.additional_data:
                for key in ["@odata.etag", "odata.etag", "etag"]:
                    if key in obj.additional_data:
                        etag = obj.additional_data[key]
                        if etag:
                            return etag

            # Sjekk om objektet har en __dict__ med ETag info
            if hasattr(obj, "__dict__"):
                for key, value in obj.__dict__.items():
                    if "etag" in key.lower() and value:
                        return value

            return ""

        except Exception as e:
            logging.error(f"Feil ved henting av ETag: {str(e)}")
            return ""

    async def add_planner_tab_to_team(
        self, team_id: str, plan_id: str, plan_title: str, channel_id: str = None
    ) -> bool:
        """
        Legger til Planner tab med manuell HTTP request.
        """
        try:
            # If no channel_id provided, get the General channel
            if not channel_id:
                logging.info(f"Henter General kanal ID for team {team_id}")
                channel_id = await self.get_general_channel_id(team_id)
                if not channel_id:
                    logging.error("Kunne ikke finne General kanal")
                    return False
                logging.info(f"Bruker General kanal ID: {channel_id}")

            tenant_id = os.getenv("AZURE_TENANT_ID")

            if not tenant_id:
                logging.error("Kunne ikke hente tenant ID")
                return False

            # Get access token from your graph client
            # This depends on how your graph client is set up

            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/tabs"

            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            request_body = {
                "displayName": plan_title,
                "teamsApp@odata.bind": "https://graph.microsoft.com/v1.0/appCatalogs/teamsApps/com.microsoft.teamspace.tab.planner",
                "configuration": {
                    "entityId": plan_id,
                    "contentUrl": f"https://tasks.office.com/{tenant_id}/Home/PlannerFrame?page=7&planId={plan_id}&mkt=nb-NO",
                    "websiteUrl": f"https://tasks.office.com/{tenant_id}/Home/PlanViews/{plan_id}?mkt=nb-NO",
                },
            }

            logging.info(f"Legger til Planner tab i kanal {channel_id} med URL: {url}")
            logging.info(f"Request body: {request_body}")

            # Try using Graph SDK first, fallback to HTTP if needed
            try:
                # Import the TeamsTab model
                from msgraph.generated.models.teams_tab import TeamsTab
                from msgraph.generated.models.teams_app_installation import (
                    TeamsAppInstallation,
                )

                # Create the tab using Graph SDK
                tab = TeamsTab()
                tab.display_name = plan_title
                tab.teams_app = TeamsAppInstallation()
                tab.teams_app.id = "com.microsoft.teamspace.tab.planner"
                tab.configuration = {
                    "entityId": plan_id,
                    "contentUrl": f"https://tasks.office.com/{tenant_id}/Home/PlannerFrame?page=7&planId={plan_id}&mkt=nb-NO",
                    "websiteUrl": f"https://tasks.office.com/{tenant_id}/Home/PlanViews/{plan_id}?mkt=nb-NO",
                }

                # Add the tab using Graph SDK
                result = (
                    await self.graph_client.teams.by_team_id(team_id)
                    .channels.by_channel_id(channel_id)
                    .tabs.post(tab)
                )
                logging.info(
                    f"Planner tab '{result.display_name}' lagt til med ID: {result.id}"
                )
                return True

            except Exception as sdk_error:
                logging.warning(f"Graph SDK failed, trying HTTP: {str(sdk_error)}")

                # Fallback to HTTP request
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url, headers=headers, json=request_body
                    ) as response:
                        response_text = await response.text()
                        logging.info(f"Response status: {response.status}")
                        logging.info(f"Response text: {response_text}")

                        if response.status == 201:
                            result = await response.json()
                            logging.info(
                                f"Planner tab '{result['displayName']}' lagt til med ID: {result['id']}"
                            )
                            return True
                        else:
                            logging.error(f"HTTP {response.status}: {response_text}")
                            return False

        except Exception as e:
            logging.error(f"Feil ved tillegging av Planner tab: {str(e)}")
            return False

    async def add_sharepoint_tab_to_channel(
        self,
        team_id: str,
        channel_id: str,
        display_name: str,
        sharepoint_url: str,
        wait_for_provisioning: bool = True,
        max_wait: int = 60,
    ) -> bool:
        """
        Legger til SharePoint document library tab som peker til Files i General kanal.

        Args:
            team_id: Team ID
            channel_id: Kanal ID
            display_name: Navn på tab
            sharepoint_url: SharePoint document library URL
            wait_for_provisioning: Om vi skal vente på provisjonering (default: True)
            max_wait: Maksimal ventetid for provisjonering i sekunder (default: 60)
        """
        try:
            if not self._access_token:
                self.get_access_token()
                if not self._access_token:
                    logging.error("Kunne ikke hente access token")
                    return False

            # Vent på at kanalen er provisjonert hvis ønsket
            if wait_for_provisioning:
                is_ready = await self.wait_for_channel_provisioning(
                    team_id, channel_id, max_wait=max_wait
                )
                if not is_ready:
                    logging.error("Kanal ble ikke provisjonert i tide")
                    return False

            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/tabs"

            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            # Use the SharePoint FILES/DOCUMENTS app, not pages
            request_body = {
                "displayName": display_name,
                "teamsApp@odata.bind": "https://graph.microsoft.com/v1.0/appCatalogs/teamsApps/com.microsoft.teamspace.tab.files.sharepoint",
                "configuration": {
                    "entityId": "",
                    "contentUrl": sharepoint_url,
                    "websiteUrl": sharepoint_url,
                    "removeUrl": None,
                },
            }

            logging.info(
                f"📎 Legger til SharePoint document library tab i kanal {channel_id}"
            )
            logging.info(f"📁 SharePoint URL: {sharepoint_url}")

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, headers=headers, json=request_body
                ) as response:
                    response_text = await response.text()
                    logging.info(f"SharePoint tab response status: {response.status}")

                    if response.status == 201:
                        result = await response.json()
                        logging.info(
                            f"✅ SharePoint document library tab '{result['displayName']}' lagt til med ID: {result['id']}"
                        )
                        return True
                    else:
                        logging.error(f"❌ HTTP {response.status}: {response_text}")
                        return False

        except Exception as e:
            logging.error(f"❌ Feil ved tillegging av SharePoint tab: {str(e)}")
            return False

    async def add_sharepoint_tab_with_retry(
        self,
        team_id: str,
        channel_id: str,
        display_name: str,
        sharepoint_url: str,
        max_retries: int = 3,
        retry_delay: int = 5,
    ) -> bool:
        """
        Legger til SharePoint document library tab med retry-logikk.

        Args:
            team_id: Team ID
            channel_id: Kanal ID
            display_name: Navn på tab
            sharepoint_url: SharePoint document library URL
            max_retries: Maksimalt antall forsøk (default: 3)
            retry_delay: Forsinkelse mellom forsøk i sekunder (default: 5)
        """
        try:
            if not self._access_token:
                self.get_access_token()
                if not self._access_token:
                    logging.error("Kunne ikke hente access token")
                    return False

            # Vent på provisjonering først
            is_ready = await self.wait_for_channel_provisioning(team_id, channel_id)
            if not is_ready:
                logging.error("Kanal ble ikke provisjonert i tide")
                return False

            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/tabs"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            # Use the SharePoint Pages app which is more flexible with URL formats
            # This app works with folders and displays natively in Teams
            app_ids = [
                "2a527703-1f6f-4559-a332-d8a7d288cd88",  # SharePoint Pages app (flexible, works with folders)
            ]

            for attempt in range(max_retries):
                for app_id in app_ids:
                    try:
                        logging.info(
                            f"🔄 Forsøk {attempt + 1}/{max_retries} med app ID: {app_id}"
                        )

                        # SharePoint document library configuration for native Teams view
                        # This ensures files open in Teams, not in a web browser

                        # Log the URL being used for debugging
                        logging.info(f"📋 SharePoint URL format: {sharepoint_url}")
                        logging.info(
                            f"📋 URL starts with: {sharepoint_url[:100] if len(sharepoint_url) > 100 else sharepoint_url}"
                        )

                        request_body = {
                            "displayName": display_name,
                            "teamsApp@odata.bind": f"https://graph.microsoft.com/v1.0/appCatalogs/teamsApps/{app_id}",
                            "configuration": {
                                "entityId": "",
                                "contentUrl": sharepoint_url,
                                "removeUrl": None,
                            },
                        }

                        logging.info(f"📋 Request body: {request_body}")

                        async with aiohttp.ClientSession() as session:
                            async with session.post(
                                url, headers=headers, json=request_body
                            ) as response:
                                response_text = await response.text()

                                if response.status == 201:
                                    result = await response.json()
                                    logging.info(
                                        f"✅ SharePoint Files tab '{result['displayName']}' lagt til med ID: {result['id']}"
                                    )
                                    return True
                                elif response.status == 404:
                                    logging.warning(
                                        f"⚠️ 404 med app ID: {app_id}, prøver neste..."
                                    )
                                    continue
                                else:
                                    logging.warning(
                                        f"⚠️ HTTP {response.status} med app ID: {app_id}: {response_text}"
                                    )

                    except Exception as e:
                        logging.warning(f"⚠️ Feil med app ID {app_id}: {str(e)}")
                        continue

                # Vent før neste forsøk
                if attempt < max_retries - 1:
                    logging.info(
                        f"⏳ Venter {retry_delay} sekunder før neste forsøk..."
                    )
                    await asyncio.sleep(retry_delay)

            logging.error(f"❌ Kunne ikke legge til tab etter {max_retries} forsøk")
            return False

        except Exception as e:
            logging.error(f"❌ Feil ved tillegging av SharePoint tab: {str(e)}")
            return False

    async def wait_for_channel_provisioning(
        self, team_id: str, channel_id: str, max_wait: int = 60, check_interval: int = 5
    ) -> bool:
        """
        Venter på at en kanal skal bli fullstendig provisjonert.

        Args:
            team_id: Team ID
            channel_id: Kanal ID
            max_wait: Maksimal ventetid i sekunder (default: 60)
            check_interval: Tid mellom sjekker i sekunder (default: 5)

        Returns:
            True hvis kanal er klar, False ved timeout
        """
        if not self._access_token:
            self.get_access_token()
            if not self._access_token:
                logging.error("Kunne ikke hente access token")
                return False

        url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}"
        headers = {"Authorization": f"Bearer {self._access_token}"}

        elapsed = 0
        logging.info(f"⏳ Venter på at kanal {channel_id} skal bli provisjonert...")

        async with aiohttp.ClientSession() as session:
            while elapsed < max_wait:
                try:
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            logging.info(f"✅ Kanal er klar etter {elapsed} sekunder")
                            return True
                        elif response.status == 404:
                            logging.debug(
                                f"   Kanal ikke klar ennå... ({elapsed}s / {max_wait}s)"
                            )
                except Exception as e:
                    logging.debug(f"   Feil ved sjekk: {str(e)}")

                await asyncio.sleep(check_interval)
                elapsed += check_interval

            logging.warning(f"⚠️ Timeout etter {max_wait} sekunder")
            return False

    async def send_channel_info_notification(
        self,
        team_id: str,
        team_name: str,
        general_channel_id: str,
        admin_channel_id: str,
        montasje_channel_id: str,
        planner_id: str,
    ) -> bool:
        """
        Sender en HTTP POST request med informasjon om opprettede kanaler.
        Denne funksjonen feiler aldri - den logger bare eventuelle problemer.

        Args:
            team_id: Team ID
            team_name: Team navn
            general_channel_id: General kanal ID
            admin_channel_id: Administrasjon kanal ID
            montasje_channel_id: Montasje kanal ID
            planner_id: Planner ID

        Returns:
            True hvis vellykket, False ved feil (men feiler aldri prosessen)
        """
        try:
            # Get the webhook URL from environment variables
            webhook_url = os.getenv("CHANNEL_INFO_WEBHOOK_URL")

            if not webhook_url:
                logging.info(
                    "ℹ️ CHANNEL_INFO_WEBHOOK_URL ikke satt, hopper over webhook notifikasjon"
                )
                return False

            payload = {
                "teamId": team_id,
                "teamName": team_name,
                "generalChannelId": general_channel_id,
                "adminChannelId": admin_channel_id,
                "montasjeChannelId": montasje_channel_id,
                "plannerId": planner_id,
            }

            logging.info(f"📤 Sender kanal informasjon til webhook: {webhook_url}")
            logging.info(f"📋 Payload: {payload}")

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),  # 10 second timeout
                ) as response:
                    response_text = await response.text()

                    if response.status in [200, 201, 204]:
                        logging.info(
                            f"✅ Kanal informasjon sendt til webhook (status: {response.status})"
                        )
                        return True
                    else:
                        logging.warning(
                            f"⚠️ Webhook returnerte status {response.status}: {response_text}"
                        )
                        return False

        except aiohttp.ClientConnectorError as e:
            logging.warning(f"⚠️ Kunne ikke koble til webhook URL: {str(e)}")
            return False
        except asyncio.TimeoutError:
            logging.warning(f"⚠️ Webhook request timeout etter 10 sekunder")
            return False
        except Exception as e:
            logging.warning(f"⚠️ Webhook notifikasjon feilet: {str(e)}")
            return False

    async def install_sharepoint_app_in_channel(
        self, team_id: str, channel_id: str
    ) -> bool:
        """
        Installerer SharePoint Files app i teamet (ikke i kanal).
        """
        try:
            if not self._access_token:
                self.get_access_token()
                if not self._access_token:
                    logging.error("Kunne ikke hente access token")
                    return False

            # First, install the app at the team level
            team_app_url = (
                f"https://graph.microsoft.com/v1.0/teams/{team_id}/installedApps"
            )

            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            # Install the SharePoint Files app at team level
            app_install_body = {
                "teamsApp@odata.bind": "https://graph.microsoft.com/v1.0/appCatalogs/teamsApps/2a527703-0f5f-4f71-9e98-8ee8678e0c79"
            }

            logging.info(f"Installerer SharePoint Files app i team {team_id}")

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    team_app_url, headers=headers, json=app_install_body
                ) as response:
                    response_text = await response.text()
                    logging.info(f"Team app install response status: {response.status}")
                    logging.info(f"Team app install response text: {response_text}")

                    if response.status == 201:
                        logging.info("SharePoint Files app installert på team-nivå")
                        return True
                    else:
                        # App might already be installed, check if it exists
                        if (
                            response.status == 400
                            and "already installed" in response_text.lower()
                        ):
                            logging.info(
                                "SharePoint Files app allerede installert på team-nivå"
                            )
                            return True
                        else:
                            logging.error(f"HTTP {response.status}: {response_text}")
                            return False

        except Exception as e:
            logging.error(f"Feil ved installasjon av SharePoint app: {str(e)}")
            return False

    async def get_general_files_sharepoint_url(
        self, team_id: str, general_channel_id: str
    ) -> Optional[str]:
        """
        Henter SharePoint URL for General kanalens Files folder.
        URL format: https://tenant.sharepoint.com/sites/TeamSite/Delte dokumenter/General
        """
        try:
            if not self._access_token:
                self.get_access_token()
                if not self._access_token:
                    logging.error("Kunne ikke hente access token")
                    return None

            # Use filesFolder endpoint to get the webUrl which includes /General path
            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{general_channel_id}/filesFolder"

            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        folder_data = await response.json()
                        web_url = folder_data.get("webUrl", "")

                        if web_url:
                            logging.info(
                                f"Fant SharePoint URL for General kanal: {web_url}"
                            )
                            # This URL should be in format:
                            # https://tenant.sharepoint.com/sites/TeamSite/Delte dokumenter/General
                            return web_url
                        else:
                            logging.error("Fant ikke webUrl i filesFolder response")
                            return None
                    else:
                        error_text = await response.text()
                        logging.error(f"HTTP {response.status}: {error_text}")

                        # Fallback: try to get from tabs
                        logging.info("Prøver fallback til tabs...")
                        return await self._get_sharepoint_url_from_tabs(
                            team_id, general_channel_id
                        )

        except Exception as e:
            logging.error(f"Feil ved henting av SharePoint URL: {str(e)}")
            # Fallback: try to get from tabs
            logging.info("Prøver fallback til tabs...")
            return await self._get_sharepoint_url_from_tabs(team_id, general_channel_id)

    async def _get_sharepoint_url_from_tabs(
        self, team_id: str, general_channel_id: str
    ) -> Optional[str]:
        """
        Fallback method to get SharePoint URL from tabs.
        """
        try:
            # Get all tabs in the General channel
            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{general_channel_id}/tabs"

            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        tabs_data = await response.json()
                        tabs = tabs_data.get("value", [])

                        # Look for the Files tab
                        for tab in tabs:
                            if tab.get("displayName", "").lower() in ["files", "filer"]:
                                configuration = tab.get("configuration", {})
                                content_url = configuration.get("contentUrl", "")
                                website_url = configuration.get("websiteUrl", "")

                                # Use contentUrl if available, otherwise websiteUrl
                                sharepoint_url = content_url or website_url
                                if sharepoint_url:
                                    logging.info(
                                        f"Fant SharePoint URL fra Files tab (fallback): {sharepoint_url}"
                                    )
                                    return sharepoint_url

                        logging.warning("Fant ikke Files tab i General kanal")
                        return None
                    else:
                        error_text = await response.text()
                        logging.error(f"HTTP {response.status}: {error_text}")
                        return None

        except Exception as e:
            logging.error(f"Feil ved fallback henting av SharePoint URL: {str(e)}")
            return None

    async def get_team_info(self, team_id: str) -> Optional[Dict]:
        """
        Henter team informasjon for å få displayName.
        """
        try:
            if not self._access_token:
                self.get_access_token()
                if not self._access_token:
                    return None

            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}"

            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_text = await response.text()
                        logging.error(f"HTTP {response.status}: {error_text}")
                        return None

        except Exception as e:
            logging.error(f"Feil ved henting av team informasjon: {str(e)}")
            return None

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
                    # General kanal har spesielle egenskaper - sjekk både norsk og engelsk navn
                    if (
                        channel.display_name == "General"
                        or channel.display_name == "Generelt"
                        or (
                            channel.membership_type == "standard"
                            and channel.display_name.lower() in ["general", "generelt"]
                        )
                    ):
                        logging.info(
                            f"Fant General kanal: {channel.display_name} med ID: {channel.id}"
                        )
                        return channel.id

            logging.warning(f"General kanal ikke funnet for team {team_id}")
            return ""

        except Exception as e:
            logging.error(f"Feil ved henting av General kanal: {str(e)}")
            return ""

    async def get_all_team_members_with_roles(self, team_id: str) -> list[dict]:
        """
        Henter alle medlemmer i et team med deres roller.

        Args:
            team_id: ID til teamet

        Returns:
            list[dict]: Liste med dicts {'email': str, 'is_owner': bool}
        """
        try:
            if not self._access_token:
                self.get_access_token()
                if not self._access_token:
                    logging.error("Kunne ikke hente access token")
                    return []

            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/members"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        members = result.get("value", [])

                        # Ekstraher e-postadresser og roller
                        member_list = []
                        for member in members:
                            email = member.get("email") or member.get(
                                "userPrincipalName"
                            )
                            if email:
                                roles = member.get("roles", [])
                                is_owner = "owner" in roles
                                member_list.append(
                                    {"email": email, "is_owner": is_owner}
                                )
                                role_text = "owner" if is_owner else "member"
                                logging.debug(f"Fant medlem: {email} ({role_text})")

                        logging.info(
                            f"Hentet {len(member_list)} medlemmer med roller fra team {team_id}"
                        )
                        return member_list
                    else:
                        error_text = await response.text()
                        logging.error(
                            f"Kunne ikke hente teammedlemmer: HTTP {response.status} - {error_text}"
                        )
                        return []

        except Exception as e:
            logging.error(f"Feil ved henting av teammedlemmer: {str(e)}")
            return []

    async def get_all_team_members(self, team_id: str) -> list[str]:
        """
        Henter alle medlemmer i et team (kun e-postadresser).

        Args:
            team_id: ID til teamet

        Returns:
            list[str]: Liste med e-postadresser til alle medlemmer
        """
        try:
            if not self._access_token:
                self.get_access_token()
                if not self._access_token:
                    logging.error("Kunne ikke hente access token")
                    return []

            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/members"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        members = result.get("value", [])

                        # Ekstraher e-postadresser og roller
                        member_list = []
                        for member in members:
                            email = member.get("email") or member.get(
                                "userPrincipalName"
                            )
                            if email:
                                roles = member.get("roles", [])
                                is_owner = "owner" in roles
                                member_list.append(
                                    {"email": email, "is_owner": is_owner}
                                )
                                role_text = "owner" if is_owner else "member"
                                logging.debug(f"Fant medlem: {email} ({role_text})")

                        logging.info(
                            f"Hentet {len(member_list)} medlemmer fra team {team_id}"
                        )
                        # Returner kun e-postadresser for bakoverkompatibilitet
                        return [m["email"] for m in member_list]
                    else:
                        error_text = await response.text()
                        logging.error(
                            f"Kunne ikke hente teammedlemmer: HTTP {response.status} - {error_text}"
                        )
                        return []

        except Exception as e:
            logging.error(f"Feil ved henting av teammedlemmer: {str(e)}")
            return []

    async def determine_company_owners(self, team_id: str) -> tuple[Optional[str], list[str]]:
        """
        Bestemmer hvilke brukere som skal være owners basert på hvilken company member som er i teamet.

        Logikk:
        - Hvis henrik@straye.no er medlem: bruk Tak-listen
        - Hvis christer@straye.no er medlem: bruk Hybridbygg-listen
        - Hvis ali@straye.no er medlem: bruk Stålbygg-listen
        - Hvis sven@straye.no er medlem: bruk Industri-listen
        - Hvis ingen er funnet: bruk tom liste (bare admin_channel_owners vil bli lagt til)

        Args:
            team_id: ID til teamet

        Returns:
            tuple[Optional[str], list[str]]: (company_name, list_of_owner_emails)
        """
        try:
            # Hent alle medlemmer i teamet
            all_members = await self.get_all_team_members_with_roles(team_id)
            member_emails = [m["email"].lower() for m in all_members]

            logging.info(f"Sjekker company-tilhørighet blant {len(member_emails)} medlemmer")

            # Sjekk hver key member i prioritert rekkefølge
            for key_member, (company_name, owner_list) in self.company_owner_mapping.items():
                if key_member.lower() in member_emails:
                    logging.info(
                        f"✅ Fant {key_member} i teamet - bruker {company_name} owner-liste med {len(owner_list)} medlemmer"
                    )
                    return company_name, owner_list

            logging.info(
                "ℹ️ Ingen company key members funnet i teamet - bruker kun admin_channel_owners"
            )
            return None, []

        except Exception as e:
            logging.error(f"Feil ved bestemmelse av company owners: {str(e)}")
            return None, []

    async def ensure_user_in_team(
        self, team_id: str, user_email: str, as_owner: bool = False
    ) -> bool:
        """
        Sikrer at en bruker er medlem av teamet. Legger til hvis ikke.

        Args:
            team_id: ID til teamet
            user_email: E-postadresse til brukeren
            as_owner: Om brukeren skal være owner (True) eller bare medlem (False)

        Returns:
            bool: True hvis brukeren er medlem (eller ble lagt til), False ved feil
        """
        try:
            if not self._access_token:
                self.get_access_token()
                if not self._access_token:
                    logging.error("Kunne ikke hente access token")
                    return False

            # Sjekk om brukeren allerede er medlem
            check_url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/members"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                # Sjekk eksisterende medlemmer
                existing_member_id = None
                is_already_owner = False

                async with session.get(check_url, headers=headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        members = result.get("value", [])
                        for member in members:
                            member_email = member.get("email") or member.get(
                                "userPrincipalName"
                            )
                            if (
                                member_email
                                and member_email.lower() == user_email.lower()
                            ):
                                existing_member_id = member.get("id")
                                member_roles = member.get("roles", [])
                                is_already_owner = "owner" in member_roles

                                if is_already_owner:
                                    logging.info(
                                        f"Bruker {user_email} er allerede owner av teamet"
                                    )
                                    return True
                                elif not as_owner:
                                    logging.info(
                                        f"Bruker {user_email} er allerede medlem av teamet"
                                    )
                                    return True
                                else:
                                    logging.info(
                                        f"Bruker {user_email} er medlem, men må oppgraderes til owner"
                                    )
                                    # Fall through to upgrade logic below
                                    break

                # Hvis bruker er medlem men trenger å være owner, må vi oppdatere rollen
                if existing_member_id and as_owner and not is_already_owner:
                    logging.info(f"Oppgraderer {user_email} til owner i teamet")
                    update_url = f"{check_url}/{existing_member_id}"
                    update_body = {
                        "@odata.type": "#microsoft.graph.aadUserConversationMember",
                        "roles": ["owner"],
                    }

                    async with session.patch(
                        update_url, headers=headers, json=update_body
                    ) as response:
                        if response.status in [200, 204]:
                            logging.info(f"Bruker {user_email} oppgradert til owner")
                            return True
                        else:
                            error_text = await response.text()
                            logging.error(
                                f"Kunne ikke oppgradere {user_email} til owner: HTTP {response.status} - {error_text}"
                            )
                            return False

                # Legg til bruker hvis ikke medlem
                role = "owner" if as_owner else "member"
                logging.info(f"Legger til {user_email} som {role} av teamet")
                add_body = {
                    "@odata.type": "#microsoft.graph.aadUserConversationMember",
                    "roles": ["owner"] if as_owner else ["member"],
                    "user@odata.bind": f"https://graph.microsoft.com/v1.0/users('{user_email}')",
                }

                async with session.post(
                    check_url, headers=headers, json=add_body
                ) as response:
                    if response.status in [201, 204]:
                        logging.info(f"Bruker {user_email} lagt til teamet")
                        return True
                    else:
                        error_text = await response.text()
                        logging.error(
                            f"Kunne ikke legge til {user_email} til teamet: HTTP {response.status} - {error_text}"
                        )
                        return False

        except Exception as e:
            logging.error(f"Feil ved å sikre bruker i team: {str(e)}")
            return False

    async def add_member_to_channel(
        self, team_id: str, channel_id: str, user_email: str, is_owner: bool = False
    ) -> bool:
        """
        Legger til et medlem i en kanal.

        Args:
            team_id: ID til teamet
            channel_id: ID til kanalen
            user_email: E-postadresse til brukeren
            is_owner: Om brukeren skal være owner (True) eller bare medlem (False)

        Returns:
            bool: True hvis vellykket, False ved feil
        """
        try:
            if not self._access_token:
                self.get_access_token()
                if not self._access_token:
                    logging.error("Kunne ikke hente access token")
                    return False

            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/members"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            roles = ["owner"] if is_owner else ["member"]
            request_body = {
                "@odata.type": "#microsoft.graph.aadUserConversationMember",
                "roles": roles,
                "user@odata.bind": f"https://graph.microsoft.com/v1.0/users('{user_email}')",
            }

            logging.info(
                f"Legger til {user_email} som {'owner' if is_owner else 'member'} i kanal {channel_id}"
            )

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, headers=headers, json=request_body
                ) as response:
                    if response.status in [201, 204]:
                        logging.info(f"Bruker {user_email} lagt til kanal")
                        return True
                    else:
                        error_text = await response.text()
                        logging.error(
                            f"Kunne ikke legge til {user_email} til kanal: HTTP {response.status} - {error_text}"
                        )
                        return False

        except Exception as e:
            logging.error(f"Feil ved å legge til medlem i kanal: {str(e)}")
            return False

    async def get_or_create_channel(
        self,
        team_id: str,
        channel_name: str,
        description: str,
        is_private: bool = False,
        membership_type: Optional[str] = None,
    ) -> Optional[tuple[str, bool]]:
        """
        Sjekker om en kanal eksisterer, og oppretter den hvis den ikke finnes.

        Args:
            team_id: ID til teamet
            channel_name: Navn på kanalen
            description: Beskrivelse av kanalen
            is_private: Om kanalen skal være privat (True) eller standard (False)
            membership_type: Membership type: "standard", "private", eller "shared"

        Returns:
            Optional[tuple[str, bool]]: Tuple med (channel_id, was_created) hvis vellykket, None ved feil
            - channel_id: ID til kanalen
            - was_created: True hvis kanalen ble opprettet, False hvis den allerede eksisterte
        """
        try:
            # Først, sjekk om kanalen allerede eksisterer
            if not self._access_token:
                self.get_access_token()
                if not self._access_token:
                    logging.error("Kunne ikke hente access token")
                    return None

            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        channels = result.get("value", [])

                        # Log all channels for debugging
                        logging.info(f"Fant {len(channels)} kanaler i teamet")
                        for ch in channels:
                            logging.info(
                                f"  - '{ch.get('displayName')}' (type: {ch.get('membershipType')}, id: {ch.get('id')})"
                            )

                        # Look for matching channel name
                        for channel in channels:
                            channel_display_name = channel.get("displayName", "")
                            # Exact match
                            if channel_display_name == channel_name:
                                channel_id = channel.get("id")
                                channel_membership_type = channel.get("membershipType")

                                # Verify the channel is actually accessible
                                verify_url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}"
                                async with session.get(
                                    verify_url, headers=headers
                                ) as verify_response:
                                    if verify_response.status == 200:
                                        logging.info(
                                            f"✅ Fant og verifiserte kanal '{channel_name}' (ID: {channel_id}, type: {channel_membership_type})"
                                        )
                                        return (
                                            channel_id,
                                            False,
                                        )  # Eksisterte allerede og er tilgjengelig
                                    else:
                                        logging.warning(
                                            f"⚠️ Kanal '{channel_name}' finnes i listen men er ikke tilgjengelig (status: {verify_response.status}). Behandler som ikke-eksisterende."
                                        )

            # Kanalen eksisterer ikke, opprett den
            logging.info(f"🆕 Kanal '{channel_name}' eksisterer ikke, oppretter ny...")
            new_channel_id = await self.create_channel(
                team_id, channel_name, description, is_private, membership_type
            )
            if new_channel_id:
                return (new_channel_id, True)  # Ble akkurat opprettet
            return None

        except Exception as e:
            logging.error(
                f"Feil ved get_or_create_channel for '{channel_name}': {str(e)}"
            )
            return None

    async def _wait_for_channel_creation(
        self, team_id: str, channel_name: str, max_wait_seconds: int = 60
    ) -> Optional[str]:
        """
        Venter på at en kanal skal bli opprettet (for async opprettelse med HTTP 202).

        Args:
            team_id: ID til teamet
            channel_name: Navnet på kanalen vi venter på
            max_wait_seconds: Maks antall sekunder å vente

        Returns:
            Optional[str]: Channel ID hvis funnet, None hvis timeout
        """
        if not self._access_token:
            self.get_access_token()
            if not self._access_token:
                logging.error("Kunne ikke hente access token")
                return None

        url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

        start_time = asyncio.get_event_loop().time()
        check_interval = 3  # Sjekk hvert 3. sekund

        async with aiohttp.ClientSession() as session:
            while (asyncio.get_event_loop().time() - start_time) < max_wait_seconds:
                try:
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            result = await response.json()
                            channels = result.get("value", [])
                            # Søk etter kanalen med riktig navn
                            for channel in channels:
                                if channel.get("displayName") == channel_name:
                                    channel_id = channel.get("id")
                                    logging.info(
                                        f"Kanal '{channel_name}' funnet med ID: {channel_id}"
                                    )
                                    return channel_id

                    # Ikke funnet ennå, vent og prøv igjen
                    logging.info(
                        f"Kanal '{channel_name}' ikke funnet ennå, venter {check_interval} sekunder..."
                    )
                    await asyncio.sleep(check_interval)

                except Exception as e:
                    logging.warning(f"Feil ved polling av kanaler: {str(e)}")
                    await asyncio.sleep(check_interval)

        # Timeout
        return None

    async def create_channel(
        self,
        team_id: str,
        channel_name: str,
        description: str,
        is_private: bool = False,
        membership_type: Optional[str] = None,
    ) -> Optional[str]:
        """
        Oppretter en ny kanal i et team.

        Args:
            team_id: ID til teamet
            channel_name: Navn på kanalen
            description: Beskrivelse av kanalen
            is_private: Om kanalen skal være privat (True) eller standard (False) - legacy parameter
            membership_type: Membership type: "standard", "private", eller "shared". Overskriver is_private.

        Returns:
            Optional[str]: Channel ID hvis vellykket, None ved feil
        """
        try:
            if not self._access_token:
                self.get_access_token()
                if not self._access_token:
                    logging.error("Kunne ikke hente access token")
                    return None

            # Determine membership type
            if membership_type is None:
                membership_type = "private" if is_private else "standard"

            # For private og shared kanaler, sørg for at nødvendige brukere er medlemmer
            if membership_type in ["private", "shared"]:
                logging.info(
                    f"Sikrer at eierne er medlemmer av teamet for {membership_type} kanal"
                )
                for email in self.admin_channel_owners:
                    # For shared channels, users must be team owners (not just members)
                    as_owner = membership_type == "shared"
                    await self.ensure_user_in_team(team_id, email, as_owner=as_owner)
                # Vent litt for at medlemskap/owner-rolle skal bli aktivert
                # Shared channels trenger lengre tid for at owner-rolle skal propagere
                wait_time = 5 if membership_type == "shared" else 2
                logging.info(
                    f"Venter {wait_time} sekunder for at roller skal aktiveres..."
                )
                await asyncio.sleep(wait_time)

            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            request_body = {
                "displayName": channel_name,
                "description": description,
                "membershipType": membership_type,
                "moderationSettings": {
                    "replyRestrictions": "everyone"  # This enables threaded conversations (Tråder)
                },
            }

            # For private og shared kanaler, legg til members med owner rolle
            # Shared channels krever minst én owner ved opprettelse
            if membership_type in ["private", "shared"]:
                request_body["members"] = [
                    {
                        "@odata.type": "#microsoft.graph.aadUserConversationMember",
                        "roles": ["owner"],
                        "user@odata.bind": f"https://graph.microsoft.com/v1.0/users('{email}')",
                    }
                    for email in self.admin_channel_owners
                ]

            logging.info(f"Oppretter kanal: {channel_name} (type: {membership_type})")

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, headers=headers, json=request_body
                ) as response:
                    if response.status == 201:
                        result = await response.json()
                        channel_id = result.get("id")
                        logging.info(
                            f"Kanal '{channel_name}' opprettet med ID: {channel_id}"
                        )
                        return channel_id
                    elif response.status == 202:
                        # Shared channels returnerer 202 (Accepted) - kanalen blir opprettet asynkront
                        logging.info(
                            f"Kanal '{channel_name}' (shared) akseptert for opprettelse (HTTP 202). Venter på at den blir klar..."
                        )
                        # Vi må polle for å finne kanalen når den er opprettet
                        channel_id = await self._wait_for_channel_creation(
                            team_id, channel_name, max_wait_seconds=60
                        )
                        if channel_id:
                            logging.info(
                                f"Kanal '{channel_name}' ferdig opprettet med ID: {channel_id}"
                            )
                            return channel_id
                        else:
                            logging.error(
                                f"Timeout: Kanal '{channel_name}' ble ikke ferdig opprettet innen 60 sekunder"
                            )
                            return None
                    else:
                        error_text = await response.text()
                        logging.error(
                            f"Kunne ikke opprette kanal '{channel_name}': HTTP {response.status} - {error_text}"
                        )
                        return None

        except Exception as e:
            logging.error(f"Feil ved opprettelse av kanal '{channel_name}': {str(e)}")
            return None
