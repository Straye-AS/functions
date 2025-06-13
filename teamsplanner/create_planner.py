import logging
from typing import Dict, Optional, List
from msgraph import GraphServiceClient
from msgraph.generated.models.o_data_errors.o_data_error import ODataError

async def list_available_plans(client: GraphServiceClient) -> List[Dict]:
    """
    Lister alle tilgjengelige planner som brukeren har tilgang til.
    Dette kan brukes for å finne ID til mal-planneren.
    
    Args:
        client: Graph API klient
        
    Returns:
        List[Dict]: Liste med informasjon om tilgjengelige planner
    """
    try:
        plans = await client.planner.plans.get()
        return [{
            "id": plan.id,
            "title": plan.title,
            "createdBy": plan.created_by.user.display_name if plan.created_by else "Unknown",
            "createdDateTime": plan.created_date_time,
            "owner": plan.owner
        } for plan in plans.value]
    except Exception as e:
        logging.error(f"Feil ved henting av planner: {str(e)}")
        return []

async def create_planner_for_team(client: GraphServiceClient, team_id: str) -> Optional[Dict]:
    """
    Oppretter en ny planner for et team ved å kopiere en eksisterende mal.
    
    Args:
        client: Graph API klient
        team_id: ID til teamet som skal få en ny planner
        
    Returns:
        Optional[Dict]: Informasjon om den nye planneren hvis vellykket, None ved feil
    """
    try:

        # ID til mal-planneren som skal kopieres
        template_planner_id = "YOUR_TEMPLATE_PLANNER_ID"  # Erstatt med faktisk ID
        
        # Hent informasjon om mal-planneren
        template_planner = await client.planner.plans.by_plan_id(template_planner_id).get()
        
        # Opprett ny plan basert på malen
        new_plan = {
            "container": {
                "containerId": team_id,
                "type": "group"
            },
            "title": f"Team Planner - {template_planner.title}",
            "details": {
                "description": template_planner.details.description if template_planner.details else ""
            }
        }
        
        # Opprett den nye planneren
        created_plan = await client.planner.plans.post(body=new_plan)
        logging.info(f"Opprettet ny planner for team {team_id}: {created_plan.id}")
        
        # Kopier oppgaver fra mal-planneren
        template_tasks = await client.planner.plans.by_plan_id(template_planner_id).tasks.get()
        
        for task in template_tasks.value:
            new_task = {
                "planId": created_plan.id,
                "title": task.title,
                "details": {
                    "description": task.details.description if task.details else "",
                    "checklist": task.details.checklist if task.details and task.details.checklist else {},
                    "references": task.details.references if task.details and task.details.references else {}
                },
                "assignments": task.assignments if task.assignments else {},
                "appliedCategories": task.applied_categories if task.applied_categories else {},
                "priority": task.priority,
                "startDateTime": task.start_date_time,
                "dueDateTime": task.due_date_time
            }
            
            await client.planner.tasks.post(body=new_task)
            logging.info(f"Kopierte oppgave: {task.title}")
        
        return {
            "planId": created_plan.id,
            "title": created_plan.title,
            "status": "success",
            "message": f"Opprettet ny planner for team {team_id}"
        }
        
    except ODataError as e:
        error_message = e.error.message if e.error else str(e)
        logging.error(f"Microsoft Graph API feil ved opprettelse av planner: {error_message}")
        return None
        
    except Exception as e:
        logging.error(f"Uventet feil ved opprettelse av planner: {str(e)}")
        return None
