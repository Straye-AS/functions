import logging
import json
import azure.functions as func
from teamsplanner.get_teams import TeamsProcessor

app = func.FunctionApp()

@app.timer_trigger(schedule="0 * * * * *", arg_name="teamsTimer", run_on_startup=False,
              use_monitor=False) 
async def teamsplanner(teamsTimer: func.TimerRequest) -> None:
    if teamsTimer.past_due:
        logging.info('The timer is past due!')

    try:
        processor = TeamsProcessor()
        # Automatisk initialisering ved første kall
        response_data = await processor.get_teams_async()
        
        logging.info(f'Success! {response_data}')
        
    except Exception as e:
        error_response = {
            "error": str(e),
            "status": "error"
        }
        
        logging.error(f'Error retrieving teams: {error_response}')

@app.function_name(name="get_teams")
@app.route(route="teams")
async def teams_endpoint(req: func.HttpRequest) -> func.HttpResponse:
    """Async HTTP endpoint for getting teams"""
    logging.info('Python HTTP trigger function processed a request.')
    
    try:
        processor = TeamsProcessor()
        # Automatisk initialisering ved første kall
        response_data = await processor.get_teams_async()
        
        logging.info(f'Successfully retrieved {response_data["count"]} teams.')
        
        return func.HttpResponse(
            body=json.dumps(response_data),
            mimetype="application/json",
            status_code=200
        )
        
    except Exception as e:
        error_response = {
            "error": str(e),
            "status": "error"
        }
        
        return func.HttpResponse(
            body=json.dumps(error_response),
            mimetype="application/json",
            status_code=500
        )