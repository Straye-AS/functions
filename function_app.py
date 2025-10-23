import logging
import json
import os
import azure.functions as func
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential
from azure.ai.agents.models import ListSortOrder

# from teamsplanner.get_teams import TeamsProcessor

app = func.FunctionApp()

# In-memory storage for conversation thread mapping
# TODO: Replace with Azure Table Storage for production
conversation_threads = {}


def get_or_create_thread(conversation_id: str) -> str:
    """
    Get existing thread_id for conversation or create a new one using SDK.
    Returns the thread_id for the conversation.
    """
    # Check if we already have a thread for this conversation
    if conversation_id in conversation_threads:
        logging.info(
            f"Using existing thread {conversation_threads[conversation_id]} for conversation {conversation_id}"
        )
        return conversation_threads[conversation_id]

    # Create a new thread using SDK
    try:
        credential = DefaultAzureCredential()
        project = AIProjectClient(
            credential=credential,
            endpoint="url",
        )

        logging.info(f"Creating new thread for conversation {conversation_id}")
        thread = project.agents.threads.create()
        thread_id = thread.id

        conversation_threads[conversation_id] = thread_id
        logging.info(f"Created thread {thread_id} for conversation {conversation_id}")
        return thread_id

    except Exception as e:
        logging.error(f"Error creating thread: {str(e)}")
        raise Exception(f"Failed to create thread: {str(e)}")


def send_message_and_get_reply(thread_id: str, text: str) -> str:
    """
    Send a message to the specified thread and get the assistant's reply using SDK.
    """
    try:
        credential = DefaultAzureCredential()
        project = AIProjectClient(
            credential=credential,
            endpoint="url",
        )

        agent = project.agents.get_agent("asst_nWEm3vsdtwxTXWWOZGCKPSO4")
        logging.info(f"Successfully connected to agent: {agent.id}")

        # Create user message
        message = project.agents.messages.create(
            thread_id=thread_id, role="user", content=text
        )
        logging.info(f"Created user message: {message.id}")

        # Run the agent
        run = project.agents.runs.create_and_process(
            thread_id=thread_id, agent_id=agent.id
        )
        logging.info(f"Agent run completed with status: {run.status}")

        if run.status == "failed":
            logging.error(f"Run failed: {run.last_error}")
            raise Exception(f"Agent run failed: {run.last_error}")

        # Get the response messages
        messages = project.agents.messages.list(
            thread_id=thread_id, order=ListSortOrder.ASCENDING
        )

        # Find the assistant's response
        reply_text = ""
        for message in messages:
            if message.role == "assistant" and message.text_messages:
                reply_text = message.text_messages[-1].text.value
                break

        if not reply_text:
            logging.warning("No assistant response found")
            reply_text = (
                "I received your message but couldn't generate a proper response."
            )

        logging.info(f"Retrieved assistant reply: {reply_text[:100]}...")
        return reply_text

    except Exception as e:
        logging.error(f"Error processing message with SDK: {str(e)}")
        raise Exception(f"Failed to process message: {str(e)}")


@app.timer_trigger(
    schedule="0 */30 * * * *",  # every 30 minutes
    arg_name="teamsTimer",
    run_on_startup=True,
    use_monitor=False,
)
async def teamsplanner(teamsTimer: func.TimerRequest) -> None:
    if teamsTimer.past_due:
        logging.info("The timer is past due!")

    try:
        # processor = TeamsProcessor()
        # Automatisk initialisering ved første kall
        # response_data = await processor.get_teams_async()
        response_data = {"message": "TeamsProcessor temporarily disabled"}

        logging.info(f"Success! {response_data}")

    except Exception as e:
        error_response = {"error": str(e), "status": "error"}

        logging.error(f"Error retrieving teams: {error_response}")


@app.route(route="teams", auth_level=func.AuthLevel.ANONYMOUS)
async def get_teams(req: func.HttpRequest) -> func.HttpResponse:
    """Async HTTP endpoint for getting teams"""
    logging.info("Python HTTP trigger function processed a request.")

    try:
        response_data = {
            "message": "Successfully called HTTP endpoint!",
            "status": "success",
        }

        logging.info(f"Successfully called teamsplanner!")

        return func.HttpResponse(
            body=json.dumps(response_data), mimetype="application/json", status_code=200
        )

    except Exception as e:
        error_response = {"error": str(e), "status": "error"}

        return func.HttpResponse(
            body=json.dumps(error_response),
            mimetype="application/json",
            status_code=500,
        )


@app.route(route="messages", auth_level=func.AuthLevel.ANONYMOUS, methods=["POST"])
def teams_message_handler(req: func.HttpRequest) -> func.HttpResponse:
    """
    Handle incoming messages from Microsoft Teams via Bot Framework.
    Uses Azure AI Foundry Threads for conversational memory.
    """
    logging.info("Teams message handler function processed a request.")

    try:
        # Parse the incoming Teams message
        req_body = req.get_json()
        logging.info(f"Received Teams message: {json.dumps(req_body, indent=2)}")

        # Extract message text and user info
        if not req_body:
            logging.error("No request body received")
            return func.HttpResponse(
                body=json.dumps({"error": "No request body"}),
                mimetype="application/json",
                status_code=400,
            )

        # Extract text from the Teams message
        text = req_body.get("text", "").strip()
        if not text:
            logging.error("No text content in message")
            return func.HttpResponse(
                body=json.dumps({"error": "No text content in message"}),
                mimetype="application/json",
                status_code=400,
            )

        # Extract conversation and user information
        conversation_info = req_body.get("conversation", {})
        conversation_id = conversation_info.get("id", "default-conversation")

        from_info = req_body.get("from", {})
        user_name = from_info.get("name", "Unknown User")
        user_id = from_info.get("id", "unknown")

        logging.info(
            f"Processing message from {user_name} (ID: {user_id}) in conversation {conversation_id}: {text}"
        )

        # Get or create thread for this conversation
        try:
            thread_id = get_or_create_thread(conversation_id)
            logging.info(f"Using thread {thread_id} for conversation {conversation_id}")

            # Send message and get reply using SDK
            reply_text = send_message_and_get_reply(thread_id, text)

            # Return Teams-compatible response
            teams_response = {"type": "message", "text": reply_text}
            logging.info(f"Returning Teams response: {json.dumps(teams_response)}")

            return func.HttpResponse(
                body=json.dumps(teams_response),
                mimetype="application/json",
                status_code=200,
            )

        except Exception as e:
            logging.error(f"Error processing message with AI agent: {str(e)}")
            return func.HttpResponse(
                body=json.dumps(
                    {
                        "type": "message",
                        "text": "⚠️ I couldn't reach the AI right now. Please try again later.",
                    }
                ),
                mimetype="application/json",
                status_code=200,
            )

    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({"error": "Invalid JSON in request body"}),
            mimetype="application/json",
            status_code=400,
        )

    except Exception as e:
        logging.error(f"Unexpected error in teams_message_handler: {str(e)}")
        return func.HttpResponse(
            body=json.dumps(
                {
                    "type": "message",
                    "text": "⚠️ An unexpected error occurred. Please try again later.",
                }
            ),
            mimetype="application/json",
            status_code=200,
        )
