import asyncio
from openai import AsyncOpenAI
from loguru import logger

# Internal Modules
from config.settings import settings
from api.schemas import ChatResponse

class ChatService:
    """
    Enterprise-grade asynchronous service to handle interactions with the 
    OpenAI Assistants API. Manages Threads, Messages, and Polling Runs securely.
    """

    def __init__(self):
        """
        Initializes the Native Async OpenAI client.
        This guarantees the FastAPI event loop is never blocked by external I/O.
        """
        if not settings.OPENAI_API_KEY:
            logger.critical("Cannot initialize ChatService: OPENAI_API_KEY missing.")
            raise ValueError("OPENAI_API_KEY is required.")
        
        self.client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
        
        # The Assistant ID is fetched from environment/settings.
        # It gets set by the SyncEngine during the initial ingestion phase.
        self.assistant_id = settings.OPENAI_ASSISTANT_ID

    async def execute_chat_turn(self, query: str, thread_id: str = None) -> ChatResponse:
        """
        Executes a complete interaction cycle with the OpenAI Assistant.
        
        Args:
            query (str): The user's input text or RCH command.
            thread_id (str, optional): The existing conversation thread. Defaults to None.

        Returns:
            ChatResponse: Pydantic validated response containing text, thread ID, and RCH flag.
        """
        if not self.assistant_id:
            logger.error("ChatService invoked but OPENAI_ASSISTANT_ID is not configured.")
            raise ValueError("System is not ready. Assistant ID missing. Run SyncEngine first.")

        try:
            # 1. Thread Management: Create a new thread if none exists
            if not thread_id:
                thread = await self.client.beta.threads.create()
                thread_id = thread.id
                logger.info(f"Created new conversation thread: {thread_id}")

            # 2. Add the User's Message to the Thread
            await self.client.beta.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=query
            )

            # 3. Create a Run (Trigger the Assistant)
            run = await self.client.beta.threads.runs.create(
                thread_id=thread_id,
                assistant_id=self.assistant_id
            )

            # 4. Asynchronous Polling Loop for Run Completion
            # We sleep asynchronously to free up the CPU for other concurrent requests
            while run.status in ["queued", "in_progress", "cancelling"]:
                await asyncio.sleep(1.0)  # Wait 1 second between checks
                run = await self.client.beta.threads.runs.retrieve(
                    thread_id=thread_id,
                    run_id=run.id
                )

            # 5. Process the Result
            if run.status == "completed":
                # Fetch the latest message added by the Assistant
                messages = await self.client.beta.threads.messages.list(
                    thread_id=thread_id,
                    order="desc",
                    limit=1
                )
                
                latest_msg = messages.data[0]
                response_text = latest_msg.content[0].text.value

                # 6. Heuristic to Detect if the RCH Protocol was successfully triggered
                # This helps the frontend switch from "Chat UI" to "Forensic Output UI"
                is_rch = "Internal Pagination:" in response_text and "Searchable String:" in response_text

                logger.success(f"Successfully processed response for thread: {thread_id}")

                return ChatResponse(
                    response_text=response_text,
                    thread_id=thread_id,
                    is_rch_triggered=is_rch
                )
            else:
                logger.error(f"Assistant Run failed with status: {run.status}")
                if run.last_error:
                    logger.error(f"Run Error Details: {run.last_error.message}")
                raise RuntimeError(f"Failed to generate response. Status: {run.status}")

        except Exception as e:
            logger.error(f"Exception during chat execution: {str(e)}")
            raise e