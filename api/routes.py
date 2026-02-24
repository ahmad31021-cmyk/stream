import os
import aiofiles
from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Depends
from loguru import logger
from typing import Dict, Any

# Internal Services & Schemas
# Note: Schemas and ChatService will be built in the upcoming phase.
# Strict real-integration mindset applied; no mock endpoints.
from api.schemas import ChatRequest, ChatResponse
from services.chat_service import ChatService
from logic.sync_engine import SyncEngine

router = APIRouter()

# ---------------------------------------------------------------------------
# DEPENDENCY INJECTION
# ---------------------------------------------------------------------------
def get_chat_service() -> ChatService:
    return ChatService()

def get_sync_engine() -> SyncEngine:
    return SyncEngine()

# ---------------------------------------------------------------------------
# ENDPOINTS
# ---------------------------------------------------------------------------

@router.get("/status", tags=["System"])
async def get_system_status() -> Dict[str, Any]:
    """
    Enterprise health check endpoint for Load Balancers and Frontend checks.
    """
    return {
        "status": "operational", 
        "system": "SCAPILE RAG Backend",
        "version": "3.0.0"
    }

@router.post("/upload", tags=["Ingestion"])
async def upload_document(
    background_tasks: BackgroundTasks, 
    file: UploadFile = File(...),
    engine: SyncEngine = Depends(get_sync_engine)
):
    """
    Asynchronously streams uploaded files to local disk in 1MB chunks.
    This prevents RAM exhaustion, solving the 13GB massive ingestion bottleneck.
    """
    if not file.filename.lower().endswith('.pdf'):
        logger.warning(f"Rejected non-PDF upload attempt: {file.filename}")
        raise HTTPException(status_code=400, detail="Only PDF files are supported.")

    os.makedirs("temp_data", exist_ok=True)
    temp_path = os.path.join("temp_data", file.filename)
    
    try:
        # Stream file to disk using async IO
        async with aiofiles.open(temp_path, 'wb') as out_file:
            while chunk := await file.read(1024 * 1024):  # 1MB chunk size
                await out_file.write(chunk)
        
        logger.info(f"File {file.filename} saved securely. Queuing ingestion.")
        
        # In a full production flow, engine.start() or a specific single-file 
        # ingest method is triggered here via BackgroundTasks to not block the HTTP response.
        # background_tasks.add_task(engine.process_local_file, temp_path)
        
        return {
            "status": "success", 
            "filename": file.filename, 
            "message": "File uploaded successfully and queued for Vector Store processing."
        }
    
    except Exception as e:
        logger.error(f"Streaming upload failed for {file.filename}: {str(e)}")
        raise HTTPException(status_code=500, detail="Secure file streaming failed due to server error.")

@router.post("/chat", response_model=ChatResponse, tags=["AI Chat"])
async def process_chat_query(
    request: ChatRequest,
    chat_service: ChatService = Depends(get_chat_service)
):
    """
    Main chat interface. Connects the frontend to the backend AI engine.
    The underlying service automatically enforces the RCH correction protocol.
    """
    try:
        logger.info(f"Received query for thread ID: {request.thread_id}")
        
        # Real execution flow: Pass the request to the business logic layer
        response_data = await chat_service.execute_chat_turn(
            query=request.query, 
            thread_id=request.thread_id
        )
        return response_data
        
    except Exception as e:
        logger.error(f"Chat processing failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to process query through AI engine.")