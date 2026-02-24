from pydantic import BaseModel, Field
from typing import Optional

class ChatRequest(BaseModel):
    """
    Validates incoming chat queries from the frontend.
    """
    query: str = Field(
        ..., 
        description="The user's input query or RCH command.",
        min_length=1
    )
    thread_id: Optional[str] = Field(
        default=None, 
        description="The active OpenAI Thread ID. Send null to start a new conversation."
    )

class ChatResponse(BaseModel):
    """
    Standardized response structure sent back to the frontend.
    """
    response_text: str = Field(
        ..., 
        description="The generated response from the SCP Assistant."
    )
    thread_id: str = Field(
        ..., 
        description="The active Thread ID to maintain conversation state."
    )
    is_rch_triggered: bool = Field(
        default=False, 
        description="Flag for the frontend UI to determine if the strict 5-line RCH UI should be rendered."
    )