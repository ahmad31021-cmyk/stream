from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

# Internal Modules
from api.routes import router as api_router

# Enterprise FastAPI Initialization
app = FastAPI(
    title="SCAPILE Enterprise API",
    description="Maritime Legal Sync & Search System Backend API",
    version="3.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Robust CORS Configuration for Frontend Integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to the frontend's domain (e.g., https://scapile.com)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API Routes
app.include_router(api_router, prefix="/api/v1")

@app.on_event("startup")
async def startup_event():
    """
    Startup hook for logging and initializing global connections if necessary.
    """
    logger.info("=" * 60)
    logger.info("   SCAPILE FastAPI Web Layer Started")
    logger.info("   Ready to accept frontend connections.")
    logger.info("=" * 60)

@app.on_event("shutdown")
async def shutdown_event():
    """
    Graceful shutdown hook for resource cleanup.
    """
    logger.warning("SCAPILE API is shutting down. Cleaning up resources...")