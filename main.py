import sys
import time
from loguru import logger

# Internal Modules
# We import settings first to ensure environment validation happens immediately
from config.settings import settings
from logic.sync_engine import SyncEngine
from utils.logger import logger  # Explicit import to ensure config is applied

def main():
    """
    Application Entry Point.
    Bootstraps the SCAPILE Sync System and handles top-level execution flow.
    """
    try:
        # 1. Startup Banner & Initialization
        logger.info("="*60)
        logger.info("   SCAPILE - ENTERPRISE MARITIME LEGAL SYNC SYSTEM")
        logger.info("   Version: 3.0 | Mode: Production")
        logger.info("="*60)
        
        logger.info(f"Environment Configured. Database Path: {settings.DB_PATH}")
        
        # 2. Initialize the Core Engine
        # This establishes connections to Google Drive and OpenAI
        logger.info("Initializing Core Services...")
        engine = SyncEngine()
        
        # 3. Execute Synchronization Workflow
        start_time = time.time()
        
        # The engine handles the complex logic of:
        # Drive Scan -> Delta Check -> Download -> Vector Store Upload -> DB Update
        engine.start()
        
        elapsed_time = time.time() - start_time
        
        # 4. Success Completion
        logger.success(f"System finished successfully in {elapsed_time:.2f} seconds.")
        logger.info("="*60)
        sys.exit(0)

    except KeyboardInterrupt:
        # Handles Manual Stop (Ctrl+C)
        print("\n") # New line for cleanliness
        logger.warning("Operation interrupted by user. Shutting down gracefully...")
        # In a real daemon, we might want to close DB connections here if they were persistent
        sys.exit(0)
        
    except Exception as e:
        # Handles Fatal/Unexpected Errors
        # We use 'logger.exception' to dump the full stack trace to the log file
        logger.critical(f"Fatal System Error: {str(e)}")
        logger.exception("Full Stack Trace:")
        sys.exit(1)

if __name__ == "__main__":
    main()