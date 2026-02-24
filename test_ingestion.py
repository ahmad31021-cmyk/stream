import sys
import asyncio
from logic.sync_engine import SyncEngine
from loguru import logger

# Loguru ko configure kar rahe hain taake DEBUG level (chunking details) terminal pe nazar aaye
logger.remove()
logger.add(
    sys.stdout, 
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>", 
    level="DEBUG"
)

async def run_forensic_test():
    logger.info("==================================================")
    logger.info("🚀 STARTING ADVANCED FORENSIC INGESTION TEST 🚀")
    logger.info("==================================================")
    
    # Engine initialize karo
    engine = SyncEngine()
    
    # Process start karo (Ye Drive se file uthayega, chunk karega, aur metadata lagayega)
    await engine.start()
    
    logger.info("==================================================")
    logger.info("✅ FORENSIC INGESTION TEST COMPLETED ✅")
    logger.info("==================================================")

if __name__ == "__main__":
    asyncio.run(run_forensic_test())