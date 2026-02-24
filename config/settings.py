import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from loguru import logger

class Settings(BaseSettings):
    """
    Enterprise Configuration Management.
    Validates all environment variables on startup using Pydantic.
    """
    
    # OpenAI Configuration
    OPENAI_API_KEY: str
    OPENAI_ASSISTANT_ID: Optional[str] = None
    OPENAI_VECTOR_STORE_ID: Optional[str] = None
    
    # Google Drive Configuration
    GOOGLE_CREDENTIALS_FILE: str = "service_account.json"
    GOOGLE_DRIVE_FOLDER_ID: str

    # Database Configuration
    DB_PATH: str = "database/state.json"

    # Pydantic Config: Read from .env file
    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8",
        extra="ignore" # Ignore extra keys in .env
    )

    def validate_setup(self):
        """
        Performs a sanity check on critical file paths.
        """
        if not os.path.exists(self.GOOGLE_CREDENTIALS_FILE):
            logger.critical(f"Missing Google Credentials File: {self.GOOGLE_CREDENTIALS_FILE}")
            raise FileNotFoundError(f"Please place '{self.GOOGLE_CREDENTIALS_FILE}' in the root directory.")
        
        # Ensure database directory exists
        db_path_obj = Path(self.DB_PATH)
        db_path_obj.parent.mkdir(parents=True, exist_ok=True)

# Instantiate global settings object
try:
    settings = Settings()
    settings.validate_setup()
    logger.success("Configuration loaded and validated successfully.")
except Exception as e:
    logger.critical(f"Configuration Error: {str(e)}")
    raise ValueError("System cannot start due to missing configuration.") from e