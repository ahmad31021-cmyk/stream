from typing import Tuple, Optional
from tinydb import TinyDB, Query
from loguru import logger
from config.settings import settings

class DBManager:
    """
    Manages the local state of synchronized files.
    Uses TinyDB (JSON-based NoSQL) to track which files have been uploaded.
    """

    def __init__(self):
        """
        Initializes the database connection.
        """
        self.db = TinyDB(settings.DB_PATH)
        self.file_table = self.db.table('uploaded_files')
        self.FileQuery = Query()

    def check_file_status(self, file_id: str) -> Tuple[bool, Optional[str]]:
        """
        Checks if a file exists in the database and returns its checksum.
        
        Args:
            file_id (str): The unique Google Drive File ID.

        Returns:
            Tuple[bool, str]: (is_processed, stored_checksum)
        """
        result = self.file_table.get(self.FileQuery.file_id == file_id)
        
        if result:
            return True, result.get('checksum')
        return False, None

    def mark_file_as_processed(self, file_id: str, file_name: str, checksum: str):
        """
        Upserts (Update or Insert) a file record into the database.
        Call this ONLY after a successful upload to OpenAI.

        Args:
            file_id (str): Google Drive File ID.
            file_name (str): File name for readability.
            checksum (str): MD5 Checksum for version control.
        """
        try:
            self.file_table.upsert(
                {
                    'file_id': file_id, 
                    'file_name': file_name, 
                    'checksum': checksum,
                    'status': 'synced'
                },
                self.FileQuery.file_id == file_id
            )
            logger.debug(f"Database updated for file: {file_name}")
        except Exception as e:
            logger.error(f"Failed to write state to DB for {file_name}: {str(e)}")
            raise e