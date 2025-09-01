import os
from dotenv import load_dotenv

# Load the utilities environment file
load_dotenv('utils.env')

class UtilsConfig:
    """Configuration class for utility services like Google Sheets and REDCap."""
    
    def __init__(self):
        # Google Sheets Configuration
        self.MIGRATION_TRACKER = os.getenv('MIGRATION_TRACKER')
        self.MIGRATION_TRACKER_SHEET_NAME = os.getenv('MIGRATION_TRACKER_SHEET_NAME')
        self.RID_HUPID_SHEET_NAME = os.getenv('RID_HUPID_SHEET_NAME')