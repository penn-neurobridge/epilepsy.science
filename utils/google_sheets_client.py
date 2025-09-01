import logging
import pandas as pd

log = logging.getLogger()

class GoogleSheetsClient:
    """
    A simple client to read Google Sheets data using pandas.
    No authentication needed for public sheets.
    """
    
    def __init__(self, config):
        """
        Initialize the Google Sheets client.
        
        Args:
            config: Configuration object containing MIGRATION_TRACKER
        """
        self.config = config
    
    def read_sheet_as_dataframe(self, spreadsheet_id: str, sheet_identifier: str, use_gid: bool = False) -> pd.DataFrame:
        """
        Read a Google Sheet directly into a pandas DataFrame.
        
        Args:
            spreadsheet_id (str): The ID from the Google Sheets URL
            sheet_identifier (str): The name of the sheet/tab or GID to read
            use_gid (bool): Whether sheet_identifier is a GID (True) or sheet name (False)
        
        Returns:
            pd.DataFrame: The sheet data as a pandas DataFrame
        """
        try:
            # Construct the CSV export URL
            # This works for public Google Sheets
            if use_gid:
                url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/gviz/tq?tqx=out:csv&gid={sheet_identifier}"
                log.info(f"Reading Google Sheet with GID: {sheet_identifier}")
            else:
                url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_identifier}"
                log.info(f"Reading Google Sheet: {sheet_identifier}")
            
            # Read directly into pandas DataFrame
            df = pd.read_csv(url)
            return df
            
        except Exception as e:
            log.error(f"Failed to read Google Sheet '{sheet_identifier}': {e}")
            raise e
    
    def read_migration_tracker(self, sheet_name: str = None) -> pd.DataFrame:
        """
        Read the migration tracker sheet using the spreadsheet ID and sheet name from config.
        
        Args:
            sheet_name (str, optional): The name of the sheet to read. 
                                      If None, uses MIGRATION_TRACKER_SHEET_NAME from config.
                                      Sheet names can contain spaces and periods.
        
        Returns:
            pd.DataFrame: The migration tracker data
        """
        spreadsheet_id = self.config.MIGRATION_TRACKER
        if sheet_name is None:
            sheet_name = self.config.MIGRATION_TRACKER_SHEET_NAME
        return self.read_sheet_as_dataframe(spreadsheet_id, sheet_name, use_gid=False) 
    
    def read_rid_hupid_sheet(self, sheet_name: str = None) -> pd.DataFrame:
        """
        Read the RID-HUPID sheet using the spreadsheet ID and sheet name from config.
        
        Args:
            sheet_name (str, optional): The name of the sheet to read. 
                                      If None, uses RID_HUPID_SHEET_NAME from config.
                                      Sheet names can contain spaces and periods.
        
        Returns:
            pd.DataFrame: The RID-HUPID mapping data
        """
        spreadsheet_id = self.config.MIGRATION_TRACKER
        if sheet_name is None:
            sheet_name = self.config.RID_HUPID_SHEET_NAME
        return self.read_sheet_as_dataframe(spreadsheet_id, sheet_name, use_gid=False)