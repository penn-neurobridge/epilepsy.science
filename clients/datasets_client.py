import requests
import json
import logging

from .base_client import BaseClient

log = logging.getLogger()

class DatasetsClient(BaseClient):
    def __init__(self, api_host, session_manager):
        super().__init__(session_manager)

        self.api_host = api_host

    @BaseClient.retry_with_refresh
    def get_all_datasets(self):
        url = f"{self.api_host}/datasets/?includeBannerUrl=false&includePublishedDataset=false&api_key={self.session_manager.session_token}"

        headers = {
            "accept": "*/*",
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            datasets = response.json()

            return datasets
        except requests.HTTPError as e:
            log.error(f"failed to create import with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode import response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to get import with error: {e}")
            raise e

    # @BaseClient.retry_with_refresh
    # def get_presign_url(self, import_id, dataset_id, upload_key):
    #     url = f"{self.api_host}/import/{import_id}/upload/{upload_key}/presign?dataset_id={dataset_id}"

    #     headers = {
    #         "Content-type": "application/json",
    #         "Authorization": f"Bearer {self.session_manager.session_token}"
    #     }

    #     try:
    #         response = requests.get(url, headers=headers)
    #         response.raise_for_status()
    #         data = response.json()

    #         return data["url"]
    #     except requests.HTTPError as e:
    #         log.error(f"failed to generate pre-sign URL for import file with error: {e}")
    #         raise e
    #     except json.JSONDecodeError as e:
    #         log.error(f"failed to decode pre-sign URL response with error: {e}")
    #         raise e
    #     except Exception as e:
    #         log.error(f"failed to generate pre-sign URL for import file with error: {e}")
    #         raise e
