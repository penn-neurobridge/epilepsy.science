import requests
import json
import logging

from .base_client import BaseClient

log = logging.getLogger()

class PackageClient(BaseClient):
    def __init__(self, api_host, session_manager):
        super().__init__(session_manager)

        self.api_host = api_host

    @BaseClient.retry_with_refresh
    def get_download_manifest(self, package_id):
        url = f"{self.api_host}/packages/download-manifest?api_key={self.session_manager.session_token}"

        payload = { "nodeIds": [f"N:package:{package_id}"] }

        headers = {
            "accept": "*/*",
            "Content-type": "application/json"
        }

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

            return data
        except requests.HTTPError as e:
            log.error(f"failed to get download manifest with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode download manifest response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to get download manifest with error: {e}")
            raise e
