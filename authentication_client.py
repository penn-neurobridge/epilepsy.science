# %%
import boto3
import requests
import json
import logging
log = logging.getLogger()

# %%
class AuthenticationClient:
    """
    This class is used to authenticate with the Pennsieve API. It is a singleton class that is used to authenticate with the Pennsieve API.
    """
    def __init__(self, api_host, api_key, api_secret):
        self.api_host = api_host
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = None

    def authenticate(self):
        url = f"{self.api_host}/authentication/cognito-config"

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = json.loads(response.content)

            cognito_app_client_id = data["tokenPool"]["appClientId"]
            cognito_region = data["region"]

            cognito_idp_client = boto3.client(
                "cognito-idp",
                region_name=cognito_region,
                aws_access_key_id="",
                aws_secret_access_key="",
            )

            login_response = cognito_idp_client.initiate_auth(
              AuthFlow="USER_PASSWORD_AUTH",
              AuthParameters={"USERNAME": self.api_key, "PASSWORD": self.api_secret},
              ClientId=cognito_app_client_id,
            )

            access_token = login_response["AuthenticationResult"]["AccessToken"]
            self.access_token = access_token
            return access_token
        
        except requests.HTTPError as e:
            log.error(f"failed to reach authentication server with error: {e}")
            raise e
        
        except json.JSONDecodeError as e:
            log.error(f"failed to decode authentication response with error: {e}")
            raise e
        
        except Exception as e:
            log.error(f"failed to authenticate with error: {e}")
            raise e

# %%
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    client = AuthenticationClient(os.getenv("PENNSIEVE_API_HOST"), os.getenv("API_TOKEN"), os.getenv("API_SECRET"))
    client.authenticate()
    print(client.access_token)

# %%
