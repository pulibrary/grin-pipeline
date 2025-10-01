from google_auth_oauthlib.flow import InstalledAppFlow
import os, json
from dotenv import load_dotenv

load_dotenv()

SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

secrets = os.environ["GOOGLE_SECRETS_FILE"]
token = os.environ["GOOGLE_TOKEN_FILE"]

flow = InstalledAppFlow.from_client_secrets_file(secrets, SCOPES)

# run a loopback local server on the REMOTE at the forwarded port
creds = flow.run_local_server(
    port=53682,
    open_browser=False,  # don't try to open a remote browser
    authorization_prompt_message="Open this URL in your LOCAL browser: {url}",
    success_message="You may now close this tab.",
    timeout_seconds=180,
)

# persist token
os.makedirs(os.path.dirname(token), exist_ok=True)
with open(token, "w") as f:
    f.write(creds.to_json())
print("Auth OK →", token)
