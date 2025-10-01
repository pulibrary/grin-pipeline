from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


class GoogleOAuth2Client:
    def __init__(self, scopes, secrets_file, token_file):
        self.scopes = scopes
        self.secrets_file = Path(secrets_file)
        self.token_file = Path(token_file)
        self.creds = None

    def authenticate(self):
        if self.token_file.exists():
            self.creds = Credentials.from_authorized_user_file(self.token_file, self.scopes)

        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(self.secrets_file), self.scopes
                )
                self.creds = flow.run_local_server(port=0)
            with open(self.token_file, "w+") as token_out:
                token_out.write(self.creds.to_json())

        return self.creds
