import os
import requests
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_httplib2 import AuthorizedHttp

class GoogleAuth():
    """ Classe para autenticação com os serviços do Google. """

    def __init__(self,
                 api_name: str,
                 api_version: str,
                 refresh_token: str,
                 use_access_token: bool = False):

        self.api_name = api_name
        self.api_version = api_version
        self.refresh_token = refresh_token

        self.token_uri = 'https://accounts.google.com/o/oauth2/token'

        self.load_env_data(use_access_token)

        self.service = self.create_service()

    def _refresh_token(self) -> dict:
        """ Atuzaliza o token de acesso. """

        params = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token
        }

        auth_url = 'https://www.googleapis.com/oauth2/v4/token'

        res = requests.post(auth_url, data=params)

        return res.json()['access_token']

    def load_env_data(self, use_access_token: bool):
        """ Carrega os tokens necessarios apartir do .env """

        self.client_id = os.environ['CLIENT_ID']
        self.client_secret = os.environ['CLIENT_SECRET']

        self.access_token = '' if not use_access_token else self._refresh_token()

    def create_service(self) -> build:
        """ Cria o serviço de authenticação e acesso. """

        try:
            self.credentials = Credentials(
                self.access_token,
                refresh_token=self.refresh_token,
                token_uri=self.token_uri,
                client_id=self.client_id,
                client_secret=self.client_secret
            )

            authentication = AuthorizedHttp(credentials=self.credentials)

            service = build(
                serviceName=self.api_name,
                version=self.api_version,
                http=authentication
            )

            return service
        except:
            raise Exception(
                'Não foi possivel autenticar com o Google, verifique suas credências.')