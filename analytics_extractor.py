import datetime as dt
from google_auth import GoogleAuth
from google.analytics.data import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest
from google.oauth2.credentials import Credentials
import requests
import os

from typing import List, Any

class GoogleAnalytics3(GoogleAuth):
    """ Classe para extrair dados da API do Google Analytics. """

    def __init__(self, refresh_token: str):
        super().__init__('analyticsreporting', 'v4', refresh_token)

        self.strftime = '%d/%m/%Y %H:%M'
        self.sampling_level = 'LARGE'

    def get_data(self, 
                 metrics: list, 
                 dimensions: list, 
                 view_id: str,
                 start_date: str, 
                 end_date: str, 
                 page_size=100, 
                 filters_expression: str = None) -> List[dict]:
        """ Obtem os dados da pagina informada utilizando métricas e dimensões. """

        metrics_body = [{'expression': f'ga:{metric}'} for metric in metrics]
        dimensions_body = [{'name': f'ga:{dimension}'} for dimension in dimensions]

        requestBody = {
            'reportRequests': [
                {
                    'viewId': view_id,
                    'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                    'metrics': metrics_body,
                    'dimensions': dimensions_body,
                    'samplingLevel': self.sampling_level,
                    'pageSize': page_size,
                    'filtersExpression': filters_expression
                }
            ]
        }

        responseHeader =  {}
        responseHeader["execution_datetime"] = str(dt.datetime.now())
        responseHeader["ID"] = str(view_id)

        response = self.service.reports().batchGet(
            body=requestBody
        ).execute()

        responseHeader.update(response.get('reports', [])[0])

        return responseHeader

class GoogleAnalytics4:

    def __init__(self) -> None:
        self.client = BetaAnalyticsDataClient(credentials = self.get_credentials())

    def run_report(self, property_id, metrics, dimensions, start_date, end_date):
        
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name = dim) for dim in dimensions],
        
            metrics=[Metric(name = met) for met in metrics],
            date_ranges=[DateRange(start_date = start_date, end_date = end_date)],
        )

        response = self.client.run_report(request)
        
        output = {"metric_headers" : [m.name for m in response.metric_headers], "dimension_headers" : [m.name for m in response.dimension_headers] , "rows" : []}
        
        for row in response.rows:
            output["rows"].append({
                "dimension": [dim.value for dim in row.dimension_values], 
                "metrics": [met.value for met in row.metric_values]
            })
        
        return output

    

    def get_credentials(self):
        
        access_token = self.refresh_token()

        creds = Credentials(
            access_token,
            refresh_token= os.environ["REFRESH_TOKEN"],
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
            token_uri="https://accounts.google.com/o/oauth2/token",
            client_id= os.environ["CLIENT_ID"],
            client_secret= os.environ["CLIENT_SECRET"]
        )

        return creds

    def refresh_token(self) -> dict:
        """ Atuzaliza o token de acesso. """

        params = {
            'grant_type': 'refresh_token',
            'client_id': os.environ["CLIENT_ID"],
            'client_secret': os.environ["CLIENT_SECRET"],
            'refresh_token': os.environ["REFRESH_TOKEN"]
        }

        auth_url = 'https://www.googleapis.com/oauth2/v4/token'

        res = requests.post(auth_url, data=params)

        return res.json()['access_token']