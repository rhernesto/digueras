import json
import time
import requests
import datetime as dt

class FacebookAdsExtractor:
    """ Classe para extrair dados da API Graph do Facebook Ads. """

    def __init__(self, act_id: str, access_token: str, api_version: str = "11.0"):
        self.act_id = act_id
        self.access_token = access_token
        self.api_version = api_version

        self.session = requests.Session()

    def _request(self, path: str, params: dict = {}) -> dict:
        """ Faz um GET para a API Graph do Facebook. """

        if "access_token" not in params:
            params.update({"access_token": self.access_token})

        response = self.session.get(
            f'https://graph.facebook.com/v{self.api_version}/{path}',
            params=params
        )
        response.encoding = 'utf-8'

        data = response.json()

        if response.ok:
            return data
        else:
            raise ConnectionError(
                f'Erro de conexão. Código: {response.status_code}. Msg: {data}')

    def check_fb_cpu_usage(self):
        res = self.session.get(f"https://graph.facebook.com/v{self.api_version}/insights")

        case_usage = res.headers.get("x-business-use-case-usage", {})
        stats = case_usage.get(self.act_id.replace("act", ""), {})

        return {
            "cpu_time": stats.get("total_cputime", 0),
            "time_to_regain_access": stats.get("estimated_time_to_regain_access", 0) * 60
        }

    def get_ads(self, fields: list, limit: int = 1000) -> list:
        res = self._request(f"{self.act_id}/ads", {
            "fields": ",".join(fields)
        })

        return res["data"]

    def get_ad_data(self, 
                    ad_id: str, 
                    fields: list, 
                    breakdowns: list = [], 
                    level: str = None, 
                    since: str = "2019-01-01",
                    until: str = None,
                    time_increment: int = None,
                    filtering: list = None) -> dict:
        fields_str = ','.join(fields)
        breakdowns_str = ','.join(breakdowns)

        if not until:
            until = dt.datetime.now().strftime("%Y-%m-%d")

        if time_increment ==0:
            time_increment = None
            
        all_data = []

        params = {
            "fields": fields_str,
            "breakdowns": breakdowns_str,
            "level": level,
            "time_range": json.dumps({"since": since, "until": until}),
            "filtering": json.dumps(filtering),
            "time_increment": time_increment,
            "after": None
        }

        while True:
            # logging.info(f"Realizando requisição fb ads. after token: {params.get('after')}")

            res = self._request(f"{ad_id}/insights", params)
            data = res.get("data", [])

            # logging.info(f"Quantia de dados ::: {len(data)}")

            if len(data) == 0:
                break

            all_data.extend(data)

            next_token = res.get("paging", {}).get("cursors", {}).get("after")

            if next_token == params.get("after"):
                break

            params["after"] = next_token

            time.sleep(10)

            if self.check_fb_cpu_usage()["cpu_time"] > 50:
                # logging.info("Graph API CPU usage > 50, esperando 500 segundos.")
                time.sleep(500)

        return all_data