import requests
import base64
import json
import pandas as pd
from pandas import DataFrame

class BaseClient:
    def __init__(self):
        f = open('config.json') 
        self.config = config = json.load(f)
        self.keywords = {}
        self.keywords.update({i: 'questionable' for i in config['questionable_keywords']})
        self.keywords.update({i: 'non-compliant' for i in config['non_compliant_keywords']})
        self.keywords.update({i: 'not-applicable' for i in config['not_applicable']})


    def post_db(self):
        pass
    def post_json(self):
        pass


class HscloudClient(BaseClient):
    def __init__(self):
        super().__init__()
        self.get_token(self.config['app_key'],self.config['app_secrect'])
        self.missing=[]

    def get_token(self, app_key, app_secrect):
        bytesString = (app_key + ':' + app_secrect).encode(encoding="utf-8")
        url = 'https://sandbox.hscloud.cn/oauth2/oauth2/token'
        header = {
            'Content-Type':
            'application/x-www-form-urlencoded',
            'Authorization':
            'Basic ' + str(base64.b64encode(bytesString), encoding="utf-8")
        }
        field = {'grant_type': 'client_credentials'}
        r = requests.post(url, data=field, headers=header)
        if r.json().get('access_token'):
            self.token = r.json().get('access_token')
            print("Pub Bearer:" + str(self.token))
            return
        else:
            print("Pub Bearer Failed")
            exit

    def get(self, secu_code: str, end_date: str = None):
        url=self.config['url']
        params = f"secu_code={secu_code}&classification=50"
        if end_date:
            params += f"&end_date={end_date}"
        header = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Bearer ' + self.token
        }
        r = requests.post(url, data=params, headers=header)
        return DataFrame(r.json().get('data')).fillna(0)

