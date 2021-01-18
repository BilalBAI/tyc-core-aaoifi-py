import requests
import base64
import json
import pandas as pd
from pandas import DataFrame

class HscloudClient:
    def __init__(self):
        f = open('config.json') 
        self.config = config = json.load(f)
        self.getToken(config['app_key'],config['app_secrect'])
        self.missing=[]

    def run(self):
        re_list=[]
        url = self.config['url']
        for i in self.config['universe']:
            print(i)
            params = f"secu_code={i}&classification=50"
            re=DataFrame(self.postOpenApi(url, params)).fillna(0)
            print(re)
            if re.empty:
                self.missing.append(i)
                continue
            for j in ['subsection_income_f_year','subsection_income_t_period']:
                re[j]=re[j].astype(float)
                re[f'{j} %']=re[j]/re.loc[re['items_name']=='合计',j].values[0]
            re_list.append(re)
        df=pd.concat(re_list,ignore_index=True)
        return df

    def getToken(self, app_key, app_secrect):
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

    def postOpenApi(self, url, params):
        header = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Bearer ' + self.token
        }
        r = requests.post(url, data=params, headers=header)
        # print("result = " + str(r.json().get('data')))
        return r.json().get('data')


if __name__ == '__main__':
    c=HscloudClient()
    df=c.run()

