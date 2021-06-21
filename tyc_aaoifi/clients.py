import requests
import base64
import json
import pandas as pd
from pandas import DataFrame
pd.set_option('display.width', 5000)
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
    def __init__(self,url,app_key,app_secrect):
        # super().__init__()
        self.url=url
        self.get_token(app_key,app_secrect)
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

    def get(self, secu_code: str, classification: int, end_date: str = None):
        url=self.url
        params = f"secu_code={secu_code}&classification={classification}"
        if end_date:
            params += f"&end_date={end_date}"
        header = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Bearer ' + self.token
        }
        r = requests.post(url, data=params, headers=header)
        return DataFrame(r.json().get('data')).fillna(0)

def h_stocks(tickers,exchange='XHKG'):
    client=HscloudClient(url_h,app_key,app_secrect)
    df_in=pd.read_sql(sql='income_segmentation',con=CON)
    exist=df_in['ticker'].unique().tolist()
    tickers=list(set(tickers) - set(exist))
    tickers.reverse()
    print(f'ticker length {len(tickers)}')
    # tickers=tickers.astype(str)
    count=0
    for i in tickers:
        if count % 20==0:
            client=HscloudClient(url_h,app_key,app_secrect)
            print('restart')
        count+=1
        print(i)
        print(type(i))
        re=client.get(i.zfill(5),50)
        if re.empty:
            client.missing.append(i)
            continue
        re=re[['classification', 'currency_unit', 'end_date', 'items_name', 'number', 'period_mark', 'secu_abbr', 'secu_code', 'subsection_income_t_period']]
        re=re.rename(columns={'currency_unit':'currency','subsection_income_t_period':'income','items_name':'business_line','end_date':'period'})
        re['ticker']=i
        re['exchange']=exchange
        re=re[['ticker','exchange','period','business_line','income','currency']]
        re['income']=re['income'].replace({'': 0}).astype(float)
        re.to_sql(con=CON,name='income_segmentation',if_exists='append',index=False)
        print(re)
    return client


def a_stocks(exchange, tickers):
    client=HscloudClient(url_a,app_key,app_secrect)
    df_in=pd.read_sql(sql='income_segmentation',con=CON)
    exist=df_in['ticker'].unique().tolist()
    tickers=list(set(tickers) - set(exist))
    tickers.reverse()
    print(f'ticker length {len(tickers)}')
    # tickers=tickers.astype(str)
    count=0
    for i in tickers:
        if count % 20==0:
            client=HscloudClient(url_a,app_key,app_secrect)
            print('restart')
        count+=1
        print(i)
        print(type(i))
        re=client.get(i.zfill(6),20)
        if re.empty:
            client.missing.append(i)
            continue
        re=re[['end_date','project','main_oper_income']]
        re=re.rename(columns={'main_oper_income':'income','project':'business_line','end_date':'period'})
        re['currency']='人民币元'
        re['ticker']=i
        re['exchange']=exchange
        re=re[['ticker','exchange','period','business_line','income','currency']]
        re['income']=re['income'].replace({'': 0}).astype(float)
        re.to_sql(con=CON,name='income_segmentation',if_exists='append',index=False)
        print(re)
    return client


# hk_tickers=df_master[df_master['exchange']=='XHKG']['ticker']
# XHKG_client=h_stocks(hk_tickers)

# shg_tickers=df_master[df_master['exchange']=='XSHG']['ticker']
# XSHG_client=a_stocks('XSHG',shg_tickers)

# she_tickers=df_master[df_master['exchange']=='XSHE']['ticker']
# XSHE_client=a_stocks('XSHE',she_tickers)

# XSHG_client=a_stocks('XSHG',XSHG_client.missing)
# XSHE_client=a_stocks('XSHE',XSHE_client.missing)

# XHKG_client=h_stocks(XHKG_client.missing)

# XHKG_client=h_stocks(hs_h.missing)


# CON='sqlite:////Users/BY/Desktop/TYC.db'
# app_key= "a00ad136-9358-4f1a-bf56-3f60a94f5f51"
# app_secrect= "11905dd4-ef7b-46aa-b4fe-7cad10c1db4b"
# url_h= "https://sandbox.hscloud.cn/gildatahk/v1/financialnotes/hk_mainincomestru"
# url_a= "https://sandbox.hscloud.cn/gildataastock/v1/financialnotes/as_mainincomestru_latest"
# df_master=pd.read_sql(sql='master',con=CON)




# # XHKG
# hs_h=HscloudClient(url_h,app_key,app_secrect)
# hk_tickers=df_master[df_master['exchange']=='XHKG']['ticker']
# hk_tickers=hk_tickers.astype(str)
# for i in hk_tickers:
#     print(i)
#     print(type(i))
#     re=hs_h.get(i.zfill(5),50)
#     if re.empty:
#         hs_h.missing.append(i)
#         continue
#     re=re[['classification', 'currency_unit', 'end_date', 'items_name', 'number', 'period_mark', 'secu_abbr', 'secu_code', 'subsection_income_t_period']]
#     re=re.rename(columns={'currency_unit':'currency','subsection_income_t_period':'income','items_name':'business_line','end_date':'period'})
#     re['ticker']=i
#     re['exchange']='XHKG'
#     re=re[['ticker','exchange','period','business_line','income','currency']]
#     re['income']=re['income'].replace({'': 0}).astype(float)
#     re.to_sql(con=CON,name='income_segmentation',if_exists='append',index=False)
#     print(re)

