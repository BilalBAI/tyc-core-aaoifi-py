import requests
import base64
import json
import pandas as pd
from pandas import DataFrame

from .clients import HscloudClient


class BAScreen:
    def __init__(self):
        self.hs_cloud_client=HscloudClient()
    
    def screen(self, secu_code, end_date, tolerance: int):
        re=self.hs_cloud_client.get(secu_code, end_date)
        if re.empty:
            print('No Data')
            return 0
        re=re[['classification', 'currency_unit', 'end_date', 'items_name', 'number', 'period_mark', 'secu_abbr', 'secu_code', 'subsection_income_t_period']]
        re['subsection_income_t_period']=re['subsection_income_t_period'].astype(float)
        re[f'subsection_income_t_period/total']=re['subsection_income_t_period']/re.loc[re['items_name']=='合计','subsection_income_t_period'].values[0]
        re['shariah_compliance'] = re['items_name'].map(self.hs_cloud_client.keywords).fillna('compliant')
        summary=re[['shariah_compliance','subsection_income_t_period',  'subsection_income_t_period/total']].groupby(by='shariah_compliance',as_index=False).sum()
        summary=summary[summary['shariah_compliance']!='not-applicable']
        if 'non-compliant' in summary['shariah_compliance'].values:
            non_compliant_perc=summary.loc[summary['shariah_compliance']=='non-compliant','subsection_income_t_period/total'].values[0]
        else:
            non_compliant_perc=0
        if 'questionable' in summary['shariah_compliance'].values:
            questionable_perc=summary.loc[summary['shariah_compliance']=='questionable','subsection_income_t_period/total'].values[0]
        else:
            questionable_perc=0
        print()
        print('---------------------------------------------------------------')
        if non_compliant_perc > tolerance:
            print(f"{re['secu_abbr'].values[0]} is Shariah Non-Compliant")
        else:
            print(f"{re['secu_abbr'].values[0]} is Shariah Compliant")
        print()
        print('Summary')
        print(summary)
        print()
        print('Details')
        print(re)
        print('---------------------------------------------------------------')
        print()

    def run_uni(self, universe, end_date):
        re_list=[]
        for i in self.hs_cloud_client.config[universe]:
            try:
                print(i)
                re=self.hs_cloud_client.get(i, end_date)
                print(re)
                if re.empty:
                    continue
                re=re[['classification', 'currency_unit', 'end_date', 'items_name', 'number', 'period_mark', 'secu_abbr', 'secu_code', 'subsection_income_t_period']]
                re_list.append(re)
            except:
                continue
        df=pd.concat(re_list,ignore_index=True)
        return df
    