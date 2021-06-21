import pandas as pd
from pandas import DataFrame
import json
BUSINESS_SCREENING_CRITERIA = 0.05
FINANCIAL_SCREENING_CRITERIA = 0.2

class JsonGenerator:
    def __init__(self,db_con_str='sqlite:////Users/BY/Desktop/TYC.db'):
        df_bas = pd.read_sql(con=db_con_str,sql='business_activity_screen').rename(columns={'activity':'business_activity_cn'})
        df_is = pd.read_sql(con=db_con_str,sql='income_segmentation')
        self.df_master = pd.read_sql(con=db_con_str,sql='master')
        self.df_market = pd.read_sql(con=db_con_str,sql='market_cap')
        self.df_is_bas = pd.merge(df_is,df_bas,how='left',on='business_activity_cn')
        self.df_fs = pd.read_sql(con=db_con_str,sql='financial_screening')
        self.df_fs = pd.read_sql(con=db_con_str,sql='debt_to_market_cap')

    def _run(self, dict_summary):
        # Business activity screening
        df_is_bas=self.df_is_bas.copy()
        df_market=self.df_market.copy()
        df_fs=self.df_fs.copy()
        dict_summary['period']='2020'
        dict_summary['market_value']=df_market.loc[(df_market['ticker']==dict_summary['ticker'])&(df_market['exchange']==dict_summary['exchange'])&(df_market['period']==dict_summary['period']),'market_cap'].values[0]
        df_ba_details=df_is_bas.loc[(df_is_bas['ticker']==dict_summary['ticker'])&(df_is_bas['exchange']==dict_summary['exchange'])]
        dict_summary['total_income']=int(df_ba_details.loc[df_ba_details['business_activity_cn']=='合计','income'].values[0])
        dict_summary['ccy']=df_ba_details['currency'].values[0]
        df_ba_details=df_ba_details[df_ba_details['permissibility']!='not-applicable']
        df_ba_details['business_income_over_total_income']=df_ba_details['income']/dict_summary['total_income']
        df_ba_details['shariah_compliance']=df_ba_details['permissibility']
        df_ba_details=df_ba_details[['business_activity_cn','business_activity_en', 'income','business_income_over_total_income','shariah_compliance']]
        df_ba_summary=df_ba_details[['income','business_income_over_total_income','shariah_compliance']].groupby('shariah_compliance',as_index=False).sum()
        compliant_p = df_ba_summary.loc[df_ba_summary['shariah_compliance']=='compliant','business_income_over_total_income'].values[0]
        non_compliant_p = df_ba_summary.loc[df_ba_summary['shariah_compliance']=='non-compliant','business_income_over_total_income'].values[0]
        unknown_p = df_ba_summary.loc[df_ba_summary['shariah_compliance']=='unknown','business_income_over_total_income'].values[0]
        if non_compliant_p > BUSINESS_SCREENING_CRITERIA:
            dict_summary['shariah_compliance_summary']='non-compliant'
        elif compliant_p < 1 - BUSINESS_SCREENING_CRITERIA:
            dict_summary['shariah_compliance_summary']='unknown'
        else:
            dict_summary['shariah_compliance_summary']='compliant'
        # Financial screening
        debt_to_mkt_cap=df_fs.loc[(df_fs['ticker']==dict_summary['ticker'])&(df_fs['exchange']==dict_summary['exchange'])&(df_fs['period']==dict_summary['period']),'DEBT_TO_MKT_CAP'].values[0]
        dict_fa={'item':'Debt to Market Capitalization','amount': debt_to_mkt_cap,'shariah_compliance':'compliant'}
        if debt_to_mkt_cap > FINANCIAL_SCREENING_CRITERIA:
            dict_fa['shariah_compliance']='non-compliant'
        # dict_fa['amount_over_market_value']=dict_fa['amount']/dict_summary['market_value']
        if (dict_fa['shariah_compliance']=='non-compliant'):
            dict_summary['shariah_compliance_summary']='non-compliant'
        # results
        # results={'basic_info':dict_summary,'business_activity_screening':{'summary':df_ba_summary.to_dict('records'),'details':df_ba_details.to_dict('records')},'financial_screening':[dict_fa]}
        dict_summary['business_activity_screening_summary']=json.dumps(df_ba_summary.to_dict('records'))
        dict_summary['business_activity_screening_details']=json.dumps(df_ba_details.to_dict('records'))
        dict_summary['financial_screening']=json.dumps([dict_fa])
        return dict_summary

    def run_excel(self):
        results=[]
        for i in self.df_master.index:
            if i==500:
                break
            dict_summary=dict(self.df_master.loc[i])
            try:
                results.append(self._run(dict_summary))
            except:
                continue
        df=DataFrame(results)
        df['key']=df['ticker']+df['exchange']
        df=df[['key','ticker', 'exchange', 'period', 'name', 'name_chinese_simplified', 'market_value', 'total_income', 'ccy', 'shariah_compliance_summary', 'business_activity_screening_details', 'business_activity_screening_summary', 'financial_screening']]
        df.ccy=df.ccy.replace({"港元":"HKD","人民币元":"CNY","美元":"USD","新加坡元":"SGD"})
        df.to_excel('results.xlsx')
        return df


    def run_json(self):
        for i in self.df_master.index:
            dict_summary=dict(self.df_master.loc[i])
            try:
                results=self._run(dict_summary)
            except:
                continue
            with open(f"results/{dict_summary['ticker']}_{dict_summary['exchange']}_{dict_summary['period']}.json", 'w') as fp:
                json.dump(results, fp,ensure_ascii=False)


if __name__ == "__main__":
    jg = JsonGenerator()
    jg.run_excel()

# dict_summary=dict(jg.df_master.loc[0])
# results=jg._run(dict_summary)

# df_bas = pd.read_sql(con='sqlite:////Users/BY/Desktop/TYC.db',sql='business_activity_screen')
# df_is = pd.read_sql(con='sqlite:////Users/BY/Desktop/TYC.db',sql='income_segmentation')
# df_master = pd.read_sql(con='sqlite:////Users/BY/Desktop/TYC.db',sql='master')
# df_market=pd.read_sql(con='sqlite:////Users/BY/Desktop/TYC.db',sql='market_cap')
# df_bas=df_bas.rename(columns={'activity':'business_activity_cn'})
# df_is_bas=pd.merge(df_is,df_bas,how='left',on='business_activity_cn')



# dict_summary=dict(df_master.loc[0])
# dict_summary['period']='2020'
# dict_summary['market_value']=df_market.loc[(df_market['ticker']==dict_summary['ticker'])&(df_market['exchange']==dict_summary['exchange'])&(df_market['period']==dict_summary['period']),'market_cap'].values[0]
# df_ba_details=df_is_bas.loc[(df_is_bas['ticker']==dict_summary['ticker'])&(df_is_bas['exchange']==dict_summary['exchange'])]
# dict_summary['total_income']=int(df_ba_details.loc[df_ba_details['business_activity_cn']=='合计','income'].values[0])
# dict_summary['ccy']=df_ba_details['currency'].values[0]
# if dict_summary['ccy']=='港元':
#     dict_summary['ccy']='HKD'
# elif dict_summary['ccy']=='人民币':
#     dict_summary['ccy']='RMB'
# df_ba_details=df_ba_details[df_ba_details['permissibility']!='not-applicable']
# df_ba_details['business_income_over_total_income']=df_ba_details['income']/dict_summary['total_income']
# df_ba_details['shariah_compliance']=df_ba_details['permissibility']
# df_ba_details=df_ba_details[['business_activity_cn','business_activity_en', 'income','business_income_over_total_income','shariah_compliance']]
# df_ba_summary=df_ba_details[['income','business_income_over_total_income','shariah_compliance']].groupby('shariah_compliance',as_index=False).sum()

# dict_fa={'item':'Interest Bearing Borrowing','amount': 624533000000,'shariah_compliance':'non-compliant'}
# dict_fa['amount_over_market_value']=dict_fa['amount']/dict_summary['market_value']
# dict_summary['shariah_compliance_summary']='non-compliant'


# results={'basic_info':dict_summary,'business_activity_screening':{'summary':df_ba_summary.to_dict('records'),'details':df_ba_details.to_dict('records')},'financial_screening':[dict_fa]}

# with open(f"{dict_summary['ticker']}_{dict_summary['exchange']}_{dict_summary['period']}.json", 'w') as fp:
#     json.dump(results, fp,ensure_ascii=False)



# df=pd.read_excel('/Users/BY/Desktop/TYC/universe.xlsx',sheet_name=1)
# df=pd.read_csv('/Users/BY/Desktop/无标题电子表格 - SZEX.csv')
# df['name']=df['Name']
# df['industry']=df['GICS Ind Name']
# df['exchange']='XSHE'
# df['period']='2020'
# df=df[['ticker','exchange','period','name','industry']]
# df.to_sql(con='sqlite:////Users/BY/Desktop/TYC.db',name='master',if_exists='append',index=False)


# df=pd.read_csv('/Users/BY/Desktop/主营业务标记20210418 - 工作表1.csv')
# df=df[['activity', 'permissibility',  'condition']]
# df.to_sql(con='sqlite:////Users/BY/Desktop/TYC.db',name='business_activity_screen',if_exists='append',index=False)