import logging
import azure.functions as func
from azure.cosmosdb.table.tableservice import TableService
from datetime import datetime
import datetime as dt
import pandas as pd
import json,os
from . import email_sender
store_list = []
def json_to_series(text, column):
    if type(text) != str:
        text = "{}"
    text = json.loads(text)

    if not text and column == 'CRI_Info':
        text = {'amount': 0, 'store': 'x','date': 'x'}
    elif not text:
        text = {'total_amount': 0, 'store_name': 'x',
                'date': 'x', 'time': '0','invoice_amount':0,'eligible_amount':0,'has_loyalty_payment':"0",'tier':'x'}
    keys, values = zip(*[(f"{column}.{dct}",text[dct]) for dct in text])
    return pd.Series(values, index=keys)

def getstorename(store,data):
    try:
        storename = list(filter(lambda x: x['id'] == str(store), data))    
        return storename[0]['name']
    except Exception as e:
        return "x"

def main(mytimer: func.TimerRequest) -> None:
    try:
        global store_list
        table_service = TableService(account_name='emgtdmubeocr', account_key='sb9vnHXijdtvvk+FMzLSOGeFpBeBkzQc6og3YQoNvOEbIneCzbHO1Z/9zHFe4S+URpc7Yq5vsxISBsHex4YkUQ==')
        
        stores = table_service.query_entities('storelist')
        for store in stores:
            store_list.append({'id':store.RowKey,'name':store.StoreName})
        logging.info(store_list)
        
        logging.info("got active store")
        today = datetime.now()
        update_date = today - dt.timedelta(days=1)
        start_date = update_date.strftime("%Y-%m-%dT00:00:00Z")
        till_date = today.strftime("%Y-%m-%dT00:00:00Z")
        logging.info("started...")
        mv_reqs_resp = table_service.query_entities('ManualValidatedData', filter=f"Timestamp ge datetime'{start_date}' and Timestamp le datetime'{till_date}'")
            
        req_list = []
        logging.info("data query..")
        for req in mv_reqs_resp:
            req_list.append(req)
            
        df = pd.DataFrame(req_list)

        result = pd.concat([df, df['DataReceived'].apply(
                    json_to_series, args=('DataReceived',))], axis=1)
        result[['DataReceived.total_amount']] = result[['DataReceived.total_amount']].apply(pd.to_numeric,errors='coerce')
        result[['DataReceived.eligible_amount']] = result[['DataReceived.eligible_amount']].apply(pd.to_numeric,errors='coerce')

        result = pd.concat([result, result['DS_Info'].apply(json_to_series, args=('DS_Info',))], axis=1)
        if 'CRI_Info' in result:
            result = pd.concat([result, result['CRI_Info'].apply(
                json_to_series, args=('CRI_Info',))], axis=1)
        if 'storeMeta' in result:
            result = pd.concat([result, result['storeMeta'].apply(
                json_to_series, args=('storeMeta',))], axis=1)
            result.drop(['storeMeta', 'storeMeta.name'], axis=1, inplace=True)
        result[['DS_Info.total_amount']] = result[['DS_Info.total_amount']].apply(pd.to_numeric,errors='coerce')
        result[['DS_Info.eligible_amount']] = result[['DS_Info.eligible_amount']].apply(pd.to_numeric,errors='coerce')
        
        result[['CRI_Info.amount']] = result[['CRI_Info.amount']].apply(pd.to_numeric,errors='coerce')
        result = result.fillna(0)
        logging.info("converted")
        result.drop(['DataReceived', 'DS_Info', 'etag',
                    'Exceptions', 'PartitionKey'], axis=1, inplace=True)
        result['Timestamp'] = result['Timestamp'].dt.tz_localize(None)
        result['DS_Timestamp'] = df['DS_Timestamp'].apply(
                    lambda a: pd.to_datetime(a).date())
        logging.info("got data")
        req_ids = list(result['RowKey'])
        mv_amount = list(result['DataReceived.eligible_amount'])
        image_url = list(result['image_url'])
        mv_date = list(result['DataReceived.date'])
        mv_store = list(result['DataReceived.store_name'])
        mv_status = list(result['DataReceived.status'])
        tier_info = list(result['DataReceived.tier'])
        ds_amount = list(result['DS_Info.eligible_amount'])
        ds_date = list(result['DS_Info.date'])
        ds_store = list(result['DS_Info.store_name'])
        cri_amount = list(result['CRI_Info.amount'])
        dubai_mall = list(result['DS_Info.is_in_dubai_mall'])
        cri_date = list(result['CRI_Info.date'])
        cri_store = list(result['CRI_Info.store'].apply(lambda x: getstorename(x,store_list)))
        loyalty = list(result['DS_Info.has_loyalty_payment'])
        result['CRI_storename'] = result['CRI_Info.store'].apply(lambda x: getstorename(x,store_list))
        result.drop(['CRI_Info.store'], axis=1, inplace=True)
        result.drop(['DS_Info.store_id'], axis=1, inplace=True)
        result['MV=DS'] = result.apply(lambda x: 'match' if x['DataReceived.store_name'].lower() == x['DS_Info.store_name'].lower() and x['DataReceived.date'] == x['DS_Info.date'] and x['DataReceived.eligible_amount'] == x['DS_Info.eligible_amount'] else 'miss', axis=1)
        result['Auto_Approved<=1000'] = result.apply(lambda x: 'auto' if x['CRI_storename'].lower() == x['DS_Info.store_name'].lower() and x['CRI_Info.date'] == x['DS_Info.date'] and x['CRI_Info.amount'] == x['DS_Info.eligible_amount'] and x['DS_Info.has_loyalty_payment'] == "0" and x['DS_Info.eligible_amount'] <= 1000 and x['DS_Info.is_in_dubai_mall'] == True else 'manual', axis=1)
        result['Auto_Approved<=2000'] = result.apply(lambda x: 'auto' if x['CRI_storename'].lower() == x['DS_Info.store_name'].lower() and x['CRI_Info.date'] == x['DS_Info.date'] and x['CRI_Info.amount'] == x['DS_Info.eligible_amount'] and x['DS_Info.has_loyalty_payment'] == "0" and x['DS_Info.eligible_amount'] <= 2000 and x['DS_Info.is_in_dubai_mall'] == True else 'manual', axis=1)
        result['Auto_Approved<=4000'] = result.apply(lambda x: 'auto' if x['CRI_storename'].lower() == x['DS_Info.store_name'].lower() and x['CRI_Info.date'] == x['DS_Info.date'] and x['CRI_Info.amount'] == x['DS_Info.eligible_amount'] and x['DS_Info.has_loyalty_payment'] == "0" and x['DS_Info.eligible_amount'] <= 4000 and x['DS_Info.is_in_dubai_mall'] == True else 'manual', axis=1)
        result['Auto_Approved<=6000'] = result.apply(lambda x: 'auto' if x['CRI_storename'].lower() == x['DS_Info.store_name'].lower() and x['CRI_Info.date'] == x['DS_Info.date'] and x['CRI_Info.amount'] == x['DS_Info.eligible_amount'] and x['DS_Info.has_loyalty_payment'] == "0" and x['DS_Info.eligible_amount'] <= 6000 and x['DS_Info.is_in_dubai_mall'] == True else 'manual', axis=1)
        result['Auto_Approved<=10000'] = result.apply(lambda x: 'auto' if x['CRI_storename'].lower() == x['DS_Info.store_name'].lower() and x['CRI_Info.date'] == x['DS_Info.date'] and x['CRI_Info.amount'] == x['DS_Info.eligible_amount'] and x['DS_Info.has_loyalty_payment'] == "0" and x['DS_Info.eligible_amount'] <= 10000 and x['DS_Info.is_in_dubai_mall'] == True else 'manual', axis=1)
        mv_eq_ds = list(result['MV=DS'])      
        all_auto_approved = list(result['Auto_Approved<=1000'])
        all_auto_approved_2000 = list(result['Auto_Approved<=2000'])
        all_auto_approved_4000 = list(result['Auto_Approved<=4000'])
        all_auto_approved_6000 = list(result['Auto_Approved<=6000'])
        all_auto_approved_10000 = list(result['Auto_Approved<=10000'])
        logging.info("new df")
        total_requests = result[(result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")]['RowKey']
        auto_approved_1000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=1000'] == "auto")]['RowKey']
        false_positives_1000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=1000'] == "auto") & (result['MV=DS'] == "miss")]['RowKey']
        auto_approved_2000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=2000'] == "auto")]['RowKey']
        false_positives_2000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=2000'] == "auto") & (result['MV=DS'] == "miss")]['RowKey']
        auto_approved_4000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=4000'] == "auto")]['RowKey']
        false_positives_4000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=4000'] == "auto") & (result['MV=DS'] == "miss")]['RowKey']
        auto_approved_6000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=6000'] == "auto")]['RowKey']
        false_positives_6000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=6000'] == "auto") & (result['MV=DS'] == "miss")]['RowKey']
        auto_approved_10000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=10000'] == "auto")]['RowKey']
        false_positives_10000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=10000'] == "auto") & (result['MV=DS'] == "miss")]['RowKey']
        
        sum_of_total = result[(result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")]['DS_Info.eligible_amount'].sum()
        sum_of_auto_1000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=1000'] == "auto")]['DS_Info.eligible_amount'].sum()
        sum_of_false_1000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=1000'] == "auto") & (result['MV=DS'] == "miss")]['DS_Info.eligible_amount'].sum()
        sum_of_auto_2000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=2000'] == "auto")]['DS_Info.eligible_amount'].sum()
        sum_of_false_2000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=2000'] == "auto") & (result['MV=DS'] == "miss")]['DS_Info.eligible_amount'].sum()
        sum_of_auto_4000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=4000'] == "auto")]['DS_Info.eligible_amount'].sum()
        sum_of_false_4000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=4000'] == "auto") & (result['MV=DS'] == "miss")]['DS_Info.eligible_amount'].sum()
        sum_of_auto_6000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=6000'] == "auto")]['DS_Info.eligible_amount'].sum()
        sum_of_false_6000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=6000'] == "auto") & (result['MV=DS'] == "miss")]['DS_Info.eligible_amount'].sum()
        sum_of_auto_10000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=10000'] == "auto")]['DS_Info.eligible_amount'].sum()
        sum_of_false_10000 = result[((result['DataReceived.status'] == "Accepted") | (result['DataReceived.status'] == "Rejected")) & (result['Auto_Approved<=10000'] == "auto") & (result['MV=DS'] == "miss")]['DS_Info.eligible_amount'].sum()
        
        df = pd.DataFrame({'Request_IDs':req_ids,'Image URL':image_url,'MV_Amount':mv_amount,'MV_Date':mv_date,'MV_Store':mv_store,'MV_Status':mv_status,'DS_Amount':ds_amount,'DS_Date':ds_date,'DS_Store':ds_store,'CRI_Amount':cri_amount,'CRI_Date':cri_date,'CRI_Store':list(result['CRI_storename']),'Loyalty':loyalty,'Mv=DS':mv_eq_ds,'From_Dubai_Mall':dubai_mall,'Tier':tier_info,'Auto_Approved<=1000':all_auto_approved,'Auto_Approved<=2000':all_auto_approved_2000,'Auto_Approved<=4000':all_auto_approved_4000,'Auto_Approved<=6000':all_auto_approved_6000,'Auto_Approved<=10000':all_auto_approved_10000})
        percentage_df = pd.DataFrame(columns=['Type' ,'Count', 'Percentage', 'Total Amount', 'Average','USD Amount','Estimated Miles','Estimated Cost to Emaar'])
        percentage_df.loc['Total Requests'] = ['Total Requests',len(list(total_requests)),"-",sum_of_total,round(sum_of_total/len(list(total_requests))),sum_of_total/3.67,"-","-"]
        percentage_df.loc['Auto Approved'] = ['Auto Approved',len(list(auto_approved_1000)),round(len(list(auto_approved_1000))/len(list(total_requests))*100,2),sum_of_auto_1000,round(sum_of_auto_1000/len(list(auto_approved_1000))),sum_of_auto_1000/3.67,"-","-"]
        if len(list(false_positives_1000)) == 0:
            percentage_df.loc['False Positives'] = ['False Postives',0,0,0,0,0,0,0]
        else:
            percentage_df.loc['False Positives'] = ['False Postives',len(list(false_positives_1000)),round(len(list(false_positives_1000))/len(list(auto_approved_1000))*100,2),sum_of_false_1000,round(sum_of_false_1000/len(list(false_positives_1000))),sum_of_false_1000/3.67,sum_of_false_1000/3.67/2,(sum_of_false_1000/3.67/2)*0.02]
        percentage_df_2000 = pd.DataFrame(columns=['Type' ,'Count', 'Percentage', 'Total Amount', 'Average','USD Amount','Estimated Miles','Estimated Cost to Emaar'])
        percentage_df_2000.loc['Total Requests'] = ['Total Requests',len(list(total_requests)),"-",sum_of_total,round(sum_of_total/len(list(total_requests))),sum_of_total/3.67,"-","-"]
        percentage_df_2000.loc['Auto Approved'] = ['Auto Approved',len(list(auto_approved_2000)),round(len(list(auto_approved_2000))/len(list(total_requests))*100,2),sum_of_auto_2000,round(sum_of_auto_2000/len(list(auto_approved_2000))),sum_of_auto_2000/3.67,"-","-"]
        if len(list(false_positives_2000)) == 0:
            percentage_df_2000.loc['False Positives'] = ['False Postives',0,0,0,0,0,0,0]
        else:
            percentage_df_2000.loc['False Positives'] = ['False Postives',len(list(false_positives_2000)),round(len(list(false_positives_2000))/len(list(auto_approved_2000))*100,2),sum_of_false_2000,round(sum_of_false_2000/len(list(false_positives_2000))),sum_of_false_2000/3.67,sum_of_false_2000/3.67/2,(sum_of_false_2000/3.67/2)*0.02]
        percentage_df_4000 = pd.DataFrame(columns=['Type' ,'Count', 'Percentage', 'Total Amount', 'Average','USD Amount','Estimated Miles','Estimated Cost to Emaar'])
        percentage_df_4000.loc['Total Requests'] = ['Total Requests',len(list(total_requests)),"-",sum_of_total,round(sum_of_total/len(list(total_requests))),sum_of_total/3.67,"-","-"]
        percentage_df_4000.loc['Auto Approved'] = ['Auto Approved',len(list(auto_approved_4000)),round(len(list(auto_approved_4000))/len(list(total_requests))*100,2),sum_of_auto_4000,round(sum_of_auto_4000/len(list(auto_approved_4000))),sum_of_auto_4000/3.67,"-","-"]
        if len(list(percentage_df_4000)) == 0:
            percentage_df_4000.loc['False Positives'] = ['False Postives',0,0,0,0,0,0,0]
        else:
            percentage_df_4000.loc['False Positives'] = ['False Postives',len(list(false_positives_4000)),round(len(list(false_positives_4000))/len(list(auto_approved_4000))*100,2),sum_of_false_4000,round(sum_of_false_4000/len(list(false_positives_4000))),sum_of_false_4000/3.67,sum_of_false_4000/3.67/2,(sum_of_false_4000/3.67/2)*0.02]
        percentage_df_6000 = pd.DataFrame(columns=['Type' ,'Count', 'Percentage', 'Total Amount', 'Average','USD Amount','Estimated Miles','Estimated Cost to Emaar'])
        percentage_df_6000.loc['Total Requests'] = ['Total Requests',len(list(total_requests)),"-",sum_of_total,round(sum_of_total/len(list(total_requests))),sum_of_total/3.67,"-","-"]
        percentage_df_6000.loc['Auto Approved'] = ['Auto Approved',len(list(auto_approved_6000)),round(len(list(auto_approved_6000))/len(list(total_requests))*100,2),sum_of_auto_6000,round(sum_of_auto_6000/len(list(auto_approved_6000))),sum_of_auto_6000/3.67,"-","-"]
        if len(list(percentage_df_6000)) == 0:
            percentage_df_6000.loc['False Positives'] = ['False Postives',0,0,0,0,0,0,0]
        else:
            percentage_df_6000.loc['False Positives'] = ['False Postives',len(list(false_positives_6000)),round(len(list(false_positives_6000))/len(list(auto_approved_6000))*100,2),sum_of_false_6000,round(sum_of_false_6000/len(list(false_positives_6000))),sum_of_false_6000/3.67,sum_of_false_6000/3.67/2,(sum_of_false_6000/3.67/2)*0.02]
        percentage_df_10000 = pd.DataFrame(columns=['Type' ,'Count', 'Percentage', 'Total Amount', 'Average','USD Amount','Estimated Miles','Estimated Cost to Emaar'])
        percentage_df_10000.loc['Total Requests'] = ['Total Requests',len(list(total_requests)),"-",sum_of_total,round(sum_of_total/len(list(total_requests))),sum_of_total/3.67,"-","-"]
        percentage_df_10000.loc['Auto Approved'] = ['Auto Approved',len(list(auto_approved_10000)),round(len(list(auto_approved_10000))/len(list(total_requests))*100,2),sum_of_auto_10000,round(sum_of_auto_10000/len(list(auto_approved_10000))),sum_of_auto_10000/3.67,"-","-"]
        if len(list(percentage_df_10000)) == 0:
            percentage_df_10000.loc['False Positives'] = ['False Postives',0,0,0,0,0,0,0]
        else:
            percentage_df_10000.loc['False Positives'] = ['False Postives',len(list(false_positives_10000)),round(len(list(false_positives_10000))/len(list(auto_approved_10000))*100,2),sum_of_false_10000,round(sum_of_false_10000/len(list(false_positives_10000))),sum_of_false_10000/3.67,sum_of_false_10000/3.67/2,(sum_of_false_10000/3.67/2)*0.02]
        logging.info("done with percentage df")
        false_reqs = list(false_positives_1000)
        false_reqs_2000 = list(false_positives_2000)
        false_reqs_4000 = list(false_positives_4000)
        false_reqs_6000 = list(false_positives_6000)
        false_reqs_10000 = list(false_positives_10000)
        
        false_req_df_1000 = pd.DataFrame({'Request IDs':false_reqs})
        false_req_df_2000 = pd.DataFrame({'Request IDs':false_reqs_2000})
        false_req_df_4000 = pd.DataFrame({'Request IDs':false_reqs_4000})
        false_req_df_6000 = pd.DataFrame({'Request IDs':false_reqs_6000})
        false_req_df_10000 = pd.DataFrame({'Request IDs':false_reqs_10000})
        try:
            email_sender.send_email(start_date,df,percentage_df,percentage_df_2000,percentage_df_4000,percentage_df_6000,percentage_df_10000,false_req_df_1000,false_req_df_2000,false_req_df_4000,false_req_df_6000,false_req_df_10000,os.environ['Report_Recipients'])
        except Exception as e:
            logging.error(f"Exception {e}")
    except Exception as e:
        logging.error(f"Main Exception {e}")