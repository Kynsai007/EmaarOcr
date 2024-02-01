import logging,traceback
import re

from azure.cosmosdb.table.tableservice import TableService
from datetime import datetime, timedelta, timezone
import json
import os
import math

import pandas as pd
from io import BytesIO
import azure.functions as func
import aiohttp

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from . import email_sender

from azure.storage.blob import ContainerClient


def json_to_series(text, column):
    if type(text) != str:
        text = "{}"
    text = json.loads(text)

    if not text and column == 'storeMeta':
        text = {'name': '', 'confidence': 0, 'from': ''}
    elif not text and column == 'DS_Info':
        text = {'total_amount': 0, 'store_name': 'x',
                'date': 'x', 'time': '0','invoice_amount':0,'eligible_amount':0,'has_loyalty_payment':"0",'auto_approved':False}
    elif not text:
        text = {'total_amount': 0, 'store_name': 'x',
                'date': 'x', 'time': '0','invoice_amount':0,'eligible_amount':0,'has_loyalty_payment':"0"}
    keys, values = zip(*[(f"{column}.{dct}",text[dct]) for dct in text])
    return pd.Series(values, index=keys)


def RepresentsInt(s):
    try:
        return int(s.replace(":", ""))
    except:
        return 0


def special_strftime(el, dic={'1': 'st', '2': 'nd', '3': 'rd'}):
    x = el.strftime('%Y,%b %d')
    end = 'th' if x[-2] == '1' else dic.get(x[-1], 'th')
    return x + end


async def main(mytimer: func.TimerRequest) -> None:
    try:
        utc_timestamp = datetime.utcnow().replace(
            tzinfo=timezone.utc).isoformat()

        if mytimer.past_due:
            logging.info('The timer is past due!')
        table_service = TableService(
            account_name=os.environ['STORAGE_ACCOUNT_NAME'], account_key=os.environ['STORAGE_ACCOUNT_KEY'])
        today = datetime.now()
        update_date = today - timedelta(days=1)
        report_date = update_date.strftime("%Y-%m-%dT00:00:00Z")
        report_date_until = today.strftime("%Y-%m-%dT00:00:00Z")

        # New LOgic and report
        mv_reqs_resp = table_service.query_entities(
            'ManualValidatedData', filter=f"Timestamp ge datetime'{report_date}' and Timestamp le datetime'{report_date_until}'")

        req_list = []
        for req in mv_reqs_resp:
            req_list.append(req)

        df = pd.DataFrame(req_list)

        result = pd.concat([df, df['DataReceived'].apply(
            json_to_series, args=('DataReceived',))], axis=1)
        result[['DataReceived.total_amount']] = result[['DataReceived.total_amount']].apply(pd.to_numeric,errors='coerce')
        result[['DataReceived.invoice_amount']] = result[['DataReceived.invoice_amount']].apply(pd.to_numeric,errors='coerce')
        result[['DataReceived.eligible_amount']] = result[['DataReceived.eligible_amount']].apply(pd.to_numeric,errors='coerce')
        result = pd.concat([result, result['DS_Info'].apply(
            json_to_series, args=('DS_Info',))], axis=1)
        result[['DS_Info.invoice_amount']] = result[['DS_Info.invoice_amount']].apply(pd.to_numeric,errors='coerce')
        result[['DS_Info.eligible_amount']] = result[['DS_Info.eligible_amount']].apply(pd.to_numeric,errors='coerce')
        if 'storeMeta' in result:
            result = pd.concat([result, result['storeMeta'].apply(
                json_to_series, args=('storeMeta',))], axis=1)
            result.drop(['storeMeta', 'storeMeta.name'], axis=1, inplace=True)
        if 'DS_Info.is_in_dubai_mall' in result:
            result.drop(['DS_Info.is_in_dubai_mall'], axis=1, inplace=True)
        result[['DS_Info.total_amount']] = result[['DS_Info.total_amount']].apply(pd.to_numeric,errors='coerce')
        result.drop(['DataReceived', 'DS_Info', 'etag',
                    'Exceptions', 'PartitionKey'], axis=1, inplace=True)
        result.fillna("",inplace=True)
        result['StoreAccuracy'] = result.apply(lambda x: 'match' if  re.sub("[^A-Za-z0-9]+","", x['DataReceived.store_name']).lower() ==  re.sub("[^A-Za-z0-9]+","", x['DS_Info.store_name']).lower() and x['DataReceived.status'] == 'Accepted' or (x['DataReceived.reason'].lower() == 'receipt is approved by ocr.' if x['DataReceived.reason'] is not None else "") else (
            'miss' if re.sub("[^A-Za-z0-9]+", " ", x['DataReceived.store_name']).lower() != re.sub("[^A-Za-z0-9]+", " ", x['DS_Info.store_name']).lower() and x['DataReceived.status'] == 'Accepted' else 'other'), axis=1)

        result['DateAccuracy'] = result.apply(lambda x: 'match' if x['DataReceived.date'] == x['DS_Info.date'] and x['DataReceived.status'] == 'Accepted' or (x['DataReceived.reason'].lower() == 'receipt is approved by ocr.' if x['DataReceived.reason'] is not None else "") else (
            'miss' if x['DataReceived.date'] != x['DS_Info.date'] and x['DataReceived.status'] == 'Accepted' else 'other'), axis=1)

        result['AmountAccuracy'] = result.apply(lambda x: 'match' if x['DataReceived.total_amount'] == x['DS_Info.total_amount'] and x['DataReceived.status'] == 'Accepted' or (x['DataReceived.reason'].lower() == 'receipt is approved by ocr.' if x['DataReceived.reason'] is not None else "") else (
            'miss' if x['DataReceived.total_amount'] != x['DS_Info.total_amount'] and x['DataReceived.status'] == 'Accepted' else 'other'), axis=1)

        result['TimeAccuracy'] = result.apply(lambda x: 'match' if RepresentsInt(x['DataReceived.time']) == RepresentsInt(x['DS_Info.time']) and x['DataReceived.status'] == 'Accepted' or (x['DataReceived.reason'].lower() == 'receipt is approved by ocr.' if x['DataReceived.reason'] is not None else "") else (
            'miss' if x['DataReceived.time'] != x['DS_Info.time'] and x['DataReceived.status'] == 'Accepted' else 'other'), axis=1)

        result['LoyaltyPaymentAccuracy'] = result.apply(lambda x: 'match' if x['DataReceived.has_loyalty_payment'] == x['DS_Info.has_loyalty_payment'] and x['DataReceived.status'] == 'Accepted' or (x['DataReceived.reason'].lower() == 'receipt is approved by ocr.' if x['DataReceived.reason'] is not None else "") else (
            'miss' if x['DataReceived.has_loyalty_payment'] != x['DS_Info.has_loyalty_payment'] and x['DataReceived.status'] == 'Accepted' else 'other'), axis=1)

        result['TotalAccuracy'] = result.apply(lambda x: 'match' if x['StoreAccuracy'] == 'match' and x['DateAccuracy'] == 'match' and x['AmountAccuracy'] == 'match' else (
            'other' if x['StoreAccuracy'] == 'other' or x['DateAccuracy'] == 'other' or x['AmountAccuracy'] == 'other' else 'miss'), axis=1)

        result['Timestamp'] = result['Timestamp'].dt.tz_localize(None)
        result['DS_Timestamp'] = df['DS_Timestamp'].apply(
            lambda a: pd.to_datetime(a).date())

        All_Request = list(
            result['DataReceived.store_name'])
        Store_All = list(
            result[(result['DataReceived.status'] == "Accepted")]['DataReceived.store_name'])
        Store_Unique = result[(result['DataReceived.status'] == "Accepted")
                              ]['DataReceived.store_name'].unique().tolist()

        Store_Match = list(
            result[(result['StoreAccuracy'] == "match")]['DataReceived.store_name'])
        Store_Miss = list(
            result[(result['StoreAccuracy'] == "miss")]['DataReceived.store_name'])

        Amount_Match = list(
            result[(result['AmountAccuracy'] == "match")]['DataReceived.store_name'])
        Amount_Miss = list(
            result[(result['AmountAccuracy'] == "miss")]['DataReceived.store_name'])

        Date_Match = list(
            result[(result['DateAccuracy'] == "match")]['DataReceived.store_name'])
        Date_Miss = list(
            result[(result['DateAccuracy'] == "miss")]['DataReceived.store_name'])

        Time_Match = list(
            result[(result['TimeAccuracy'] == "match")]['DataReceived.store_name'])
        Time_Miss = list(
            result[(result['TimeAccuracy'] == "miss")]['DataReceived.store_name'])
        
        Loyalty_Match = list(
            result[(result['LoyaltyPaymentAccuracy'] == "match")]['DataReceived.store_name'])

        All_Match = list(result[(result['StoreAccuracy'] == "match") &
                                (result['AmountAccuracy'] == "match") &
                                (result['DateAccuracy'] == "match")]['DataReceived.store_name'])

        store_df = pd.DataFrame(
            columns=['Store', 'AllCount', 'MatchCount', 'MissCount'])
        amount_df = pd.DataFrame(
            columns=['Store', 'AllCount', 'MatchCount', 'MissCount'])
        date_df = pd.DataFrame(
            columns=['Store', 'AllCount', 'MatchCount', 'MissCount'])
        time_df = pd.DataFrame(
            columns=['Store', 'AllCount', 'MatchCount', 'MissCount'])
        for elm in Store_Unique:
            if elm is not None:
                store_df.loc[elm] = [elm, Store_All.count(
                    elm), Store_Match.count(elm), Store_Miss.count(elm)]
                amount_df.loc[elm] = [elm, Store_All.count(
                    elm), Amount_Match.count(elm), Amount_Miss.count(elm)]
                date_df.loc[elm] = [elm, Store_All.count(
                    elm), Date_Match.count(elm), Date_Miss.count(elm)]
                time_df.loc[elm] = [elm, Store_All.count(
                    elm), Time_Match.count(elm), Time_Miss.count(elm)]
        # print(len(store_df))
        store_df['%Miss'] = store_df.apply(
            lambda x: math.ceil(x.MissCount/x.AllCount*100), axis=1)
        amount_df['%Miss'] = amount_df.apply(
            lambda x: math.ceil(x.MissCount/x.AllCount*100), axis=1)
        date_df['%Miss'] = date_df.apply(
            lambda x: math.ceil(x.MissCount/x.AllCount*100), axis=1)
        time_df['%Miss'] = time_df.apply(
            lambda x: math.ceil(x.MissCount/x.AllCount*100), axis=1)
        store_df.sort_values(by=['%Miss'], ascending=False, inplace=True)
        amount_df.sort_values(by=['%Miss'], ascending=False, inplace=True)
        date_df.sort_values(by=['%Miss'], ascending=False, inplace=True)
        time_df.sort_values(by=['%Miss'], ascending=False, inplace=True)

        accuracy_df = pd.DataFrame(columns=['Type', 'Accuracy'])
        accuracy_df.loc['All'] = ['All', str(math.ceil(
            (len(All_Match)/len(Store_All)*100)))+" %"]
        accuracy_df.loc['Store'] = ['Store', str(math.ceil(
            (len(Store_Match)/len(Store_All)*100)))+" %"]
        accuracy_df.loc['Date'] = ['Date', str(math.ceil(
            (len(Date_Match)/len(Store_All)*100)))+" %"]
        accuracy_df.loc['Amount'] = ['Amount', str(math.ceil(
            (len(Amount_Match)/len(Store_All)*100)))+" %"]
        accuracy_df.loc['Time'] = ['Time', str(math.ceil(
            (len(Time_Match)/len(Store_All)*100)))+" %"]
        accuracy_df.loc['LoyaltyPayment'] = ['LoyaltyPayment',str(math.ceil(
            (len(Loyalty_Match)/len(Store_All)*100)))+" %"]


        # Get Store to be sent by Emaar
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={os.environ['STORAGE_ACCOUNT_NAME']};AccountKey={os.environ['STORAGE_ACCOUNT_KEY']};EndpointSuffix=core.windows.net"
        container_name = "traininginvoices"
        container = ContainerClient.from_connection_string(
            conn_str=connection_string, container_name=container_name)
        blob_list_clean = []
        for elm in container.list_blobs():
            folder_exists = elm['name'].split(
                ".")[-1] in ['png', 'jpg', 'jpeg', 'pdf']
            folder_name = elm['name'].split("/")[0].lower().strip()
            if folder_exists and folder_name not in blob_list_clean:
                blob_list_clean.append(folder_name)
        stores = table_service.query_entities(
            'storelist')
        store_list_clean = [re.sub(
            "[^A-Za-z0-9]+", " ", store['StoreName']).lower().strip() for store in stores]
        Emaar_Missing = list(set(store_list_clean)-set(blob_list_clean))
        df_2 = pd.DataFrame(Emaar_Missing, columns=["StoreName"])
        # Get Store to be Tagged by DS
        models = table_service.query_entities(
            os.environ['FORM_RECOGNIZER_MODELS'])
        model_list_clean = [re.sub(
            "[^A-Za-z0-9]+", " ", model['ActualStoreName']).lower().strip() for model in models]
        DS_Missing = list(set(blob_list_clean)-set(model_list_clean))
        df_3 = pd.DataFrame(DS_Missing, columns=["StoreName"])
        # FILE TO SEND AND ITS PATH
        email_sender.send_email(update_date,All_Request,Store_All,All_Match,Store_Match,Date_Match,Amount_Match,os.environ['Report_Recipients'],result,accuracy_df,store_df,amount_df,date_df,time_df,df_2,df_3)
        logging.info('Daily Report timer trigger function ran at %s ',
                     utc_timestamp)
    except Exception as e:
        exc = f"Exception in PreviousDayReport  error: {e}"
        async with aiohttp.ClientSession() as client:
            async with client.post(os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": exc})) as response:
                await response.text()
        logging.info(f"{traceback.format_exc()}")
