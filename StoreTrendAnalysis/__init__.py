import logging
from datetime import datetime, timedelta
from azure.cosmosdb.table.tableservice import TableService
import pandas as pd
import json
import traceback
import uuid
from collections import Counter, OrderedDict
import azure.functions as func

def json_to_series(text, column):
    if type(text) != str:
        text = "{}"
    text = json.loads(text)

    if not text and column == 'storeMeta':
        text = {'name': '', 'confidence': 0, 'from': ''}
    elif not text:
        text = {'total_amount': 0, 'store_name': 'x',
                'date': 'x', 'time': '0', 'invoice_amount': 0, 'eligible_amount': 0, 'has_loyalty_payment': 0}
    keys, values = zip(*[(f"{column}.{dct}", text[dct]) for dct in text])
    return pd.Series(values, index=keys)


def RepresentsInt(s):
    try:
        return int(s.replace(":", ""))
    except:
        return 0

def main(mytimer: func.TimerRequest) -> None:
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s')
    global total_req
    try:
        today = datetime.now()
        update_date = today - timedelta(days=1)
        report_date = update_date.strftime("%Y-%m-%dT00:00:00Z")
        report_date_until = today.strftime("%Y-%m-%dT00:00:00Z")

        table_service = TableService(account_name="emgtdmubeocr",
                                     account_key='sb9vnHXijdtvvk+FMzLSOGeFpBeBkzQc6og3YQoNvOEbIneCzbHO1Z/9zHFe4S+URpc7Yq5vsxISBsHex4YkUQ==')
        table_service1 = TableService(account_name="emgdevtdmubeocr",
                                      account_key='WVCErlxrE2ReNEQ8NizMY2/aZpiaAUO5qAIiGghM+Dpa4pbaVZrAKoY2QSdnxa4HqRQaZTK0+6juGqkKhM6SgA==')

        response = table_service.query_entities('ManualValidatedData',
                                                filter=f"Timestamp ge datetime'{report_date}' and Timestamp le datetime'{report_date_until}'")

        req_list = []
        for req in response:
            req_list.append(req)

        df = pd.DataFrame(req_list)

        result = pd.concat([df, df['DataReceived'].apply(
            json_to_series, args=('DataReceived',))], axis=1)
        result[['DataReceived.total_amount']] = result[['DataReceived.total_amount']].apply(pd.to_numeric,
                                                                                            errors='coerce')
        result = pd.concat([result, result['DS_Info'].apply(
            json_to_series, args=('DS_Info',))], axis=1)
        if 'storeMeta' in result:
            result = pd.concat([result, result['storeMeta'].apply(
                json_to_series, args=('storeMeta',))], axis=1)
            result.drop(['storeMeta', 'storeMeta.name'], axis=1, inplace=True)
        if 'DS_Info.is_in_dubai_mall' in result:
            result.drop(['DS_Info.is_in_dubai_mall'], axis=1, inplace=True)
        result[['DS_Info.total_amount']] = result[['DS_Info.total_amount']].apply(pd.to_numeric, errors='coerce')
        result.drop(['DataReceived', 'DS_Info', 'etag',
                     'Exceptions', 'PartitionKey'], axis=1, inplace=True)

        result['StoreAccuracy'] = result.apply(
            lambda x: 'match' if x['DataReceived.store_name'] == x['DS_Info.store_name'] and x[
                'DataReceived.status'] == 'Accepted' else (
                'miss' if x['DataReceived.store_name'] != x['DS_Info.store_name'] and x[
                    'DataReceived.status'] == 'Accepted' else 'other'), axis=1)

        result['DateAccuracy'] = result.apply(lambda x: 'match' if x['DataReceived.date'] == x['DS_Info.date'] and x[
            'DataReceived.status'] == 'Accepted' else (
            'miss' if x['DataReceived.date'] != x['DS_Info.date'] and x[
                'DataReceived.status'] == 'Accepted' else 'other'),
                                              axis=1)

        result['AmountAccuracy'] = result.apply(
            lambda x: 'match' if x['DataReceived.total_amount'] == x['DS_Info.total_amount'] and x[
                'DataReceived.status'] == 'Accepted' else (
                'miss' if x['DataReceived.total_amount'] != x['DS_Info.total_amount'] and x[
                    'DataReceived.status'] == 'Accepted' else 'other'), axis=1)

        Store_All = list(
            result[(result['DataReceived.status'] == "Accepted")]['DataReceived.store_name'])

        Store_Match = list(
            result[(result['DataReceived.status'] == "Accepted") & (result['StoreAccuracy'] == 'match')][
                'DataReceived.store_name'])
        Store_Miss = list(
            result[(result['DataReceived.status'] == "Accepted") & (result['StoreAccuracy'] == 'miss')][
                'DataReceived.store_name'])

        Amount_Match = list(
            result[(result['DataReceived.status'] == "Accepted") & (result['AmountAccuracy'] == 'match')][
                'DataReceived.store_name'])

        Amount_Miss = list(
            result[(result['DataReceived.status'] == "Accepted") & (result['AmountAccuracy'] == 'miss')][
                'DataReceived.store_name'])

        Date_Match = list(
            result[(result['DataReceived.status'] == "Accepted") & (result['DateAccuracy'] == 'match')][
                'DataReceived.store_name'])
        Date_Miss = list(
            result[(result['DataReceived.status'] == "Accepted") & (result['DateAccuracy'] == 'miss')][
                'DataReceived.store_name'])

        All_Stores_cnt = Counter(Store_All)
        All_Stores_cnt = OrderedDict(sorted(All_Stores_cnt.items()))
        Store_match_cnt = Counter(Store_Match)
        Store_miss_cnt = Counter(Store_Miss)
        Amount_match_cnt = Counter(Amount_Match)
        Amount_miss_cnt = Counter(Amount_Miss)
        Date_match_cnt = Counter(Date_Match)
        Date_miss_cnt = Counter(Date_Miss)

        for store in All_Stores_cnt.keys():
            Rowkkey = str(uuid.uuid4())

            if store in All_Stores_cnt:
                total_req = All_Stores_cnt[store]

            if store in Store_match_cnt:
                Storematch = Store_match_cnt[store]
            else:
                Storematch = 0

            if store in Store_miss_cnt:
                Storemiss = Store_miss_cnt[store]
            else:
                Storemiss = 0

            if store in Amount_match_cnt:
                Amountmatch = Amount_match_cnt[store]
            else:
                Amountmatch = 0

            if store in Amount_miss_cnt:
                Amountmiss = Amount_miss_cnt[store]
            else:
                Amountmiss = 0

            if store in Date_match_cnt:
                Datematch = Date_match_cnt[store]
            else:
                Datematch = 0

            if store in Date_miss_cnt:
                Datemiss = Date_miss_cnt[store]
            else:
                Datemiss = 0

            if Storemiss == 0 and Amountmiss == 0 and Datemiss == 0:
                Store_Success = "Success"
            else:
                Store_Success = "Failed"

            Entity = {'PartitionKey': 'TDM', 'RowKey': Rowkkey, 'Store_Name': store, 'Total_Requests': total_req,
                      'Store_Matchcount': Storematch, 'Store_Misscount': Storemiss, 'Amount_Matchcount': Amountmatch,
                      'Amount_Misscount': Amountmiss, 'Date_Macthcount': Datematch, 'Date_Misscount': Datemiss,
                      'Date': report_date, 'Store_Success': Store_Success}

            table_service1.insert_entity('StoreTrendAnalysis', Entity)

        logging.info("Successfully saved data to db")

    except:
        logging.info("Error",traceback.format_exc())
