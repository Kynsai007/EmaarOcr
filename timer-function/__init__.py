import datetime
import logging
import os
import azure.functions as func
import requests,json, ast
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
from requests.api import head

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    accountname = os.environ['STORAGE_ACCOUNT_NAME']#emgtdmubeocr
    accountkey = os.environ['STORAGE_ACCOUNT_KEY']#'sb9vnHXijdtvvk+FMzLSOGeFpBeBkzQc6og3YQoNvOEbIneCzbHO1Z/9zHFe4S+URpc7Yq5vsxISBsHex4YkUQ=='
    table_service = TableService(account_name=accountname, account_key=accountkey)
    tablename = os.environ['STORAGE_TABLE_NAME']#'storelist'
    headers = {
        'X-Token':os.environ['EMAAR_X_TOKEN']#'DS869133AMGH0OLBF988TDAPLGFLL'
    }
    try:
        resp = requests.get(os.environ['EMMAR_INACTIVESTORE_ENDPOINT'],headers,verify=False)
        data = resp.json()
        for obj in data:
            try:
                now = datetime.datetime.now()
                store_name = obj['name'].replace("'","''")
                store_name = store_name.replace("\"","")
                past_syns = "[]"
                storename_in_receipt = store_name
                try:
                    stores = table_service.get_entity(tablename,'TDM',str(obj['id']))
                    past_syns = stores.synonyms
                    if stores.StoreNameInReceipt != "":
                        storename_in_receipt = stores.StoreNameInReceipt
                except Exception as e:
                    if 'ResourceNotFound' in str(e):
                        past_syns = "[]"
                        storename_in_receipt = store_name
                try:
                    table_service.delete_entity(tablename,'TDM',str(obj['id']))
                except:
                    logging.info(f"obj id {obj['id']}: {obj['name']} does not exists")
                    pass
                obj_to_insert = {'PartitionKey': 'TDM', 'RowKey': str(obj['id']),'StoreName':obj['name'].replace("\"",""),'updateDate':now,"synonyms": str(past_syns),"StoreNameInReceipt":storename_in_receipt,"formalName":storename_in_receipt}
                table_service.insert_or_merge_entity(tablename, obj_to_insert)
            except Exception as ex:
                logging.error(f"Failed to insert object {obj['id']} due to {ex}")

        resp = requests.get(os.environ['EMMAR_ACTIVESTORE_ENDPOINT'],headers,verify=False)#'https://azuremcp.thedubaimall.com/api/v5/stores'
        data = resp.json()
        for obj in data:
            try:
                now = datetime.datetime.now()
                store_name = obj['name']
                formal_name = obj['formalName']
                if store_name:
                    store_name = store_name.replace("'","''")
                else:
                    store_name = obj['name'].replace("'","''")
                store_name = store_name.replace("\"","")
                past_syns = "[]"
                storename_in_receipt = store_name
                try:
                    stores = table_service.get_entity(tablename,'TDM',str(obj['id']))
                    past_syns = stores.synonyms
                except Exception as e:
                    if 'ResourceNotFound' in str(e):
                        past_syns = "[]"
                        storename_in_receipt = store_name
                try:
                    table_service.delete_entity(tablename,'TDM',str(obj['id']))
                except:
                    logging.info(f"obj id {obj['id']}: {obj['name']} does not exists")
                    pass
                obj_to_insert = {'PartitionKey': 'TDM', 'RowKey': str(obj['id']),'StoreName':store_name,'unitNumber': obj['unitNumber'], 'StoreEmail': obj['email'],'StorePhone':obj['phone'],'giftCard':obj['giftCard'],'updateDate':now,"synonyms": str(past_syns),"StoreNameInReceipt":storename_in_receipt,"formalName":formal_name}
                table_service.insert_or_merge_entity(tablename, obj_to_insert)
            except Exception as ex:
                logging.error(f"Failed to insert object {obj['id']} due to {ex}")
                        
    except Exception as e:
        logging.error(f"Failed to fetch stores due to {e}")
