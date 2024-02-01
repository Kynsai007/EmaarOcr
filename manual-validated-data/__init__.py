import logging, os, jwt, json, aiohttp, time, ast, requests,uuid
import traceback
from azure.cosmosdb.table.tableservice import TableService
import azure.functions as func
from datetime import datetime
from dateutil import parser
from azure.eventgrid import EventGridClient
from azure.eventgrid.models import EventGridEvent
from msrest.authentication import TopicCredentials

#table_service = TableService(account_name=os.environ['STORAGE_ACCOUNT_NAME'], account_key=os.environ['STORAGE_ACCOUNT_KEY'])
table_service = TableService(account_name='emgtdmubeocr', account_key='sb9vnHXijdtvvk+FMzLSOGeFpBeBkzQc6og3YQoNvOEbIneCzbHO1Z/9zHFe4S+URpc7Yq5vsxISBsHex4YkUQ==')
# credentials = TopicCredentials(
#     os.environ['EVENTGRID_TOPIC_KEY']
# )        

# event_grid_client = EventGridClient(credentials)
async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Manual Validated Data Request  is Received ...')
    algorithm = 'HS256'
    private_key = "s@cr@t"
    try:
        x_api_key = req.headers['x-api-key']
        req_body = req.get_body()
        if x_api_key != "QXUiwGFz4uGVchRP2QHSqY4slwdv3/K6KqHRZrDzX9vYy1GnCJkWag==":
            logging.info('Unauthorized request Received func.')
            data_to_sent = {'code': 1, 'message': 'Unauthorized request Received func.' }
            jwt_token = jwt.encode(data_to_sent, key=private_key, algorithm=algorithm)
            return func.HttpResponse(jwt_token,status_code=200)
    except Exception as e:
        logging.warning("Exception...")
        data_to_sent = {'code': 1, 'message': 'request Received with missing required parameters.' }
        jwt_token = jwt.encode(data_to_sent, key=private_key, algorithm=algorithm)
        return func.HttpResponse(jwt_token,status_code=200)
    
    private_key = "s@cr@t"
    decoded = {"request_id": "00000" }
    try:
        decoded = jwt.decode(req_body,private_key,algorithms = 'HS256')
        logging.info(decoded)
        if decoded['code'] == 0:
            log_app_id = "96e26b32-a8f6-441a-b961-dcf0bf97c13d"
            log_app_key = "plp4i7hpdqzi8nb1y0id5vw0twzck411cfbycn5v"
            log_base_url = "https://api.applicationinsights.io"
            req_id_search = "{'req_id': '"+decoded['request_id']+"'"
            headers = {'x-api-key':log_app_key}
            full_url = log_base_url+"/v1/apps/"+log_app_id+'/query?query=traces | where operation_Name == "WebHook" and  message startswith "'+req_id_search+'"'

            try:
                start_epoch_sec = int(time.time())
                async with aiohttp.ClientSession() as client:
                    async with client.get(full_url,headers=headers) as response:
                        
                        resp = await response.json()
                        end_epoch_sec = int(time.time())
                        result_vectorize = end_epoch_sec - start_epoch_sec
                        
                        if 'error' in resp:
                            result = (False, resp['error'], result_vectorize,'blank')
                        tables = resp['tables']
                        # logging.info(f"ReqID-{decoded['request_id']}|{tables}")
                        if len(tables) >= 1 and len(tables[0]['rows']) >= 1:
                            result = (True, ast.literal_eval(tables[0]['rows'][0][1])['raw_response'], result_vectorize, tables[0]['rows'][0][0])
                        else:
                            result = (False, {'message': 'Data Not Available', 'code': 'DataMissingError'},result_vectorize, 'blank')
            except Exception as e:
                end_epoch_sec = int(time.time())
                result_vectorize = end_epoch_sec - start_epoch_sec
                result = (False, {'message': f'{e}', 'code': 'ServiceMissingError'},result_vectorize, 'blank')
            DS_Info = ""
            CRI_Info = ""
            DS_Timestamp=""
            image_url = ""
            storeMeta = {'name': '', 'confidence': 0, 'from': ''}
            # logging.info(f"ReqID-{decoded['request_id']}|{result}")
            if result[0]:
                DS_Info = result[1]['result']
                DS_Info['invoice_amount'] = result[1]['result']['total_amount']
                DS_Info['has_loyalty_payment'] = "0"
                if result[1]['receipt_status']['has_loyalty_payment'] == True:
                    try:
                        DS_Info['invoice_amount'] = str(float(result[1]['result']['total_amount'])+result[1]['receipt_status']['loyalty_redeemed'])
                    except:
                        DS_Info['invoice_amount'] = '0'
                    DS_Info['has_loyalty_payment'] = "1"
                if result[1]['receipt_status']['has_store_credit_payment'] == True:
                    if DS_Info['has_loyalty_payment'] == "1":
                        try:
                            DS_Info['invoice_amount'] = str(float(DS_Info['invoice_amount'])+result[1]['receipt_status']['store_credit_redeemed'])
                        except:
                            DS_Info['invoice_amount'] = '0'
                        DS_Info['has_loyalty_payment'] = "1"
                    else:
                        try:
                            DS_Info['invoice_amount'] = str(float(result[1]['result']['total_amount'])+result[1]['receipt_status']['store_credit_redeemed'])
                        except:
                            DS_Info['invoice_amount'] = '0'
                        DS_Info['has_loyalty_payment'] = "1"
                DS_Info['is_dubai_mall'] = result[1]['result']['is_in_dubai_mall']        
                DS_Info['eligible_amount'] = result[1]['result']['total_amount']
                if 'auto_approve' in result[1]['receipt_status']:
                    DS_Info['auto_approved'] = result[1]['receipt_status']['auto_approve']
                else:
                    DS_Info['auto_approved'] = False
                DS_Timestamp = parser.parse(result[-1])
                if 'cri_data' in result[1]:
                    CRI_Info = result[1]['cri_data']
                if 'image_url' in result[1]:
                    image_url = result[1]['image_url']
                if 'storeMeta' in result[1]:
                    storeMeta = result[1]['storeMeta']
            entity = {'PartitionKey':'TDM','RowKey': decoded['request_id'],'Exceptions':'','DataReceived':json.dumps(decoded['result']),'DS_Info': json.dumps(DS_Info),'CRI_Info': json.dumps(CRI_Info),'DS_Timestamp': DS_Timestamp ,'image_url': image_url, 'storeMeta': json.dumps(storeMeta)}
            table_service.insert_or_merge_entity('ManualValidatedData',entity)
            # result = []
            # datasent = {'data':decoded,'image_url':image_url}
            # result.append(EventGridEvent(
            #         id=uuid.uuid4(),
            #         subject=os.environ['EVENTGRID_SUBJECT_4'],#f"DocumentClassifier",
            #         data= datasent,
            #         event_type=os.environ['EVENTGRID_EVENT_TYPE_4'],#'DocumentClassifier',
            #         event_time=datetime.now(),
            #         data_version=2.0
            #     ))
            
            # event_grid_client.publish_events(
            #     os.environ['EVENTGRID_ENDPOINT'],
            #     events= result
            # )
        else:
            entity = {'PartitionKey':'TDM','RowKey': decoded['request_id'],'Exceptions':decoded['message'],'DataReceived': ''}
            table_service.insert_or_merge_entity('ManualValidatedData',entity)
        logging.info(f"Success in execution.")
        
        data_to_sent = {'code': 0, 'message': 'OK', 'request_id': decoded['request_id'] }
        jwt_token = jwt.encode(data_to_sent, key=private_key, algorithm=algorithm)
        return func.HttpResponse(jwt_token,status_code=200)
    except Exception as e:
        logging.info(traceback.format_exc())
        exc = f"Exception in manual-validated-data, Request ID: {decoded['request_id']},error: {e}"
        # requests.post(os.environ['EMAIL_EXCEPTION_ENDPOINT'],data=json.dumps({"exception":exc}))
        entity = {'PartitionKey':'TDM','RowKey':decoded['request_id'],'Exceptions':str(e),'DataReceived': str(decoded) }
        table_service.insert_or_merge_entity('ManualValidatedData',entity)
        data_to_sent = {'code': 1, 'message': 'Failure in Saving the data.' }
        jwt_token = jwt.encode(data_to_sent, key=private_key, algorithm=algorithm)
        return func.HttpResponse(jwt_token,status_code=200)
        
    
