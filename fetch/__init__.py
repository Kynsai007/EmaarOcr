import logging
import os
import json,requests,ast

import azure.functions as func

from azure.cosmosdb.table.tableservice import TableService

table_service = TableService(
    account_name=os.environ['STORAGE_ACCOUNT_NAME'], account_key=os.environ['STORAGE_ACCOUNT_KEY'])

app_id = os.environ['APP_INSIGHTS_ID'] 
key = os.environ['APP_INSIGHTS_KEY']
base_url = os.environ['APP_INSIGHTS_URL']

def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    logging.info('Received func request.')
    try:
        # Check for Authorization
        try:
            x_api_key = req.headers['x-api-key']
            if x_api_key != os.environ['X_API_KEY']:
                logging.info('Unauthorized request Received func.')
                return func.HttpResponse("", status_code=401)
        except Exception as e:
            logging.info(f'Exception in func.{e}')
            return func.HttpResponse("", status_code=401)

        # Check for parameter check
        try:
            req_id = req.params.get('req_id')
            #track = req.params.get('track')
            # ent = table_service.get_entity('RequestResponseInfo','TDM')
            # tasks = table_service.query_entities(
            #     'RequestResponseInfo', filter=f"RequestID eq '{req_id}'", select='CurrentStatus,Timestamp,DataSent')
            # task_list = []
            # for t in tasks:
            #     task_list.append(dict(t))
            # task_list.sort(key=lambda item: item['Timestamp'], reverse=True)
            headers = {'x-api-key':key}
            status = ""
            data = ""
            message = ""
            search_str = "{'req_id': '"+req_id+"'"
            full_url = base_url+'/v1/apps/'+app_id+'/query?query=traces | where operation_Name == "WebHook" and message startswith "'+search_str+'" and timestamp >= ago(14d)'
            resp = requests.get(full_url,headers=headers)
            result = resp.json()
            try:
                tables = result['tables']
                rows = tables[0]['rows']
            except:
                tables = []
                rows = []
            row_found = False
            if len(rows) > 0:
                json_results = ast.literal_eval(rows[0][1])
                if json_results['req_id'] == req_id:
                    data = json_results['webhook_response']
                    status = "delivered"
                    message = "request received and previously sent"
                    row_found = True
            if not row_found:
                search_str = '{"req_id": "'+req_id+'"'
                full_url = base_url+"/v1/apps/"+app_id+"/query?query=traces | where operation_Name == 'PrebuiltModel' and message contains '"+search_str+"' and timestamp >= ago(14d)"
                resp = requests.get(full_url,headers=headers)
                result = resp.json()
                try:
                    tables = result['tables']
                    rows = tables[0]['rows']
                except:
                    tables = []
                    rows = []
                if len(rows) > 0:
                    if 'Received:' in rows[0][1]:
                        message = "request received (entry-mode) and yet to complete"
                        status = "delivering"
                        row_found = True
            if not row_found:
                search_str = "Data in Business Logic {'req_id': '"+req_id+"'"
                full_url = base_url+'/v1/apps/'+app_id+'/query?query=traces | where operation_Name == "BusinessLogic" and message startswith "'+search_str+'" and timestamp >= ago(14d)'
                resp = requests.get(full_url,headers=headers)
                result = resp.json()
                try:
                    tables = result['tables']
                    rows = tables[0]['rows']
                except:
                    tables = []
                    rows = []
                if len(rows) > 0:
                    message = "request received and ready to be sent"
                    status = "ready_to_deliver"
                    row_found = True
            if not row_found:
                status = "failed_to_deliver"
                data = ""
                message = "request not received in the last 14 days"
            # for task in task_list:
            #     if task['CurrentStatus'].startswith("Webhook Execution"):
            #         status = "delivered"
            #         message = "request received and previously sent"
            #         if 'webhook-resp' in json.loads(task['DataSent']):
            #             data = json.loads(task['DataSent'])['webhook-resp']
            #         break
            # else:
            #     if len(task_list) > 0:
            #         if task_list[0]['CurrentStatus'] != "Webhook In/ Received" and task_list[0]['CurrentStatus'] != "Webhook Execution Completed":
            #             message = "request received and not yet ready to be sent"
            #             status = "not_ready"
            #         if task_list[0]['CurrentStatus'] == "PrebuiltModel In/ Received" or task_list[0]['CurrentStatus'] == "PrebuiltModel Out/ Sent" or task_list[0]['CurrentStatus'] == "DefaultOCRResponse In/ Received" or task_list[0]['CurrentStatus'] == "DefaultOCRResponse Out/ Sent" or task_list[0]['CurrentStatus'] == "BusinessLogic In/ Received" or task_list[0]['CurrentStatus'] == "BusinessLogic Out/ Received":
            #             message = "request received (entry-mode) and yet to complete"
            #             status = "delivering"
            #         if task_list[0]['CurrentStatus'] == "Webhook In/ Received":
            #             message = "request received and ready to be sent"
            #             status = "ready_to_deliver"
            #             if 'webhook-resp' in json.loads(task_list[0]['DataSent']):
            #                 data = json.loads(task_list[0]['DataSent'])[
            #                     'webhook-resp']
            #         if task_list[0]['CurrentStatus'] == "Webhook Execution Completed":
            #             status = "delivered"
            #             message = "request received and previously sent"
            #             if 'webhook-resp' in json.loads(task_list[0]['DataSent']):
            #                 data = json.loads(task_list[0]['DataSent'])[
            #                     'webhook-resp']
            #     else:
            #         status = "failed_to_deliver"
            #         data = None
            #         message = "request not received in the last 3 days"
            return func.HttpResponse(json.dumps(
                {
                    "error": 0,
                    "result": {
                        "status": status,
                        "message": message,
                        "data": data
                    }
                })
            )
        except Exception as e:
            logging.info(f"Exception in fetch api {str(e)}")
            status = "exception"
            data = None
            message = "Server Side Exception"
            return func.HttpResponse(json.dumps(
                {
                    "error": -1,
                    "result": {
                        "status": status,
                        "message": message,
                        "data": data
                    }
                })
            )

    except Exception as e:
        return func.HttpResponse("Invalid Request", status_code=400)
