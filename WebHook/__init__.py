import json
import requests
import jwt
import logging
import os
import uuid

import azure.functions as func
from azure.cosmosdb.table.tableservice import TableService

table_service = TableService(
    account_name=os.environ['STORAGE_ACCOUNT_NAME'], account_key=os.environ['STORAGE_ACCOUNT_KEY'])


def main(event: func.EventGridEvent):
    result = event.get_json()
    rowkey = str(uuid.uuid4())
    raw_response = result['raw_response']
    try:
        # json.dumps({
        #     'id': event.id,
        #     'data': event.get_json(),
        #     'topic': event.topic,
        #     'subject': event.subject,
        #     'event_type': event.event_type,
        # })
        entity = {'PartitionKey': 'TDM', 'RowKey': rowkey,
                  'RequestID': result['req_id'], 'CurrentStatus': 'Webhook In/ Received', 'Exceptions': '', 'DataSent': '', 'RawDataSent': ''}
        table_service.insert_entity('RequestResponseInfo', entity)

        logging.info(result)
        headers = {
            'x-token': result['x-token']
        }
        resp = requests.post(
            url=result['callback_url'], data=result['webhook_response'], headers=headers,verify=False)
        datatosave = {
            'webhook-resp': result['webhook_response'],
            'result-text': resp.text
        }
        logging.info(
            f'Webhook Response: {result["req_id"]} {resp.text} [status code: {resp.status_code}]')
        algorithm = 'HS256'
        private_key = os.environ['JWT_PRIVATE_KEY']
        try:
            decoded = jwt.decode(resp.text, private_key, algorithms='HS256')
            if 'error_code' in decoded and decoded['error_code'] == 0:
                ent = table_service.get_entity(
                    'RequestResponseInfo', 'TDM', rowkey)
                exception = ent.Exceptions
                entity = {'PartitionKey': 'TDM', 'RowKey': rowkey, 'RequestID': result['req_id'], 'CurrentStatus': 'Webhook Execution Completed', 'Exceptions': exception, 'DataSent': json.dumps(
                    datatosave), 'RawDataSent': json.dumps(raw_response)}
                table_service.merge_entity('RequestResponseInfo', entity)
            else:
                ent = table_service.get_entity(
                    'RequestResponseInfo', 'TDM', rowkey)
                exception = ent.Exceptions + "," + decoded['message']
                entity = {'PartitionKey': 'TDM', 'RowKey': rowkey, 'RequestID': result['req_id'], 'CurrentStatus': f'Webhook Execution Failed', 'Exceptions': exception, 'DataSent': json.dumps(
                    datatosave), 'RawDataSent': json.dumps(raw_response)}
                table_service.merge_entity('RequestResponseInfo', entity)
        except Exception as e:
            logging.info(e)
            if 'Not enough segments' in str(e):
                exc = f"Exception in Webhook, Request ID: {result['req_id']},error: Callback URL returned 500"
            else:
                exc = f"Exception in Webhook, Request ID: {result['req_id']},error: Signture Verification Failed"
            ent = table_service.get_entity(
                'RequestResponseInfo', 'TDM', rowkey)
            emailresp = requests.post(
                os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": exc}))

            exception = ent.Exceptions + ",Signature Verification Failed"
            entity = {'PartitionKey': 'TDM', 'RowKey': rowkey, 'RequestID': result['req_id'], 'CurrentStatus': 'Webhook Execution Failed', 'Exceptions': exception, 'DataSent': json.dumps(
                datatosave), 'RawDataSent': json.dumps(raw_response)}
            table_service.merge_entity('RequestResponseInfo', entity)

    except Exception as e:
        ent = table_service.get_entity('RequestResponseInfo', 'TDM', rowkey)
        exc = f"Exception in Webhook, Request ID: {result['req_id']},error: {e}"
        emailresp = requests.post(
            os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": exc}))
        exception = ent.Exceptions + ',Exception in Webhook'
        entity = {'PartitionKey': 'TDM', 'RowKey': rowkey,
                  'RequestID': result['req_id'], 'CurrentStatus': 'Exception in Webhook', 'Exceptions': exception, 'DataSent': '', 'RawDataSent': ''}
        table_service.merge_entity('RequestResponseInfo', entity)
        logging.info(f"Exception in WebHook {e}")
        if 'Max retries exceeded with url' in str(e) or "('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))" in str(e):
            raise
