import json
import logging
import math
import sys
import uuid
import requests
import time
import os
import jwt

import azure.functions as func

from PyPDF2 import PdfFileReader, PdfFileWriter
from io import BytesIO
from PIL import Image
from msrest.authentication import TopicCredentials
from azure.eventgrid import EventGridClient
from azure.eventgrid.models import EventGridEvent
from azure.cosmosdb.table.tableservice import TableService

import datetime
import uuid

table_service = TableService(
    account_name=os.environ['STORAGE_ACCOUNT_NAME'], account_key=os.environ['STORAGE_ACCOUNT_KEY'])

logging.getLogger("azure.cosmosdb").setLevel(logging.WARNING)
credentials = TopicCredentials(
    # 'XSTPJTytz2rFkp0hIAaONieh5xqDeD/+H6noKFR4sjg='
    os.environ['EVENTGRID_TOPIC_KEY']
)

event_grid_client = EventGridClient(credentials)

accepted_inch = 10*72
accepted_pixel_max = 8000
accepted_pixel_min = 50
accepted_filesize_max = 50


def pre_process(file_name, file_bytes, file_type):
    global accepted_inch, accepted_pixel_max, accepted_pixel_min, accepted_filesize_max
    if file_type == "pdf":
        pdf = PdfFileReader(file_bytes, strict=False)
        dimention = pdf.getPage(0).mediaBox
        writer = PdfFileWriter()
        num_pages = pdf.getNumPages()
        if num_pages > 10:
            num_pages = 10
            logging.info(f"Document has more than 10 pages, File name {file_name}, Resetting to 10 page")
            return False,""
        for page_no in range(num_pages):
            page = pdf.getPage(page_no)
            if max(dimention[2], dimention[3]) > accepted_inch:
                logging.info(f"Resizing Pdf {file_name} - Page {page_no+1}")
                page.scaleBy(
                    accepted_inch/max(int(dimention[2]), int(dimention[3])))
            writer.addPage(page)

        tmp = BytesIO()
        writer.write(tmp)
        data = tmp.getvalue()
    else:
        img = Image.open(file_bytes)
        w, h = img.size
        if w <= accepted_pixel_min or h <= accepted_pixel_min:
            # Discard this due to low quality
            logging.error(f'Discard {file_name} due to low quality.')
            return False, f"File is below {accepted_pixel_min}"
        elif w >= accepted_pixel_max or h >= accepted_pixel_max:
            ''' # resize image
            '''
            logging.info(f'Resize the Image. {file_name}')
            factor = accepted_pixel_max/max(img.size[0], img.size[1])
            img.thumbnail(
                (int(img.size[0]*factor), int(img.size[1]*factor)), Image.ANTIALIAS)

        byte_io = BytesIO()
        format = 'PNG' if file_type == "png" else 'JPEG'
        img.save(byte_io, format)
        data = byte_io.getvalue()
    # Upload to Blob

    ''' If Filesize is greater than prescribed reject'''
    if math.ceil(sys.getsizeof(data)/1024/1024) >= accepted_filesize_max:
        logging.error(f"Filesize for {file_name} is more..")
        return False, f"File Size is above {accepted_filesize_max}"
    return True, data


def main(event: func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('Received: %s', result)
    prev_data = event.get_json()
    rowkey = str(uuid.uuid4())
    DataSent_ImageUrl = {
        'image_url': prev_data['image_url']
    }
    entity = {'PartitionKey': 'TDM', 'RowKey': rowkey,
              'RequestID': prev_data['req_id'], 'CurrentStatus': 'PrebuiltModel In/ Received', 'Exceptions': '', 'DataSent': str(DataSent_ImageUrl), 'RawDataSent': ''}
    table_service.insert_entity('RequestResponseInfo', entity)
    try:
        encrypted = prev_data['encrypted']
        image_url = prev_data['image_url']
        file_name = image_url.split("?")[0].split("/")[-1]
        headers = {
            'Content-Type': 'application/json',
            'Ocp-Apim-Subscription-Key': os.environ['OCP_APIM_SUBSCRIPTION_KEY']
        }
        ext = file_name.split(".")[-1].lower()
        if encrypted:
            ext = "enc"
        if ext == "pdf":
            headers['Content-Type'] = "application/pdf"
        elif ext == "jpeg" or ext == "jpg":
            headers['Content-Type'] = 'image/jpeg'
        elif ext == "png":
            headers['Content-Type'] = 'image/png'
        else:
            headers['Content-Type'] = 'application/json'
        res = requests.get(image_url)
        body = BytesIO(res.content)
        if ext != "enc":
            status, data = pre_process(file_name, body, ext)
        else:
            status = True
            data = json.dumps({"source":image_url})
        if status:
            # https://emrreceiptocr.cognitiveservices.azure.com,v2.1-preview.2,receipt
            url = f"{os.environ['FORM_RECOGNIZER_URL']}/formrecognizer/{os.environ['FORM_RECOGNIZER_VERSION']}/prebuilt/{os.environ['FORM_RECOGNIZER_DOCTYPE']}/analyze?includeTextDetails=true"

            r_status = False
            while not r_status:
                r = requests.post(url, headers=headers, data=data)
                if r.status_code == 202:
                    r_status = True
                elif 'error' in r.json():
                    logging.info(
                        f"Rate Limit Exceeded Waiting ... {r.json()['error']['message']}")
                    if 'code' in r.json()['error']:
                        if r.json()['error']['code'] == "429":
                            time.sleep(2)
                        else:
                            logging.info(
                                f"Error in prebuilt model status code {r.json()['error']['code']}")
                            error_type = r.json()['error']['code']
                            error_message = r.json()['error']['message']
                            raise Exception(error_type, error_message)

            geturl = r.headers["operation-location"]
            status = "notStarted"

            while status != "succeeded":
                r2 = requests.get(geturl, headers=headers)
                result = r2.json()
                if r2.status_code in (404, 500, 503):
                    logging.warning(
                        f"Form Recognizer Failure :- {result['error']['message']}")
                    error_type = result['error']['code']
                    error_message = result['error']['message']
                    raise Exception(error_type, error_message)
                if r2.status_code == 200:
                    if result['status'] == 'failed':
                        logging.warning(
                            f"Form Recognizer Failure :- {result['analyzeResult']['errors'][0]['message']}")
                        error_type = result['analyzeResult']['errors'][0]['code']
                        error_message = result['analyzeResult']['errors'][0]['message']
                        raise Exception(error_type, error_message)
                    else:
                        status = r2.json()['status']
                else:
                    if result['error']['code'] == "429":
                        time.sleep(2)
                    else:
                        raise Exception("Unknown Error", r2.content)
            logging.info(
                f"Form Recognizer GET Results succeeded for {prev_data['req_id']} ")
            documentResults = r2.json()["analyzeResult"]["documentResults"]
            default_fields = {'MerchantName': '', 'MerchantNameConfidence': '', 'TransactionDate': '', 'TransactionDateConfidence': '',
                              'TransactionTime': '', 'TransactionTimeConfidence': '', 'Total': '', 'TotalConfidence': ''}

            for item in documentResults:
                fields = item['fields']
                if "MerchantName" in fields:
                    if "text" in fields['MerchantName']:
                        default_fields['MerchantName'] = fields['MerchantName']['text']
                        default_fields['MerchantNameConfidence'] = fields['MerchantName']['confidence']
                if "TransactionDate" in fields:
                    if "text" in fields['TransactionDate']:
                        default_fields['TransactionDate'] = fields['TransactionDate']['text']
                        default_fields['TransactionDateConfidence'] = fields['TransactionDate']['confidence']
                if "TransactionTime" in fields:
                    if "text" in fields['TransactionTime']:
                        default_fields['TransactionTime'] = fields['TransactionTime']['text']
                        default_fields['TransactionTimeConfidence'] = fields['TransactionTime']['confidence']
                if "Total" in fields:
                    if "text" in fields['Total']:
                        default_fields['Total'] = fields['Total']['text']
                        default_fields['TotalConfidence'] = fields['Total']['confidence']

            logging.info(f"Default Fields {default_fields}")
            ocr_text = "#splitter#".join([k["text"].replace('"', '') for i in r2.json()[
                                         "analyzeResult"]['readResults'] if 'lines' in i for k in i['lines']])
            # Call the event GRID
            dataSent = {
                'req_id': prev_data['req_id'],
                'default_fields': default_fields,
                'encrypted':encrypted,
                'ocr_text': ocr_text,
                'callback_url': prev_data['callback_url'],
                'x-token': prev_data['x-token'],
                'image_url': prev_data['image_url'],
                'cri_data': prev_data.get('cri_data', {})
            }

            logging.info(
                f"Sending Byte Size {sys.getsizeof(json.dumps(dataSent))}")
            logging.info(
                f"Sending KB datasize {sys.getsizeof(json.dumps(dataSent))/1024}")

            ent = table_service.get_entity(
                'RequestResponseInfo', 'TDM', str(rowkey))
            exception = ent.Exceptions
            entity = {'PartitionKey': 'TDM', 'RowKey': str(
                rowkey), 'RequestID': prev_data['req_id'], 'CurrentStatus': 'PrebuiltModel Out/ Sent', 'Exceptions': exception, 'DataSent': json.dumps(dataSent), 'RawDataSent': ''}
            if sys.getsizeof(json.dumps(dataSent))/1024 <= 30:
                table_service.merge_entity('RequestResponseInfo', entity)
            result = []

            for i in range(1):
                result.append(EventGridEvent(
                    id=uuid.uuid4(),
                    # f"DefaultOCRResponse Completed",
                    subject=os.environ['EVENTGRID_SUBJECT_1'],
                    data=dataSent,
                    # 'DefaultOCRResponse',
                    event_type=os.environ['EVENTGRID_EVENT_TYPE_1'],
                    event_time=datetime.datetime.now(),
                    data_version=2.0
                ))
            event_grid_client.publish_events(
                os.environ['EVENTGRID_ENDPOINT'],
                events=result
            )
        else:
            webhook_response = {
                "code": 0,
                "message": "OK",
                "request_id": prev_data['req_id'],
                "result": {
                    "error_code": 0,
                    "error_message": str(data)
                }
            }
            raw_response = {
                "request_id": prev_data['req_id'],
                "result": {
                    "error_code": 0,
                    "error_message": str(data)
                },
                'cri_data': prev_data.get('cri_data', {})
            }
            algorithm = 'HS256'
            private_key = os.environ['JWT_PRIVATE_KEY']
            jwt_token = jwt.encode(
                webhook_response, key=private_key, algorithm=algorithm)
            datatosend = {
                'req_id': prev_data['req_id'],
                'webhook_response': jwt_token,
                'raw_response': raw_response,
                'callback_url': prev_data['callback_url'],
                'x-token': prev_data['x-token']
            }
            result = []
            for i in range(1):
                result.append(EventGridEvent(
                    id=uuid.uuid4(),
                    # f"DefaultOCRResponse Completed",
                    subject=os.environ['EVENTGRID_SUBJECT_3'],
                    data=datatosend,
                    # 'DefaultOCRResponse',
                    event_type=os.environ['EVENTGRID_EVENT_TYPE_3'],
                    event_time=datetime.datetime.now(),
                    data_version=2.0
                ))
            event_grid_client.publish_events(
                os.environ['EVENTGRID_ENDPOINT'],
                events=result
            )
            ent = table_service.get_entity(
                'RequestResponseInfo', 'TDM', rowkey)
            exception = ent.Exceptions
            entity = {'PartitionKey': 'TDM', 'RowKey': rowkey,
                      'RequestID': prev_data['req_id'], 'CurrentStatus': 'PreBuilt Out/Sent', 'Exceptions': exception, 'DataSent': json.dumps(datatosend), 'RawDataSent': ''}
            if sys.getsizeof(json.dumps(dataSent))/1024 <= 30:
                table_service.merge_entity('RequestResponseInfo', entity)

            logging.info(
                'Python EventGrid trigger processed an event: %s', webhook_response)
    except Exception as e:
        exc = f"Exception in PrebuiltModel, Request ID: {prev_data['req_id']},error: {e}"
        logging.info(exc)
        if "cannot identify image file" in str(e) or "EOF marker not found" in str(e):
            # Sent Empty data to webhook
            webhook_response = {
                "code": 0,
                "message": "OK",
                "request_id": prev_data['req_id'],
                "result": {
                    "total_amount": "",
                    "is_in_dubai_mall": False,
                    "store_name": "",
                    "date": "",
                    "time": "",
                    "rb_receipt_number": "",
                    "ocr_text": "",
                    "receipt_status": {
                        'has_loyalty_payment': False,
                        'has_store_credit_payment': False,
                        'reason_text': "",
                        'loyalty_redeemed': 0,
                        'store_credit_redeemed': 0
                    },
                    "auto_approve":False,
                    "auto_approve_reject_reason":"",
                    "auto_approval_amount_limit_check":True,
                    "auto_approval_store_match_check":True,
                    "auto_approval_date_match_check":True,
                    "auto_approval_amount_match_check":True,
                    "auto_approval_loyalty_reward_check":True,
                    "auto_approval_store_match_check":True
                }
            }
            raw_response = {
                "request_id": prev_data['req_id'],
                "storeMeta": {'name': '', 'confidence': 0, 'from': ''},
                "AmountMeta": {'Store': '', 'Amount': '', 'GotFrom': ''},
                "result": {
                    "total_amount": "",
                    "is_in_dubai_mall": False,
                    "store_name": "",
                    "date": "",
                    "time": "",
                    "rb_receipt_number": "",
                },
                'cri_data': prev_data.get('cri_data', {}),
                'image_url': prev_data.get('image_url', ''),
                "receipt_status": {
                    'has_loyalty_payment': False,
                    'has_store_credit_payment': False,
                    'reason_text': "",
                    'loyalty_redeemed': 0,
                    'store_credit_redeemed': 0
                },
                "auto_approve":False,
                "auto_approve_reject_reason":"",
                "auto_approval_amount_limit_check":True,
                "auto_approval_store_match_check":True,
                "auto_approval_date_match_check":True,
                "auto_approval_amount_match_check":True,
                "auto_approval_loyalty_reward_check":True,
                "auto_approval_store_match_check":True
            }
            # Call the event GRID
            result = []
            algorithm = 'HS256'
            private_key = os.environ['JWT_PRIVATE_KEY']
            jwt_token = jwt.encode(
                webhook_response, key=private_key, algorithm=algorithm)
            datatosend = {
                'req_id': prev_data['req_id'],
                'webhook_response': jwt_token,
                'raw_response': raw_response,
                'callback_url': prev_data['callback_url'],
                'x-token': prev_data['x-token'],
            }
            for i in range(1):
                result.append(EventGridEvent(
                    id=uuid.uuid4(),
                    # f"DefaultOCRResponse Completed",
                    subject=os.environ['EVENTGRID_SUBJECT_3'],
                    data=datatosend,
                    # 'DefaultOCRResponse',
                    event_type=os.environ['EVENTGRID_EVENT_TYPE_3'],
                    event_time=datetime.datetime.now(),
                    data_version=2.0
                ))
            event_grid_client.publish_events(
                os.environ['EVENTGRID_ENDPOINT'],
                events=result
            )
        else:
            emailresp = requests.post(
                os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": exc}))

            ent = table_service.get_entity(
                'RequestResponseInfo', 'TDM', rowkey)
            exception = ent.Exceptions + ',Exception in PrebuiltModel'+str(e)
            entity = {'PartitionKey': 'TDM', 'RowKey': rowkey, 'RequestID': prev_data['req_id'], 'CurrentStatus': 'Exception In PrebuiltModel', 'Exceptions': exception, 'DataSent': str(
                DataSent_ImageUrl), 'RawDataSent': ''}
            table_service.merge_entity('RequestResponseInfo', entity)
            if 'Max retries exceeded with url' in str(e) or "Connection aborted" in str(e):
                raise
