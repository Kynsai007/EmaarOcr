import logging
import os

import azure.functions as func

import aiohttp
import json


from msrest.authentication import TopicCredentials
from azure.eventgrid import EventGridClient
from azure.eventgrid.models import EventGridEvent
import datetime
import uuid
from PyPDF2 import PdfFileReader
from io import BytesIO
import requests


credentials = TopicCredentials(
    os.environ['EVENTGRID_TOPIC_KEY']
)

event_grid_client = EventGridClient(credentials)

def page_more_than_10(file_bytes, file_type):
    if file_type == "pdf":
        try:
            pdf = PdfFileReader(file_bytes, strict=False)
            dimention = pdf.getPage(0).mediaBox
            num_pages = pdf.getNumPages()
            if num_pages > 10:
                return True,"invalid"
            else:
                return False,"valid"
        except Exception as e:
            return False,"error"
    return False,"valid"
    
async def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    logging.info('Received process-invoice request.')
    try:
        requestId = req.params.get('req_id')
    except:
        requestId = 0
    if requestId == "coldstart":
        return func.HttpResponse(json.dumps({"code": 200, "message": "Cold Start Trigger",
                                                 "request_id": "Test"}), status_code=200) 
    try:
        logging.info(dict(req.headers))
        try:
            x_api_key = req.headers['x-api-key']
            if x_api_key != os.environ['X_API_KEY']:
                raise NameError('Unauthorized Request')

        except Exception as e:

            exc = f"Exception in Process Invoice : Invalid X-api-key, request ID: {requestId}"
            async with aiohttp.ClientSession() as client:
                async with client.post(os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": exc})) as response:
                    await response.text()

            logging.info('Exception in func.')
            return func.HttpResponse("", status_code=401)
        encrypted = False
        try:
            data = req.get_json()
            logging.info(f"Received:- {data}")
            image_url = data.get('image_url')
            req_id = req.params.get('req_id')
            try:
                file_name = image_url.split("?")[0].split("/")[-1]
                ext = file_name.split(".")[-1].lower()
                res = requests.get(image_url)
                body = BytesIO(res.content)
                status,mes = page_more_than_10(body, ext)
                logging.info(f"Encrypted {mes}")
                if mes == "error":
                    encrypted = True
                if status:
                    return func.HttpResponse(json.dumps({"code": 500, "message": "Invalid File,File Pages is more than 10",
                                                 "request_id": req_id}), status_code=200)
            except:
                return func.HttpResponse(json.dumps({"code": 500, "message": "Invalid File",
                                                 "request_id": req_id}), status_code=200)
            callback_url = data.get('callback_url')
            x_token = os.environ['EMAAR_X_TOKEN']#req.headers['x-token']
            cri_data = data.get('cri_data', {})
        except Exception as e:

            exc = f"Exception in Process Invoice : Invalid JSON Schema, Request ID: {requestId}"
            async with aiohttp.ClientSession() as client:
                async with client.post(os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": exc})) as response:
                    await response.text()

            return func.HttpResponse(json.dumps({"code": 400, "message": "Invalid JSON Schema",
                                                 "request_id": req_id}), status_code=200)

        result = []
        dataSent = {
            'req_id': req_id,
            'encrypted': encrypted,
            'callback_url': callback_url,
            'x-token': x_token,
            'image_url': image_url,
            'cri_data': cri_data
        }
        logging.info(f"DataTobeSent:- {dataSent}")
        try:
            result.append(EventGridEvent(
                id=uuid.uuid4(),
                subject=os.environ['EVENTGRID_SUBJECT_5'],
                data=dataSent,
                event_type=os.environ['EVENTGRID_EVENT_TYPE_5'],
                event_time=datetime.datetime.now(),
                data_version=2.0
            ))

            event_grid_client.publish_events(
                os.environ['EVENTGRID_ENDPOINT'],
                events=result
            )
        except:
            return func.HttpResponse(
            json.dumps({
                "code": 200,
                "message": "success",
                "request_id": req_id
            }),
            status_code=200
        )    
        return func.HttpResponse(
            json.dumps({
                "code": 200,
                "message": "success",
                "request_id": req_id
            }),
            status_code=200
        )

    except Exception as ex:
        exc = f"Exception in Process Invoice, Request ID: {requestId},error: {ex}"
        async with aiohttp.ClientSession() as client:
            async with client.post(os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": exc})) as response:
                await response.text()
        logging.info(exc)

        return func.HttpResponse(
            json.dumps({
                "code": 500,
                "message": f"Exception in Process Invoice {ex}",
                "request_id": requestId
            }),
            status_code=400
        )
