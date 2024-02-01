import json
import os
import re
import requests
import ast
import logging

import azure.functions as func

from io import BytesIO

import math
import sys
from PyPDF2 import PdfFileReader, PdfFileWriter
from PIL import Image
from fuzzywuzzy import fuzz
from azure.cosmosdb.table.tableservice import TableService
from datetime import datetime, timedelta

from msrest.authentication import TopicCredentials
from azure.eventgrid import EventGridClient
from azure.eventgrid.models import EventGridEvent

import uuid
import time

table_service = TableService(
    account_name=os.environ['STORAGE_ACCOUNT_NAME'], account_key=os.environ['STORAGE_ACCOUNT_KEY'])
credentials = TopicCredentials(
    # 'XSTPJTytz2rFkp0hIAaONieh5xqDeD/+H6noKFR4sjg='
    os.environ['EVENTGRID_TOPIC_KEY']
)

event_grid_client = EventGridClient(credentials)
accepted_inch = 10*72
accepted_pixel_max = 8000
accepted_pixel_min = 50
accepted_filesize_max = 50


def call_ocr_model(image_url, model_id, tid, rid, encrypted):
    headers = {
        'Content-Type': 'application/json',
        'Ocp-Apim-Subscription-Key': os.environ['OCP_APIM_SUBSCRIPTION_KEY']
    }
    file_name = image_url.split("?")[0].split("/")[-1]
    ext = file_name.split(".")[-1].lower()
    if encrypted:
        ext = 'enc'
    if ext == "pdf":
        headers['Content-Type'] = "application/pdf"
    elif ext == "jpeg" or ext == "jpg":
        headers['Content-Type'] = 'image/jpeg'
    elif ext == "png":
        headers['Content-Type'] = 'image/png'
    else:
        headers['Content-Type'] = 'application/json'
    body = BytesIO(requests.get(image_url).content)
    if ext != 'enc':
        status, data = pre_process(file_name, body, ext)
    else:
        status = True
        data = json.dumps({"source":image_url})
    if status:
        url = f"{os.environ['FORM_RECOGNIZER_URL']}/formrecognizer/{os.environ['FORM_RECOGNIZER_VERSION']}/custom/models/{model_id}/analyze?includeTextDetails=false"
        resp = None
        r_status = False
        while not r_status:
            resp = requests.post(url=url, data=data, headers=headers)
            if resp.status_code == 202:
                r_status = True
            elif 'error' in resp.json():
                logging.info("Waiting for Custom Model POST Analyze ...")
                if 'code' in resp.json()['error']:
                    if resp.json()['error']['code'] == "429":
                        time.sleep(3)
                    else:
                        logging.info(
                            f"Error in custom model status code {resp.json()['error']}")
                        return False, {}

        # if resp.status_code != 202:
        #     ent = table_service.get_entity('RequestResponseInfo','TDM',tid)
        #     exception = ent.Exceptions + ',Failed to Post Analysed'
        #     entity = {'PartitionKey':'TDM','RowKey':tid,'RequestID':rid,'CurrentStatus':'Exception In DefaultOCRResponse','Exceptions':exception,'DataSent':'','RawDataSent':''}
        #     table_service.merge_entity('RequestResponseInfo',entity)
        #     return False,resp.json()
        get_url = resp.headers["operation-location"]

        n_tries = 15
        n_try = 0
        # wait_sec = 5
        # max_wait_sec = 60
        while n_try < n_tries:
            try:
                resp = requests.get(url=get_url, headers=headers)
                resp_json = resp.json()
                if resp.status_code != 200:
                    logging.info(
                        "GET analyze results failed in Custom Model:\n%s" % json.dumps(resp_json))
                    return False, resp_json
                status = resp_json["status"]
                if status == "succeeded":
                    logging.info("Analysis succeeded:\n%s" %
                                 json.dumps(resp_json))
                    return True, resp_json
                if status == "failed":
                    ent = table_service.get_entity(
                        'RequestResponseInfo', 'TDM', tid)
                    exception = ent.Exceptions + ',Analysis Failed'
                    entity = {'PartitionKey': 'TDM', 'RowKey': tid, 'RequestID': rid,
                              'CurrentStatus': 'Exception In DefaultOCRResponse', 'Exceptions': exception, 'DataSent': '', 'RawDataSent': ''}
                    table_service.merge_entity('RequestResponseInfo', entity)
                    logging.info("Analysis failed:\n%s" %
                                 json.dumps(resp_json))
                    return False, resp_json
                # Analysis still running. Wait and retry.
                time.sleep(2)
                n_try += 1
                # wait_sec = min(2*wait_sec, max_wait_sec)
            except Exception as e:

                ent = table_service.get_entity(
                    'RequestResponseInfo', 'TDM', tid)
                exception = ent.Exceptions + ',Failed to GET Analysed results'
                entity = {'PartitionKey': 'TDM', 'RowKey': tid, 'RequestID': rid,
                          'CurrentStatus': 'Exception In DefaultOCRResponse', 'Exceptions': exception, 'DataSent': '', 'RawDataSent': ''}
                table_service.merge_entity('RequestResponseInfo', entity)
                msg = "GET analyze results failed:\n%s" % str(e)
                logging.info(msg)
                return False, {}
        logging.info(
            "Analyze operation did not complete within the allocated time.")
        return False, {}
    else:
        return False, {}


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
            factor = accepted_pixel_max/max(img.size)
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
    logging.info(f"Received {result}")
    prev_data = event.get_json()
    rowkey = str(uuid.uuid4())
    entity = {'PartitionKey': 'TDM', 'RowKey': rowkey,
              'RequestID': prev_data['req_id'], 'CurrentStatus': 'DefaultOCRResponse In/ Received', 'Exceptions': '', 'DataSent': '', 'RawDataSent': ''}
    table_service.insert_entity('RequestResponseInfo', entity)
    try:
        # previous_date = (datetime.now()-timedelta(days=14)).isoformat()
        stores = table_service.query_entities(
            'storelist')
        store_list = []
        for store in stores:
            try:
                ast.literal_eval(store['synonyms'])
                store_list.append((store['StoreNameInReceipt'], ast.literal_eval(
                    store['synonyms']), store['StoreName']))
            except Exception as e:
                exc = f"Error in synonyms for Store:- {store['StoreName']}"
                emailresp = requests.post(
                    os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": exc}))
                logging.warning(
                    f"Error in synonium of Store:- {store['StoreName']}")
        # store_list = [(store['StoreName'],ast.literal_eval(store['synonyms'])) for store in stores]

        ocr_text = prev_data['ocr_text']
        default_fields = prev_data['default_fields']

        Ratio_Table = {}
        Default_Ratio_Table = {}
        Synonyms_Ratio_Table = {}
        logging.info(f"ocr_text {ocr_text}")
        # tdmcheckArr = {}
        ocr_text_list = ocr_text.split("#splitter#")
        store_found_from_syn = False
        storeMeta = {'name': '', 'confidence': 0, 'from': ''}
        storeConfidenceValue = 0
        for k in ocr_text_list:
            for x in store_list:
                clean_x = re.sub("[^A-Za-z0-9]+", " ", x[0]).lower().strip()
                clean_k_text = k.strip()
                # tdmcheck = fuzz.ratio("TDM",clean_k_text.upper())
                # dubaimallcheck = fuzz.ratio("the dubai mall",clean_k_text.lower())
                # dubaimallcheck = int(dubaimallcheck) if int(dubaimallcheck) > int(tdmcheck) else int(tdmcheck)
                # if dubaimallcheck >= 80:
                # tdmcheckArr.update({clean_k_text.lower():dubaimallcheck})
                if clean_x != "" and clean_x not in Synonyms_Ratio_Table:
                    Synonyms_Ratio_Table[clean_x] = 0
                for syns in x[1]:
                    if syns != "":
                        if Synonyms_Ratio_Table[clean_x] < fuzz.ratio(syns.strip(), clean_k_text):
                            Synonyms_Ratio_Table.update(
                                {clean_x: fuzz.ratio(syns.strip(), clean_k_text)})
                            if fuzz.ratio(syns.strip(), clean_k_text) == 100:
                                store_found_from_syn = True
                                storeConfidenceValue = 100
                                break
                if store_found_from_syn:
                    break
            if store_found_from_syn:
                break
        syn_store_list_confidence = dict(
            sorted(Synonyms_Ratio_Table.items(), key=lambda item: item[1], reverse=True))
        logging.info(f"Synonym matches {syn_store_list_confidence}")
        store_found_from_syn_conf_name = next(iter(syn_store_list_confidence))
        if not store_found_from_syn:
            storeConfidenceValue = syn_store_list_confidence[store_found_from_syn_conf_name]
            if storeConfidenceValue >= 91:
                store_found_from_syn = True

        default_store_list_confidence_number = 0
        default_store_name = ''
        store_found = False
        if not store_found_from_syn:
            if default_fields['MerchantName'] != '':
                store_found = False
                default_store_name = re.sub(
                    "[^A-Za-z0-9]+", " ", default_fields['MerchantName'])
                for x in store_list:
                    clean_x = re.sub("[^A-Za-z0-9]+", " ",
                                     x[0]).lower().strip()
                    if clean_x != "":
                        if clean_x in Default_Ratio_Table:
                            if Default_Ratio_Table[clean_x] < fuzz.ratio(clean_x, default_store_name.lower().strip()):
                                Default_Ratio_Table.update({clean_x: fuzz.ratio(
                                    clean_x, default_store_name.lower().strip())})
                                if fuzz.ratio(clean_x, default_store_name.lower().strip()) == 100:
                                    store_found = True
                                    break
                        else:
                            Default_Ratio_Table.update({clean_x: fuzz.ratio(
                                clean_x, default_store_name.lower().strip())})
                            if fuzz.ratio(clean_x, default_store_name.lower().strip()) == 100:
                                store_found = True
                                break
                    if store_found:
                        break
                # logging.info(f'Default_Ratio_Table: {Default_Ratio_Table}')
                default_store_list_confidence = dict(
                    sorted(Default_Ratio_Table.items(), key=lambda item: item[1], reverse=True))
                # list(default_store_list_confidence.keys())[0]
                default_store_name = next(iter(default_store_list_confidence))
                default_store_list_confidence_number = int(
                    default_store_list_confidence[default_store_name])
                storeConfidenceValue = default_store_list_confidence_number
                logging.info(f"{default_store_list_confidence} Fuzzywuzzy")
                # default_store_name = next(iter(default_store_list_confidence))
                # default_store_name = re.sub("[^A-Za-z0-9]+", " ",default_store_name)
                logging.info(f"{default_store_name} default by Fuzzywuzzy")
            if not store_found and default_store_list_confidence_number < 90:
                for k in ocr_text_list:
                    for x in store_list:
                        clean_x = re.sub("[^A-Za-z0-9]+", " ",
                                         x[0]).lower().strip()
                        # re.sub("[^A-Za-z0-9]+", " ",k["text"])
                        clean_k_text = k.strip()
                        if clean_x != "":
                            if clean_x in Ratio_Table:
                                if Ratio_Table[clean_x] < fuzz.ratio(clean_x, clean_k_text.lower()):
                                    Ratio_Table.update(
                                        {clean_x: fuzz.ratio(clean_x, clean_k_text.lower())})
                                    if fuzz.ratio(clean_x, clean_k_text.lower()) == 100:
                                        store_found = True
                                        break
                            else:
                                Ratio_Table.update(
                                    {clean_x: fuzz.ratio(clean_x, clean_k_text.lower())})
                                if fuzz.ratio(clean_x, clean_k_text.lower()) == 100:
                                    store_found = True
                                    break
                        if store_found:
                            break
                    if store_found:
                        break
                store_list_confidence = dict(
                    sorted(Ratio_Table.items(), key=lambda item: item[1], reverse=True))
                logging.info(f"{store_list_confidence} Fuzzywuzzy")
                store_name = next(iter(store_list_confidence))
                store_name = re.sub("[^A-Za-z0-9]+", " ", store_name)
                store_name = store_name.strip()
                storeConfidenceValue = int(store_list_confidence[store_name])
                logging.info(f"{store_name} first by Fuzzywuzzy")

        call_subs_model = False
        storeMeta['confidence'] = storeConfidenceValue
        if store_found_from_syn:
            store_name = store_found_from_syn_conf_name
            storeMeta['from'] = 'SYNONYM'
            if storeMeta['confidence'] < 100:
                storeMeta['from'] = 'SYNONYM-PARTIAL'

            # logging.info(f"Syns if ocr text store {store_name}:{syn_store_list_confidence[store_name]} has more confidence")
            call_subs_model = True
        elif default_store_list_confidence_number >= 90:
            store_name = default_store_name
            storeMeta['from'] = 'DEFAULT'
            # logging.info(f"if-default model store {default_store_name}:{default_store_list_confidence_number} has more confidence")
        elif default_store_list_confidence_number < int(store_list_confidence[store_name]):
            store_name = store_name
            storeMeta['from'] = 'OCR'
            # logging.info(f"ocr text store {store_name}:{store_list_confidence[store_name]} has more confidence")
            call_subs_model = True
        else:
            store_name = store_found_from_syn_conf_name
            storeMeta['from'] = 'SYNONYM-TOPMOST'
            # logging.info(f"else-default model store {default_store_name}:{default_store_list_confidence_number} has more confidence")

        for x in store_list:
            store_in_receipt = re.sub(
                "[^A-Za-z0-9]+", " ", x[0]).lower().strip()
            if store_name == store_in_receipt:
                store_name = re.sub("[^A-Za-z0-9]+", " ", x[2]).lower().strip()
                break
        
        storeMeta['name'] = store_name
        Ahmed_Substores = ast.literal_eval(os.environ['Ahmed_Substores'])
        GrandStore_Substores = ast.literal_eval(os.environ['GrandStore_Substores'])
        SandSports_Substores = ast.literal_eval(os.environ['SandSports_Substores'])
        if store_name == 'sun sand sports':
            for sub_store in SandSports_Substores:
                if sub_store["keyword"].lower() in ocr_text.lower():
                    storeMeta['name'] = sub_store["store"]
                    storeMeta['confidence'] = 100
                    storeMeta['from'] = 'From Store Specific Logic'
                    store_name = sub_store["store"]
                    break
        if store_name == 'ahmed seddiqi sons' or store_name == 'ahmed seddiqi sons br':
            for sub_store in Ahmed_Substores:
                if sub_store.lower() in ocr_text.lower():
                    storeMeta['name'] = sub_store.upper()
                    storeMeta['confidence'] = 100
                    storeMeta['from'] = 'From Store Specific Logic'
                    store_name = sub_store.title()
                    break
        if store_name == 'grand stores digital':
            for sub_store in GrandStore_Substores:
                if sub_store.lower() in ocr_text.lower():
                    storeMeta['name'] = sub_store.upper()
                    storeMeta['confidence'] = 100
                    storeMeta['from'] = 'From Store Specific Logic'
                    store_name = sub_store.title()
                    break
        logging.info(f"StoreMeta {storeMeta}")
        store_model = table_service.query_entities(
            os.environ['FORM_RECOGNIZER_MODELS'], filter=f"CleanStoreName eq '{store_name}'"
        )
        encrypted = prev_data['encrypted']
        image_url = prev_data['image_url']
        model_id = ''
        logging.info(f"Got {len(list(store_model))}")
        for model in store_model:
            model_id = str(model.ModelID).strip()
            break
        
        logging.info(f"Model id {model_id}")
        if model_id:
            status, custom_data = call_ocr_model(
                image_url, model_id, rowkey, prev_data['req_id'],encrypted)
        else:
            status, custom_data = False, {}

        logging.info(f"custom_data :- {custom_data} ")

        custom_fields = {'MerchantName': '', 'MerchantNameConfidence': '', 'TransactionDate': '', 'TransactionDateConfidence': '',
                         'TransactionTime': '', 'TransactionTimeConfidence': '', 'Total': '', 'TotalConfidence': '', 'InvoiceNumber': '', 'InvoiceNumberConfidence': ''}
        if status:
            CustomdocumentResults = custom_data["analyzeResult"]["documentResults"]
            for item in CustomdocumentResults:
                fields = item['fields']
                if "Store_Name" in fields:
                    if "text" in fields['Store_Name']:
                        custom_fields['MerchantName'] = fields['Store_Name']['text']
                        custom_fields['MerchantNameConfidence'] = fields['Store_Name']['confidence']
                if "Date" in fields:
                    if "text" in fields['Date']:
                        custom_fields['TransactionDate'] = fields['Date']['text']
                        custom_fields['TransactionDateConfidence'] = fields['Date']['confidence']
                if "TransactionTime" in fields:
                    if "text" in fields['TransactionTime']:
                        custom_fields['TransactionTime'] = fields['TransactionTime']['text']
                        custom_fields['TransactionTimeConfidence'] = fields['TransactionTime']['confidence']
                if "Total_Amount" in fields:
                    if "text" in fields['Total_Amount']:
                        custom_fields['Total'] = fields['Total_Amount']['text']
                        custom_fields['TotalConfidence'] = fields['Total_Amount']['confidence']
                if "Invoice_Number" in fields:
                    if "text" in fields['Invoice_Number']:
                        custom_fields['InvoiceNumber'] = fields['Invoice_Number']['text']
                        custom_fields['InvoiceNumberConfidence'] = fields['Invoice_Number']['confidence']
                if storeMeta['name'] == 'Al Rifai':
                    if "Tax_Amount" in fields:
                        if "text" in fields['Tax_Amount']:
                            custom_fields['Tax_Amount'] = fields['Tax_Amount']['text']
                            custom_fields['InvoiceNumberConfidence'] = fields['Tax_Amount']['confidence']
                    if "Taxable_Amount" in fields:
                        if "text" in fields['Taxable_Amount']:
                            custom_fields['Taxable_Amount'] = fields['Taxable_Amount']['text']
                            custom_fields['InvoiceNumberConfidence'] = fields['Taxable_Amount']['confidence']

        if custom_fields['MerchantName'] == "" and custom_fields['TransactionDate'] == "" and custom_fields['TransactionTime'] == "" and custom_fields['Total'] == "":
            logging.info(
                "{'message': 'Model Retured Blank','req_id':'"+prev_data['req_id']+"'}")

        # Call the event GRID

        result = []
        datatosend = {
            'req_id': prev_data['req_id'],
            # 'dubaimall_check_arr': tdmcheckArr,
            'callback_url': prev_data['callback_url'],
            'x-token': prev_data['x-token'],
            'custom_fields': custom_fields,
            'default_fields': default_fields,
            'custom_ocr_response': ocr_text,
            'storeMeta': storeMeta,
            'cri_data': prev_data.get('cri_data', {}),
            'image_url': prev_data.get('image_url', '')
        }
        logging.info(datatosend)
        ent = table_service.get_entity('RequestResponseInfo', 'TDM', rowkey)
        exception = ent.Exceptions
        entity = {'PartitionKey': 'TDM', 'RowKey': rowkey,
                  'RequestID': prev_data['req_id'], 'CurrentStatus': 'DefaultOCRResponse Out/ Sent', 'Exceptions': exception, 'DataSent': json.dumps(datatosend), 'RawDataSent': ''}
        if sys.getsizeof(json.dumps(datatosend))/1024 <= 30:
            table_service.merge_entity('RequestResponseInfo', entity)
        for i in range(1):
            result.append(EventGridEvent(
                id=uuid.uuid4(),
                # f"BusinessLogic Calling",
                subject=os.environ['EVENTGRID_SUBJECT_2'],
                data=datatosend,
                # 'BusinessLogic',
                event_type=os.environ['EVENTGRID_EVENT_TYPE_2'],
                event_time=datetime.now(),
                data_version=2.0
            ))
        # prev_data['form_recognizer_response']|"{'sample':''}",
        # custom_data, ||"{'sample':''}",
        logging.info(f"Sending Data size {sys.getsizeof(result)}")
        event_grid_client.publish_events(
            os.environ['EVENTGRID_ENDPOINT'],
            events=result
        )
        logging.info(f'DefaultOCRResponse Completed')
    except Exception as e:
        # e = ""
        exc = f"Exception in DefaultOCRResponse, Request ID: {prev_data['req_id']}, error: {e}"
        emailresp = requests.post(
            os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": exc}))
        logging.info(exc)
        ent = table_service.get_entity('RequestResponseInfo', 'TDM', rowkey)
        exception = ent.Exceptions + ',Exception in DefaultOCRResponse'+str(e)
        entity = {'PartitionKey': 'TDM', 'RowKey': rowkey,
                  'RequestID': prev_data['req_id'], 'CurrentStatus': 'Exception In DefaultOCRResponse', 'Exceptions': exception, 'DataSent': '', 'RawDataSent': ''}
        table_service.merge_entity('RequestResponseInfo', entity)
        if 'Max retries exceeded with url' in str(e) or "Connection aborted" in str(e):
            raise
