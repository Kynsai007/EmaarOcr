import json
import os
import re
import ast
import logging
import requests


import azure.functions as func

from fuzzywuzzy import fuzz, process
from msrest.authentication import TopicCredentials
from azure.eventgrid import EventGridClient
from azure.eventgrid.models import EventGridEvent
from azure.cosmosdb.table.tableservice import TableService

from datetime import datetime, timedelta
import uuid
import sys
        
from dateutil import parser

credentials = TopicCredentials(
    # 'XSTPJTytz2rFkp0hIAaONieh5xqDeD/+H6noKFR4sjg='
    os.environ['EVENTGRID_TOPIC_KEY']
)
table_service = TableService(
    account_name=os.environ['STORAGE_ACCOUNT_NAME'], account_key=os.environ['STORAGE_ACCOUNT_KEY'])
logging.getLogger("azure.cosmosdb").setLevel(logging.WARNING)

event_grid_client = EventGridClient(credentials)


def clean_amount(amount):
    cleaned_amount = re.sub("[^.,:\d\s]", "", amount)
    cleaned_amount = re.sub('[.;:,A-Z ]+$', '', cleaned_amount)
    try:
        cleaned_amount = re.sub(
            r'^.*?[0-9]', re.match(r'^.*?[0-9]', cleaned_amount).group(0)[-1], cleaned_amount)
    except:
        logging.info(f"error {cleaned_amount}")
    if len(cleaned_amount) > 3:
        if cleaned_amount[-3] == " " or cleaned_amount[-3] == ":":
            cleaned_amount = cleaned_amount[:-3]+"."+cleaned_amount[-2:]
    comma_index = cleaned_amount.find(",")
    dot_index = cleaned_amount.find(".")
    if comma_index > dot_index and dot_index != -1:
        cleaned_amount = cleaned_amount.replace(".", "")
    cleaned_amount = cleaned_amount.replace(
        ".", "", cleaned_amount.count('.')-1)
    if len(cleaned_amount) > 3 and cleaned_amount.count(",") == 1 and cleaned_amount[-3] == ",":
        cleaned_amount = cleaned_amount.replace(",", ".")
    else:
        cleaned_amount = cleaned_amount.replace(",", "")
    cleaned_amount = re.sub("[^.,\d]", "", cleaned_amount)
    return cleaned_amount

def format_to_two_decimal_places(string):
    try:
        number = float(string)
        formatted_string = "{:.2f}".format(number)
        return formatted_string
    except ValueError:
        return string

def clean_amount_n(amount):
    try:
        cleaned_amount = re.sub("[^.,:\d\s]", "", amount)
        cleaned_amount = re.sub('[.;:,A-Z ]+$', '', cleaned_amount)
        try:
            cleaned_amount = re.sub(
                r'^.*?[0-9]', re.match(r'^.*?[0-9]', cleaned_amount).group(0)[-1], cleaned_amount)
        except:
            logging.info(f"error {cleaned_amount}")
        if len(cleaned_amount) > 3:
            if cleaned_amount[-3] == " " or cleaned_amount[-3] == ":":
                cleaned_amount = cleaned_amount[:-3]+"."+cleaned_amount[-2:]
        comma_index = cleaned_amount.rfind(",")
        dot_index = cleaned_amount.rfind(".")
        if comma_index > dot_index and dot_index != -1:
            cleaned_amount = cleaned_amount.replace(".", "")
        cleaned_amount = cleaned_amount.replace(".", "", cleaned_amount.count('.')-1)
        if len(cleaned_amount) > 3 and cleaned_amount.count(",") >= 1 and cleaned_amount[-3] == ",":
            cleaned_amount = cleaned_amount[:comma_index] + "." + cleaned_amount[comma_index + 1:]
            cleaned_amount = cleaned_amount.replace(",", "")
        else:
            cleaned_amount = cleaned_amount.replace(",", "")
        matches = re.finditer(
            r"[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)", cleaned_amount)
        for match in matches:
            cleaned_amount = match.group(0)
        return format_to_two_decimal_places(cleaned_amount)
    except Exception as e:
        logging.info(f"Exception in clean_amount_n {e}")
        return amount


def get_clean_date(date, month_first):
    try:
        if "\\" in date:
            date = "-".join(date.split("\\"))
        test_string = date.upper().replace(",", " ").replace(".", "")
        test_string = re.sub(r"(0CT)","OCT",test_string)
        test_string = re.sub(r"(N0V)","NOV",test_string)
        matches = re.findall(
            r"((?:\d{4}|\d{2}|\d{1})(\s|-|/|\\)((JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\w*|([\d]{1,2}))(\s|-|/|\\)+([\d]{2,4}))", test_string)
        if not matches:
            matches = re.findall(
                r"(([\d]{2,4})(\s|-|/)((JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\w*|[\d]{1,2})(\s|-|/)([\d]{1,2}))", test_string)
        if not matches:
            matches = re.findall(
                r"((?:\d{2})([-|,|\s]*)((JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\w*)([ ]?)(?:\d{4}|\d{2}))", test_string)
        if not matches:
            matches = re.findall(
                r"((JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\w*([ ]?)([\d]{2}|[\d]{1})([', ]*)([\d]{1,4}))", test_string)
        if not matches:
            matches = re.findall(
                r"([\d]{2}|[\d]{1})((JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\w*([ ]?)([', ]*)([\d]{1,4}))", test_string)
        if not matches:
            matches = re.findall(
                r"(([\d]{1,2})(\s)?((JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\w*)(\s|-|/|\\|')+([\d]{2,4}))", test_string)
        if not matches:
            matches = re.findall(
                r"((JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\w*([, ]*)([\d]{1,2})([, ]*)(?:\d{4}|\d{2}))", test_string)
        if not matches:
            matches = re.findall(
                r"(([\d]{1,2})([.\s\/-]+)([\d]{1,2})([.\s\/-]+)([\d]{4}))", test_string)
            if matches:
                matches = [
                    [f"{matches[0][1]}/{matches[0][3]}/{matches[0][5]}"]]
        if not matches:
            matches = re.findall(
                r"(([\d]{2})([:,\\/ ]*)([\d]{1,2})([:,\\/ ]*)([\d]{4}|[\d]{2}))", test_string)
            if matches:
                matches = [
                    [f"{matches[0][1]}/{matches[0][3]}/{matches[0][5]}"]]
        for el in matches:
            try:
                plain_date = el[0]
                if month_first:
                    date = parser.parse(plain_date)
                else:
                    date = parser.parse(plain_date, dayfirst=True)
            except Exception as e:
                match = re.findall('[/-][\d][ ]?[\d][/-]', test_string)
                i_new = match[0].replace(' ', '')
                test_string = test_string.replace(
                    match[0], i_new).replace(' ', '')
                plain_date = re.findall(
                    '[\d]{1,2}[/-][\d]{1,2}[/-][\d]{2,4}', test_string)[0]
                date = parser.parse(plain_date)
            if datetime.now() < date:
                date = parser.parse(plain_date, dayfirst=True)

            if date.strftime("%Y") not in [(datetime.now()-timedelta(days=_*365)).strftime("%Y") for _ in range(4)]:
                date = parser.parse(
                    f'{datetime.now().strftime("%Y")}/{date.strftime("%m")}/{date.strftime("%d")}')
            return date.strftime("%Y-%m-%d")
        return ''
    except Exception as e:
        #         print(e,date)
        logging.info(f"Date Function:- {e} {date}")
        return ''


def main(event: func.EventGridEvent, context: func.Context):
    prev_data = event.get_json()
    rowkey = str(uuid.uuid4())
    try:
        entity = {'PartitionKey': 'TDM', 'RowKey': rowkey,
                  'RequestID': prev_data['req_id'], 'CurrentStatus': 'BusinessLogic In/ Received', 'Exceptions': '', 'DataSent': '', 'RawDataSent': ''}
        table_service.insert_entity('RequestResponseInfo', entity)
        logging.info("Business Logic Called")
        logging.info(f"Data in Business Logic {event.get_json()}")

        ocr_text = prev_data["custom_ocr_response"]
        # dubaimall_check_arr = prev_data["dubaimall_check_arr"]
        # logging.info(f"Dubai mall Arr {dubaimall_check_arr}")
        is_in_dubai_mall = False
        # or len(dubaimall_check_arr) > 0:
        if "TDM" in ocr_text or "dubai mall" in ocr_text.lower():
            is_in_dubai_mall = True
        else:
            is_in_dubai_mall = False
        if re.search(r"(the( ?))?(du(b|h)a(i|l)( ?)(m|h)all)", ocr_text.lower()):
            ocr_text = re.sub(
                r"(the( ?))?(du(b|h)a(i|l)( ?)(m|h)all)", "The Dubai Mall", ocr_text.lower())
            is_in_dubai_mall = True
        if is_in_dubai_mall:
            rejection_keywords = ast.literal_eval(os.environ["Dubai_Mall_Rejection"])
            for r in rejection_keywords:
                if r in ocr_text.lower():
                    is_in_dubai_mall = False
        # STORE NAME
        store_name_a = ''
        store_name_b = ''
        store_name_c = ''
        # Store_Name
        if 'MerchantName' in prev_data['custom_fields'] and prev_data['custom_fields']['MerchantName'] != "":
            store_name_a = prev_data['custom_fields']['MerchantName']
        if 'MerchantName' in prev_data['default_fields'] and prev_data['default_fields']['MerchantName'] != "":
            store_name_b = prev_data['default_fields']['MerchantName']

        store_name_c = prev_data['storeMeta']['name']

        # To get clean store name get top match from active store
        # previous_date = (datetime.now()-timedelta(days=14)).isoformat()
        stores = table_service.query_entities(
            'storelist')  # ,filter=f"updateDate ge datetime'{previous_date}'"
        store_list = [store['StoreName'] for store in stores]
        store_list_clean = [
            re.sub("[^A-Za-z0-9]+", " ", store).lower().strip() for store in store_list]
        store_name_clean_a = ('', 0)
        store_name_clean_b = ('', 0)
        store_name_clean_c = ('', 0)
        if store_name_a != "":
            store_name_clean_a = process.extractOne(
                re.sub("[^A-Za-z0-9]+", " ", store_name_a).lower().strip(), store_list_clean)
        if store_name_b != "":
            store_name_clean_b = process.extractOne(
                re.sub("[^A-Za-z0-9]+", " ", store_name_b).lower().strip(), store_list_clean)
        if store_name_c != "":
            store_name_clean_c = process.extractOne(
                re.sub("[^A-Za-z0-9]+", " ", store_name_c).lower().strip(), store_list_clean)

        store_name_clean_x = ('', 0)
        if store_name_clean_a[1] >= store_name_clean_x[1]:
            store_name_clean_x = store_name_clean_a
        if store_name_clean_b[1] >= store_name_clean_x[1]:
            store_name_clean_x = store_name_clean_b
        if store_name_clean_c[1] >= store_name_clean_x[1]:
            store_name_clean_x = store_name_clean_c
        try:
            store_name_official = store_list[store_list_clean.index(
                store_name_clean_x[0])]
        except Exception as e:
            store_name_official = store_name_c
            logging.info(
                "Unable to find official store name, setting to raw ocr store_name")
        
        filterstore = store_name_official.replace("'","''")
        store_ids = table_service.query_entities('storelist',filter=f"StoreName eq '{filterstore}'")
        store_id_official = ""
        for st in store_ids:
            store_id_official = st.RowKey
            break

        logging.info(
            f"store_names:- [{store_name_a},{store_name_b},{store_name_c}] , store_name_clean [fuzzy]:- {store_name_clean_x}, store_name_official:- {store_name_official}, store_id : {store_id_official}")

        # STORE NAME END
        # TOTAL START
        total_from = 'custom'
        if prev_data['custom_fields']['Total'] != '':  # Total_Amount
            total_amount = prev_data['custom_fields']['Total']
        elif prev_data['default_fields']['Total'] != '':
            total_amount = prev_data['default_fields']['Total']
            total_from = 'prebuilt'
        else:
            total_amount = ''
        if total_from == 'custom' and prev_data['custom_fields']['TotalConfidence'] != '' and prev_data['default_fields']['TotalConfidence'] != '':
            if float(prev_data['custom_fields']['TotalConfidence']) <= 0.5 and float(prev_data['default_fields']['TotalConfidence']) >= 0.9:
                total_amount = prev_data['default_fields']['Total']

        if filterstore == "Al Rifai":
            if "Taxable_Amount" in prev_data['custom_fields'] and prev_data['custom_fields']['Taxable_Amount'] != '':  # Total_Amount
                total_amount = prev_data['custom_fields']['Taxable_Amount']
            else:
                total_amount = prev_data['custom_fields']['Total']


        # Confirm Total from ocr_text
        total_finding_text = ocr_text
        total_match_obj = {}
        amount_meta = {'Store': '', 'Amount': '', 'GotFrom': ''}
        cri_data = prev_data['cri_data']
        check_amount = clean_amount_n(total_amount)
        if check_amount != '':
            for w in ocr_text.split("#splitter#"):
                cleanedtext = clean_amount_n(w)
                if check_amount in cleanedtext:
                    total_amount = check_amount
                    amount_meta['Store'] = store_name_official
                    amount_meta['Amount'] = total_amount
                    amount_meta['GotFrom'] = 'Model'
                    break
        else:
            if 'total' in total_finding_text.lower():
                total_finding_text = total_finding_text.lower().split(
                    "total")[-1]
                ocr_text_list = []
                ocr_text_list = total_finding_text.split("#splitter#")
                for w in ocr_text_list:
                    cleanedtext = clean_amount_n(w)
                    if cleanedtext != "":
                        total_match_obj.update({cleanedtext: 0})
                if len(total_match_obj.keys()) > 0:
                    total_amount = list(total_match_obj.keys())[0]
                    amount_meta['Store'] = store_name_official
                    amount_meta['Amount'] = total_amount
                    amount_meta['GotFrom'] = 'TotalKeywordLogic'
        if filterstore == "Al Rifai":
            if 'Tax_Amount' in prev_data['custom_fields'] :
                tot_tax = clean_amount_n(prev_data['custom_fields']['Tax_Amount'])
                tot_tax = float(tot_tax)
                if tot_tax != '':
                    total_amount = float(total_amount)
                    if total_amount != '':
                        total_amount = total_amount+tot_tax
                        total_amount = str(round(total_amount,2))
            else:
                if prev_data['custom_fields']['Total'] != '':  # Total_Amount
                    total_amount = prev_data['custom_fields']['Total']
                else:
                    total_amount = ''

        if len(total_amount.split(".")[0]) >= 6 or len(total_amount.split(".")[0]) <= 2:
            logging.warning(
                f"Total Amount WARNING, Receipt ID {prev_data['req_id']}, total amount = {total_amount}")
            if cri_data and 'amount' in cri_data and cri_data['amount']:
                total_amount = cri_data['amount']
                amount_meta['Store'] = store_name_official
                amount_meta['Amount'] = total_amount
                amount_meta['GotFrom'] = 'CustomerData'
            else:
                total_amount = clean_amount_n(total_amount)
        else:
            total_amount = clean_amount_n(total_amount)

        # Clean Total
        #store_set_1 = ['CH Carolina Herrera','Clarins','Chanel']
        # if store_name_official in store_set_1:
        #     if len(total_amount) > 3:
        #         if total_amount[-3] == " " or total_amount[-3] == ":":
        #             total_amount = total_amount[:-3]+"."+total_amount[-2:]
        #     total_amount = re.sub('[^0-9,]+', '', total_amount)
        #     total_amount = total_amount.replace(",", "",total_amount.count(',')-1)
        #     total_amount = total_amount.replace(",", ".")

        # if len(total_amount) > 3:
        #     if total_amount[-3] == " " or total_amount[-3] == ":":
        #         total_amount = total_amount[:-3]+"."+total_amount[-2:]
        # total_amount = re.sub('[^0-9.]+', '', total_amount)
        # total_amount = total_amount.replace(".", "",total_amount.count('.')-1)

        # TOTAL END
        # DATE START
        MONTH_FIRST_STORES = []
        month_first = False
        try:
            MONTH_FIRST_STORES = ast.literal_eval(
                os.environ['MONTH_FIRST_STORES'])
        except Exception as e:
            exc = f"Error in MONTH_FIRST_STORES"
            emailresp = requests.post(
                os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": exc}))
            logging.warning(f"Error in MONTH_FIRST_STORES")
        date = ""
        # ["H&M", "Apple Store", "Pottery Barn and Pottery Barn Kids", "Tips and Toes", "Cesare Paciotti", "Salsa Jeans", "Obagi Medi Spa", "Galeries Lafayette", "Jashanmal Home Department Store", "Debenhams", "Rituals"]:
        if store_name_official.lower() in MONTH_FIRST_STORES:
            month_first = True
        else:
            month_first = False
        logging.info(f"Month first {month_first}")
        if prev_data['custom_fields']['TransactionDate'] != "":
            date = get_clean_date(
                prev_data['custom_fields']['TransactionDate'], month_first)
            logging.info(f"date by custom_fields {date}")
        if date == "" and prev_data['default_fields']['TransactionDate'] != "":
            date = get_clean_date(
                prev_data['default_fields']['TransactionDate'], month_first)
            logging.info(f"date by default_fields {date}")
        if date == "":
            date_list = []
            for sample in prev_data['custom_ocr_response'].split("#splitter#"):
                if re.findall(r"\w*\d{1,}\w*", sample):
                    date = get_clean_date(sample, month_first)
                    if date != '':
                        date_list.append(date)
            date_list.sort(key=lambda date: datetime.strptime(
                date, '%Y-%m-%d'), reverse=True)
            for _ in date_list:
                if datetime.now() < parser.parse(_):
                    date_list.remove(_)
            if date_list:
                date = date_list[0]
                logging.info(f"date by ocr  {date}")
            else:
                date = ""

        # today = datetime.today()
        # if date != "" and today < parser.parse(date):
        #     i = 0
        #     while True:
        #         if int(date[-2:]) == (today - timedelta(days=i)).day:
        #             break
        #         i += 1
        #     date = (today - timedelta(days=i)).strftime('%Y-%m-%d')

        # New logic to handle date interchange
        try:
            cri_data = prev_data.get('cri_data', {})
            if cri_data and 'date' in cri_data and cri_data['date']:
                ds = date.split("-")
                cri = cri_data['date'].split("-")
                if len(cri) == 3 and ds[1] == cri[2] and ds[2] == cri[1] and cri[0] == ds[0]:
                    date = cri_data['date']
                    logging.info(f"Customer date take {date}")
            else:
                today = datetime.today()
                if date != "" and today < parser.parse(date):
                    i = 0
                    while True:
                        if int(date[-2:]) == (today - timedelta(days=i)).day:
                            break
                        i += 1
                    date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        except Exception as e:
            logging.warning(f"Error in new logic {e}")

        time = "00:00"
        if prev_data['custom_fields']['TransactionTime'] != "":
            time = prev_data['custom_fields']['TransactionTime']
        elif prev_data['default_fields']['TransactionTime'] != "":
            time = prev_data['default_fields']['TransactionTime']
        else:
            time = "00:00"

        # Clean the Time
        if time != "00:00":
            try:
                time = time.replace(",", " ")
                time = parser.parse(time).strftime("%H:%M")
            except Exception as e:
                logging.info(f"Time parsing failed for {time}")
                time = "00:00"

        if time == "00:00":
            time_found = False
            time_text = prev_data['custom_ocr_response']
            if "time" in time_text.lower():
                time_text = time_text.lower().split("time")[1]
            for sample in time_text.split("#splitter#"):
                time = re.sub("[^\d]", ':', sample)
                time = re.sub('[.;:,a-z ]+$', '', time)
                if len(time) > 0:
                    try:
                        time = re.sub(
                            r'^.*?[0-9]', re.match(r'^.*?[0-9]', time).group(0)[-1], time)
                    except:
                        logging.info(f"time {time}")
                    try:
                        time = parser.parse(time).strftime("%H:%M")
                        time_found = True
                    except Exception as e:
                        print(f"No Time in {time}")
                        time = "00:00"
                if time_found:
                    break
        # Time End

        # InvoiceNumber Starts
        invoice_number = ''
        if 'InvoiceNumber' in prev_data['custom_fields'] and prev_data['custom_fields']['InvoiceNumber'] != "":
            invoice_number = prev_data['custom_fields']['InvoiceNumber']
            # no matter the length we trim and take only first 50 characters
            invoice_number = invoice_number[:50]
        else:
            invoice_number = ''
        cri_data = prev_data.get('cri_data', {})
        
        # # Store name logic for Waitrose
        # if store_name_official.lower() == 'waitrose':
        #     store_name_official = 'WAITROSE'
        try:
            total_amount = str(float(round(total_amount,2)))    
        except:
            total_amount = total_amount
        webhook_response = {
            "code": 0,
            "message": "OK",
            "request_id": prev_data['req_id'],
            "result": {
                "total_amount": total_amount,
                "is_in_dubai_mall": is_in_dubai_mall,
                "store_name": store_name_official,
                "store_id": store_id_official,
                "date": date,
                "time": time,
                "rb_receipt_number": invoice_number,
                "ocr_text": ocr_text.replace("#splitter#", " ")
            }
        }
        raw_response = {
            "request_id": prev_data['req_id'],
            "storeMeta": prev_data['storeMeta'],
            "AmountMeta": amount_meta,
            "result": {
                "total_amount": total_amount,
                "is_in_dubai_mall": is_in_dubai_mall,
                "store_name": store_name_official,
                "store_id": store_id_official,
                "date": date,
                "time": time,
                "rb_receipt_number": invoice_number,
            },
            'cri_data': cri_data,
            'image_url': prev_data.get('image_url', '')
        }
        logging.info(f"Raw Response: {amount_meta}")
        # Call the event GRID
        datatosend = {
            'req_id': prev_data['req_id'],
            'webhook_response': webhook_response,
            'raw_response': raw_response,
            'callback_url': prev_data['callback_url'],
            'x-token': prev_data['x-token'],
            'custom_ocr_response': ocr_text
        }
        result = []
        for i in range(1):
            result.append(EventGridEvent(
                id=uuid.uuid4(),
                # f"DefaultOCRResponse Completed",
                subject=os.environ['EVENTGRID_SUBJECT_4'],
                data=datatosend,
                # 'DefaultOCRResponse',
                event_type=os.environ['EVENTGRID_EVENT_TYPE_4'],
                event_time=datetime.now(),
                data_version=2.0
            ))
        event_grid_client.publish_events(
            os.environ['EVENTGRID_ENDPOINT'],
            events=result
        )
        ent = table_service.get_entity('RequestResponseInfo', 'TDM', rowkey)
        exception = ent.Exceptions
        entity = {'PartitionKey': 'TDM', 'RowKey': rowkey,
                  'RequestID': prev_data['req_id'], 'CurrentStatus': 'BusinessLogic Out/Sent', 'Exceptions': exception, 'DataSent': json.dumps(datatosend), 'RawDataSent': ''}
        
        if sys.getsizeof(json.dumps(datatosend))/1024 <= 30: 
            table_service.merge_entity('RequestResponseInfo', entity)

        logging.info(
            'Python EventGrid trigger processed an event: %s', webhook_response)
    except Exception as e:
        exc = f"Exception in Business Logic: Request Id: {prev_data['req_id']}, error: {e}"
        emailresp = requests.post(
            os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": exc}))

        ent = table_service.get_entity('RequestResponseInfo', 'TDM', rowkey)
        exception = ent.Exceptions + ',Exception in BusinessLogic'
        entity = {'PartitionKey': 'TDM', 'RowKey': rowkey,
                  'RequestID': prev_data['req_id'], 'CurrentStatus': 'Exception in BusinessLogic', 'Exceptions': exception, 'DataSent': '', 'RawDataSent': ''}
        table_service.merge_entity('RequestResponseInfo', entity)
        logging.info(f"Exception in BusinessLogic {e}")
