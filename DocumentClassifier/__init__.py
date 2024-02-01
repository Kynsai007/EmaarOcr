import json,traceback
import os
import ast
import jwt
import logging
import pickle
import requests
import re
import uuid
from azure.cosmosdb.table.tableservice import TableService
from fuzzywuzzy import fuzz
from azure.storage.blob import BlobServiceClient
from azure.storage.blob._shared_access_signature import BlobSharedAccessSignature
from datetime import datetime, timedelta
import azure.functions as func
from msrest.authentication import TopicCredentials
from azure.eventgrid import EventGridClient
from azure.eventgrid.models import EventGridEvent

accepted_inch = 5*72
accepted_pixel_max = 10000
accepted_pixel_min = 50
accepted_filesize_max = 50

credentials = TopicCredentials(
    # 'XSTPJTytz2rFkp0hIAaONieh5xqDeD/+H6noKFR4sjg='
    os.environ['EVENTGRID_TOPIC_KEY']
)
event_grid_client = EventGridClient(credentials)
# 'DefaultEndpointsProtocol=https;AccountName=emgtdmubeocr;AccountKey=sb9vnHXijdtvvk+FMzLSOGeFpBeBkzQc6og3YQoNvOEbIneCzbHO1Z/9zHFe4S+URpc7Yq5vsxISBsHex4YkUQ==;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(
    os.environ['STORAGE_ACC_CONNECTION_STRING'])
table_service = TableService(
    account_name=os.environ['STORAGE_ACCOUNT_NAME'], account_key=os.environ['STORAGE_ACCOUNT_KEY'])
# Clean Amount


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
    if "." in cleaned_amount or "," in cleaned_amount:
        cleaned_amount = cleaned_amount
    else:
        cleaned_amount = ''
    return cleaned_amount

# predictModel

def checkExcludedKeywords(keyword):
    keywords_to_exclude = ast.literal_eval(os.environ['PHASE_2_KEYWORDS_TO_EXCLUDE'])
    for k in keywords_to_exclude:
        if k in keyword:
            return True
    return False

def classifyUsingKeywords(receipt_id,document, total_amount):
    isloyalty = False
    isStoreCredit = False
    category = ''
    total_loyalty_paid = '0.00'
    total_credit_paid = '0.00'
    try:
        loyalty_payment_keywords = ast.literal_eval(
            os.environ['LOYALTY_PAYMENT_KEYWORDS'])
        store_credit_keywords = ast.literal_eval(
            os.environ['STORE_CREDIT_KEYWORDS'])
        clean_document = document.lower()
        loyalty_obj = {}
        store_credit_obj = {}
        for keyword in loyalty_payment_keywords:
            i = 0
            clean_list = clean_document.split("#splitter#")
            for k in clean_list:
                if keyword in k.strip() and checkExcludedKeywords(k.strip()) == False:
                    # cleanedtext = clean_document.split(keyword)[1]
                    # cleanedtext = cleanedtext.split("#splitter#")
                    total_loyalty_paid = '0.00'
                    if len(clean_list[i]) < 10 and len(clean_list) > i and clean_amount(clean_list[i]) != '' and float(clean_amount(clean_list[i]).replace(",", "")) <= float(total_amount):
                        total_loyalty_paid = clean_amount(
                            clean_list[i]).replace(",", "")
                    elif len(clean_list[i+1]) < 10 and len(clean_list) > (i+1) and clean_amount(clean_list[i+1]) != '' and float(clean_amount(clean_list[i+1]).replace(",", "")) <= float(total_amount):
                        total_loyalty_paid = clean_amount(
                            clean_list[i+1]).replace(",", "")
                    if keyword in loyalty_obj:
                        loyalty_obj[keyword] += float(total_loyalty_paid)
                    else:
                        loyalty_obj.update({keyword: float(total_loyalty_paid)})
                    break
                i = i + 1

        if 'points redeemed' in loyalty_obj and len(loyalty_obj.keys()) > 1:
            loyalty_obj['points redeemed'] = 0.00
        total_loyalty_paid = 0.00
        for l in loyalty_obj:
            total_loyalty_paid += float(loyalty_obj[l])
        logging.info(f"Loyalty Object For Request ID {receipt_id} : {loyalty_obj}")
                    
        if total_loyalty_paid == float(total_amount) and float(total_amount) != 0.00:
            isloyalty = True
            category = 'Loyalty Payment'
        elif total_loyalty_paid > 0.00 and total_loyalty_paid < float(total_amount):
            isloyalty = True
            category = 'Partial Loyalty Payment'
        else:
            isloyalty = False
            category = ''
        if isloyalty == False and len(loyalty_obj.keys()) > 0:
            isloyalty = True
            category = 'Loyalty Payment'
        for keyword in store_credit_keywords:
            i = 0
            clean_list = clean_document.split("#splitter#")
            for k in clean_list:
                if keyword in k.strip() and checkExcludedKeywords(k.strip()) == False:
                    # cleanedtext = clean_document.split(keyword)[1]
                    # cleanedtext = cleanedtext.split("#splitter#")
                    total_credit_paid = '0.00'
                    if len(clean_list[i]) < 10 and len(clean_list) > i and clean_amount(clean_list[i]) != '' and float(clean_amount(clean_list[i]).replace(",", "")) <= (float(total_amount) - total_loyalty_paid):
                        total_credit_paid = clean_amount(
                            clean_list[i]).replace(",", "")
                    elif len(clean_list[i+1]) < 10 and len(clean_list) > (i+1) and clean_amount(clean_list[i+1]) != '' and float(clean_amount(clean_list[i+1]).replace(",", "")) <= (float(total_amount) - total_loyalty_paid):
                        total_credit_paid = clean_amount(
                            clean_list[i+1]).replace(",", "")
                    if keyword in store_credit_obj:
                        store_credit_obj[keyword] += float(total_credit_paid)
                    else:
                        store_credit_obj.update(
                            {keyword: float(total_credit_paid)})
                    break
                i = i + 1

        total_credit_paid = 0.00
        for s in store_credit_obj:
            total_credit_paid += float(store_credit_obj[s])
        logging.info(f"Store Credit Object For Request ID {receipt_id} : {store_credit_obj}")
                    
        if total_credit_paid == float(total_amount) and float(total_amount) != 0.00:
            isStoreCredit = True
            category = 'Store Credit Payment'
        elif total_credit_paid > 0.00 and total_credit_paid < float(total_amount):
            isStoreCredit = True
            category = 'Partial Store Credit Payment' if category == '' else category + \
                ' and Partial Store Credit Payment'
        else:
            isStoreCredit = False
            category = '' if category == '' else category
        if isStoreCredit == False and len(store_credit_obj.keys()) > 0:
            isStoreCredit = True
            category = 'Store Credit Payment'

    except Exception as e:
        isloyalty = False
        isStoreCredit = False
        category = ''
        total_loyalty_paid = 0.00
        total_credit_paid = 0.00
    return isloyalty, isStoreCredit, category, total_loyalty_paid, total_credit_paid


def main(event: func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })
    prev_data = event.get_json()
    logging.info(f"Received: {prev_data}")
    receipt_id = prev_data['req_id']
    ocr_text = prev_data['custom_ocr_response']
    total_amount = prev_data['raw_response']['result']['total_amount']
    date = prev_data["raw_response"]["result"]["date"]
    if total_amount:
        total_amount = total_amount.replace(",", "")
    else:
        total_amount = ""
    is_float = False
    try:
        total_amount = float(total_amount)
        is_float = True
    except:
        total_amount = 0.00
        #image_url = prev_data['raw_response']['image_url']
    try:
        isloyalty, isStorecredit, category_from_keywords, loyalty_value, store_credit_value = classifyUsingKeywords(
            receipt_id,ocr_text, total_amount)
        #cleandoc = clean_document(ocr_text)
        #logging.info("Classifier Predicting...")
        #category = predictUsingNBModel(cleandoc)
        webhook_response = prev_data['webhook_response']
        raw_response = prev_data['raw_response']
        if is_float:
            webhook_response['result']['total_amount'] = str(
                total_amount - loyalty_value - store_credit_value)
            raw_response['result']['total_amount'] = webhook_response['result']['total_amount']
        cri_data = prev_data['raw_response']['cri_data']
        auto_approve = False
        ap_amount_limit_check = True
        ap_date_check = True
        date_less_than_14_days_check = True
        ap_amount_check =True
        ap_store_check = True
        ap_loyalty_check = True
        invalid_auto_approve_receipt = False
        random_sampling = False
        notAutoApproveReason = []
        try:
            target_date = datetime.strptime(date, '%Y-%m-%d')
        except:
            target_date = datetime.now()
        diff = datetime.now() - target_date
        diff_days = diff.days
        if 'date' in cri_data and cri_data['date'] and 'store' in cri_data and cri_data['store'] and 'amount' in cri_data and cri_data['amount']:
            try:
                cri_data['amount'] = cri_data['amount'].replace(",",".")
                if raw_response['result']['total_amount'] == '':
                    raw_response['result']['total_amount'] = '0.00'
                if raw_response['AmountMeta']['GotFrom'] != "CustomerData" and float(raw_response['result']['total_amount']) <= 10000.00 and cri_data['date'] == raw_response['result']['date'] and float(cri_data['amount']) == float(raw_response['result']['total_amount']) and cri_data['store'] == raw_response['result']['store_id'] and raw_response['result']['is_in_dubai_mall'] == True and isloyalty == False and isStorecredit == False:
                    count = table_service.get_entity('AutoApprovedCount',partition_key='TDM',row_key='1').Count
                    count = int(count) + 1
                    countentity = table_service.get_entity('AutoApprovedCount',partition_key='TDM',row_key='1') 
                    countentity.Count = count
                    auto_approve = True
                    if count > 5:
                        countentity.Count = '0'
                    if count == 5:
                        random_sampling = True
                        logging.info(f"Receipt for Random Sampling : {receipt_id}")
                    else:
                        random_sampling = False
                        logging.info(f"Skip for Random Sampling : {receipt_id}")
                    table_service.update_entity('AutoApprovedCount',countentity)
                   
                else:
                    if float(raw_response['result']['total_amount']) > 10000.00:
                        ap_amount_limit_check = False
                        notAutoApproveReason.append('Amount Greater than 10000')
                    if  cri_data['date'] != raw_response['result']['date']:
                        ap_date_check = False
                        notAutoApproveReason.append("Customer Date is not Equal to OCR Date")
                    if diff_days > 14:
                        date_less_than_14_days_check = False
                        notAutoApproveReason.append("Date is more than 14 days")
                    if float(cri_data['amount']) != float(raw_response['result']['total_amount']):
                        ap_amount_check = False
                        notAutoApproveReason.append("Customer Amount is not Equal to OCR Amount")
                    if cri_data['store'] != raw_response['result']['store_id']:
                        ap_store_check = False
                        notAutoApproveReason.append("Customer Store Id is not Equal to OCR Store Id")
                    if isloyalty or isStorecredit:
                        ap_loyalty_check = False
                        notAutoApproveReason.append("Receipt contains Loyalty Rewards payment")
                    auto_approve = False
                if auto_approve == True:
                    rejectionKeywordslist = [{'keywords':['reprint'],'rejectionreason':'Reprint receipt'},{'keywords':['duplicate'],'rejectionreason':'Duplicate Tax Invoice'},{'keywords':['approval code'],'rejectionreason':'Approval Code Present'},{'keywords':['auth code','auth. code'],'rejectionreason':'Bank Slip'},{'keywords':['tax credit note'],'rejectionreason':'Tax credit note receipt'},{'keywords':['not an original'],'rejectionreason':'Duplicate Receipt'}]
                    for v in rejectionKeywordslist:
                        for keyword in v["keywords"]:
                            if keyword in ocr_text.lower():
                                auto_approve = False
                                invalid_auto_approve_receipt = True
                                notAutoApproveReason.append(v['rejectionreason'])

            except Exception as e:
                logging.info(f"Exception {e}")
                auto_approve = False
        else:
            notAutoApproveReason.append('Customer Data not Found')
        if auto_approve == False:
            logging.info(f"Not Auto Approved : Requests ID {receipt_id}, Reason : {','.join(notAutoApproveReason)}")
            
        receipt_status = {
            'has_loyalty_payment': isloyalty,
            'has_store_credit_payment': isStorecredit,
            'reason_text': category_from_keywords,
            'loyalty_redeemed': loyalty_value,
            'store_credit_redeemed': store_credit_value
        }
        
        raw_response['receipt_status'] = receipt_status
        raw_response['auto_approve'] = auto_approve
        raw_response['auto_approve_reject_reason'] = ",".join(notAutoApproveReason)
        raw_response['random_sampling'] = random_sampling
        raw_response['auto_approval_amount_limit_check'] = ap_amount_limit_check
        raw_response['auto_approval_store_match_check'] = ap_store_check
        raw_response['auto_approval_date_match_check'] = ap_date_check
        raw_response['auto_approval_amount_match_check'] = ap_amount_check
        raw_response['auto_approval_loyalty_reward_check'] = ap_loyalty_check
        raw_response['invalid_auto_approve_receipt'] = invalid_auto_approve_receipt
        webhook_response['result']['receipt_status'] = receipt_status
        webhook_response['result']['auto_approve'] = auto_approve
        webhook_response['result']['auto_approve_reject_reason'] = ",".join(notAutoApproveReason)
        webhook_response['result']['auto_approval_amount_limit_check'] = ap_amount_limit_check
        webhook_response['result']['random_sampling'] = random_sampling
        webhook_response['result']['auto_approval_store_match_check'] = ap_store_check
        webhook_response['result']['auto_approval_date_match_check'] = ap_date_check
        webhook_response['result']['auto_approval_amount_match_check'] = ap_amount_check
        webhook_response['result']['auto_approval_loyalty_reward_check'] = ap_loyalty_check
        webhook_response['result']['invalid_auto_approve_receipt'] = invalid_auto_approve_receipt
        result = []
        algorithm = 'HS256'
        private_key = os.environ['JWT_PRIVATE_KEY']
        jwt_token = jwt.encode(
            webhook_response, key=private_key, algorithm=algorithm)
        datatosend = {
            'req_id': receipt_id,
            'webhook_response': jwt_token,
            'raw_response': raw_response,
            'callback_url': prev_data['callback_url'],
            'x-token': prev_data['x-token'],
        }
        for i in range(1):
            result.append(EventGridEvent(
                id=uuid.uuid4(),
                # f"BusinessLogic Calling",
                subject=os.environ['EVENTGRID_SUBJECT_3'],
                data=datatosend,
                # 'BusinessLogic',
                event_type=os.environ['EVENTGRID_EVENT_TYPE_3'],
                event_time=datetime.now(),
                data_version=2.0
            ))
        # prev_data['form_recognizer_response']|"{'sample':''}",
        # custom_data, ||"{'sample':''}",
        event_grid_client.publish_events(
            os.environ['EVENTGRID_ENDPOINT'],
            events=result
        )
        # if 'Loyalty payment' in official_reason or 'Store credit payment' in official_reason:
        # 	new_data = {'Receipt ID':receipt_id,'Image':image_url,'Reason':official_reason if len(official_reason.split(";")) == 0 else official_reason.split(";")[0],'OCR Text':cleandoc}
        # 	append_status = appendTrainingData(new_data)
        # 	if append_status == "success":
        # 		score = trainSaveNBModel()
        # 		logging.info(f"Re-Training score: {score}"
    except Exception as e:
        logging.info(f"Exception in Document Classifier: {traceback.format_exc()}")
        logging.info(f'Python EventGrid trigger processed an event: {result}')
        if "Connection aborted" in str(e):
            raise
