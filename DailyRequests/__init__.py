import datetime
import logging
import re
import os
import pandas as pd
import requests as requests
from datetime import date, timedelta
import azure.functions as func
from . import email_sender
import jwt

# Credentials to get the data from application insights
# app_id = '96e26b32-a8f6-441a-b961-dcf0bf97c13d'
# key = 'plp4i7hpdqzi8nb1y0id5vw0twzck411cfbycn5v'
# base_url = 'https://api.applicationinsights.io'
headers = {'x-api-key': os.environ['Key']}
# Calculating start time and getting current date

today = date.today()  # Getting Current date
Previous_Date = today - timedelta(days=1)  # Getting previous days date
Url_Date = Previous_Date.strftime('%m/%d/%Y')  # Converting the date into string type and for a particular date formats
XL_Date = Previous_Date.strftime('%d-%m-%Y')  # Converting the date into string type and for a particular date formats

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    # Creating a list for storing the process invoice functions return data from application insights
    Process_Invoice_lst = list()
    Webhook_lst = list()

    # URL to call the application insights API fro Process Invoice function
    search_str = "{'req_id': '"
    try:
        full_url = os.environ['Base_URL'] + '/v1/apps/' + os.environ[
            'Api_id'] + '/query?query=traces| where operation_Name == "process-invoice" and message startswith "DataTobeSent" and timestamp between (datetime("' + Url_Date + ', 12:00:00.000 AM")..datetime("' + Url_Date + ', 11:59:59.999 PM"))'
        resp = requests.get(full_url, headers=headers)
        result = dict(resp.json())
        result = result["tables"][0]["rows"]
        logging.info("Succesfully retrieved data from process invoice")
    except Exception as e:
        logging.info(f"Error in fetching data from process invoice Application Insights: {e}")
    data = re.compile(r"'req_id': '(UBE\d+|\d+)'")  # using regular expression to get Request ids
    # Looping through the rows to get the request ids
    for x in result:
        for y in x:
            if type(y) == str:
                if str(y).startswith("DataTobeSent"):
                    try:
                        Process_Invoice_lst.append(data.findall(y)[0])  # storing the request ids into the list
                    except:
                        logging.info("error in appending list")
    # End of for loop

    df = pd.DataFrame(columns=['RequestID', 'Status', 'Message'])

    # Fetching the data for Webhook response from App insights 
    try:
        full_url1 = os.environ['Base_URL'] + '/v1/apps/' + os.environ[
                'Api_id'] + '/query?query=traces | where operation_Name == "WebHook" and message startswith "Webhook Response" and timestamp between (datetime("' + Url_Date + ', 12:00:00.000 AM")..datetime("' + Url_Date + ', 11:59:59.999 PM"))'
        resp1 = requests.get(full_url1, headers=headers)
        result1 = resp1.json()  # Converting the data into json format
        result1 = result1['tables'][0]['rows']  # Getting row data into a list variable
    except:
        logging.info("Webhook response fetching error")

    # for loop to get all the rows from Webhook response into list

    for row in result1:
        for y in row:
            if str(y).startswith("Webhook Response"):
                Webhook_lst.append(y)

    # End of for loop

    # section for checking the incoming request id with the webhook response

    Flag = 0  # flag for the missed request ids

    for req_id in Process_Invoice_lst:
        for rows in Webhook_lst:
            code = rows.split()
            Text = code[3]
            if req_id == code[2]:
                jwt_options = {'verify_signature': False}
                decode = jwt.decode(Text, algorithms='HS256', options=jwt_options)
                df = df.append({'RequestID': code[2], 'Status': decode['status'], 'Message': decode['message']},
                               ignore_index=True, sort=False)
                Flag = 1
                break
            else:
                Flag = 0
        if Flag == 0:
            df = df.append(
                {'RequestID': req_id, 'Status': "Unsuccess", 'Message': "Receipt was not saved successfully"},
                ignore_index=True, sort=False)
    total = len(list(df["RequestID"]))
    success = len(list(df[(df['Status'] == "success")]['RequestID']))
    failure = len(list(df[(df['Status'] != "success")]['RequestID']))
    # End of for loop         

    try:
        email_sender.send_email(XL_Date,df,total,success,failure,os.environ['Report_Recipients']) 
    except Exception as e:
        logging.info(f"Exception Occured :{e}")

    logging.info("Data saved successfully into excel file")
