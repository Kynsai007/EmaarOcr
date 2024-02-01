import logging
import os,base64
from io import BytesIO
import pandas as pd
def send_email(start_date,df,percentage_df,percentage_df_2000,percentage_df_4000,percentage_df_6000,percentage_df_10000,false_req_df_1000,false_req_df_2000,false_req_df_4000,false_req_df_6000,false_req_df_10000,receipients):
    # FILE TO SEND AND ITS PATH
    try:
        filename = f"Daily-AutoApproval-Report-{start_date}.xlsx"
        body = f"""<html>
                    <body>
                    <h2>Auto Approval Report</h2>
                    </body>
                </html>"""
        base = ""
        with BytesIO() as buffer:
            writer = pd.ExcelWriter(buffer)
            df.to_excel(writer, sheet_name=f'Report', index=False)
            percentage_df.to_excel(writer, sheet_name=f'Summary<=1000', index=False)
            percentage_df_2000.to_excel(writer, sheet_name=f'Summary<=2000', index=False)
            percentage_df_4000.to_excel(writer, sheet_name=f'Summary<=4000', index=False)
            percentage_df_6000.to_excel(writer, sheet_name=f'Summary<=6000', index=False)
            percentage_df_10000.to_excel(writer, sheet_name=f'Summary<=10000', index=False)
            
            false_req_df_1000.to_excel(writer, sheet_name=f'False Positives List<=1000', index=False)
            false_req_df_2000.to_excel(writer, sheet_name=f'False Positives List<=2000', index=False)
            false_req_df_4000.to_excel(writer, sheet_name=f'False Positives List<=4000', index=False)
            false_req_df_6000.to_excel(writer, sheet_name=f'False Positives List<=6000', index=False)
            false_req_df_10000.to_excel(writer, sheet_name=f'False Positives List<=10000', index=False)
            
            writer.save()
            base = buffer.getvalue()
        receivers = [{"emailAddress": {"address": r}} for r in receipients.split(",")]
        encoded_attachment_content = base64.b64encode(base).decode("utf-8")
        attachments = [{"@odata.type": "#Microsoft.Graph.FileAttachment","name": filename,"contentBytes": encoded_attachment_content}]
        send_email_using_oauth(os.environ["EMAIL_ALERT_SENDER"],receivers,f"Daily Auto-Approval Report - {start_date}","HTML",body,attachments)
        return "success"
    except Exception as e:
        logging.info(f"Failed to send Email {e}")
        return "exception in email_sender"

import requests

def get_token():
    tenant = "86fb359e-1360-4ab3-b90d-2a68e8c007b9"
    url = "https://login.microsoftonline.com/{}/oauth2/v2.0/token".format(tenant)
    grant_type = "client_credentials"
    client_id = "44e503fe-f768-46f8-99bf-803d4a2cf62d"
    client_secret = "aDb8Q~gTHy6kyWZFUgCAGSZXV90QdAWKRuZSJa_H"
    scope = "https://graph.microsoft.com/.default"

    body = {
        "grant_type":grant_type,
        "client_id":client_id,
        "client_secret":client_secret,
        "scope":scope,
        "tenant": tenant
    }
    resp = requests.post(url,data=body)
    if resp.status_code == 200:
        message = 'success'
        access_token = resp.json()['access_token']
        expires_in = resp.json()['expires_in']
    else:
        message = 'failure'
        access_token = ""
        expires_in = 0
    return {"message":message,"access_token":access_token,"expires_in":expires_in}


def send_email_using_oauth(sender,receipients,subject,contentType,content,attachments):
    resp = get_token()
    if resp["message"] == "success":
        url = "https://graph.microsoft.com/v1.0/users/{}/sendMail".format(sender)

        headers = {
            "Authorization": "Bearer " + resp["access_token"],
            "Content-Type": "application/json"
        }

        data = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": contentType,
                    "content": content
                },
                "toRecipients": receipients,
                "attachments": attachments
            },
            "saveToSentItems": "true"
        }

        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 202:
            return "success"
        else:
            return "failed"
    return "failed"