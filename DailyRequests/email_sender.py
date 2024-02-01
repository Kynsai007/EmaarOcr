import logging
import os,base64
from io import BytesIO
import pandas as pd
def send_email(XL_Date,df,total,success,failure,receipients):
    # FILE TO SEND AND ITS PATH
    try:
        filename = f"DailyRequestsReport{XL_Date}.xlsx"
        body = f"""<html>
                    <body>
                    <h2>Daily Requests Report for {XL_Date}</h2>
                    <table>
                        <tr>
                            <th>Total Requests</th>
                            <th>Successful</th>
                            <th>Failed</th>
                        </tr>
                        <tr>
                            <td>{total}</td>
                            <td>{success}</td>
                            <td>{failure}</td>
                        </tr>
                    </table>
                    </body>
                </html>"""
        base = ""
        with BytesIO() as buffer:
            writer = pd.ExcelWriter(buffer)
            df.to_excel(writer, sheet_name='Missing_Requests', index=False)
            writer.save()
            base = buffer.getvalue()
        receivers = [{"emailAddress": {"address": r}} for r in receipients.split(",")]
        encoded_attachment_content = base64.b64encode(base).decode("utf-8")
        attachments = [{"@odata.type": "#Microsoft.Graph.FileAttachment","name": filename,"contentBytes": encoded_attachment_content}]
        send_email_using_oauth(os.environ["EMAIL_ALERT_SENDER"],receivers,f"Sending DailyRequests Report - {XL_Date}","HTML",body,attachments)
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