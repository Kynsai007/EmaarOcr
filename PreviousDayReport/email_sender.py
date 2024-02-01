import logging,math
import os,base64
from io import BytesIO
import pandas as pd
def send_email(update_date,All_Request,Store_All,All_Match,Store_Match,Date_Match,Amount_Match,receipients,result,accuracy_df,store_df,amount_df,date_df,time_df,df_2,df_3):
    # FILE TO SEND AND ITS PATH
    try:
        filename = f"DailyReport{update_date.strftime('%Y-%m-%d')}.xlsx"
        body = f"""<html>
                    <body>
                    <h2>Automated OCR Report DS</h2>
                    <h3>Accuracy Table</h3>
                    <h5>Total Requests (Duplicates+Rejected) : {len(All_Request)}</h5>
                    <h5>Accepted Requests : {len(Store_All)}</h5>
                    <h5>Matched Requests : {len(All_Match)}</h5>
                    <table style="width:100%">
                        <tr>
                            <th>Accuracy For</th>
                            <th>Accuracy</th>
                        </tr>
                        <tr>
                            <td style="text-align:center">Overall</td>
                            <td style="text-align:center">{str(math.ceil((len(All_Match)/len(Store_All)*100)))+" %"}</td>
                        </tr>
                        <tr>
                            <td style="text-align:center">Store</td>
                            <td style="text-align:center">{str(math.ceil((len(Store_Match)/len(Store_All)*100)))+" %"}</td>
                        </tr>
                        <tr>
                            <td style="text-align:center">Date</td>
                            <td style="text-align:center">{str(math.ceil((len(Date_Match)/len(Store_All)*100)))+" %"}</td>
                        </tr>
                        <tr>
                            <td style="text-align:center">Amount</td>
                            <td style="text-align:center">{str(math.ceil((len(Amount_Match)/len(Store_All)*100)))+" %"}</td>
                        </tr>
                    </table>
                    </body>
                </html>"""
        base = ""
        with BytesIO() as buffer:
            writer = pd.ExcelWriter(buffer)
            result.to_excel(writer, sheet_name='Daily Report', index=False)
            accuracy_df.to_excel(
                writer, sheet_name='Indivitual Accuracy', index=False)
            store_df.to_excel(writer, sheet_name='Store Miss', index=False)
            amount_df.to_excel(writer, sheet_name='Amount Miss', index=False)
            date_df.to_excel(writer, sheet_name='Date Miss', index=False)
            time_df.to_excel(writer, sheet_name='Time Miss', index=False)
            df_2.to_excel(writer, sheet_name='Emaar Backlog', index=False)
            df_3.to_excel(writer, sheet_name='DS Baklog', index=False)
            writer.save()
            base = buffer.getvalue()
        receivers = [{"emailAddress": {"address": r}} for r in receipients.split(",")]
        encoded_attachment_content = base64.b64encode(base).decode("utf-8")
        attachments = [{"@odata.type": "#Microsoft.Graph.FileAttachment","name": filename,"contentBytes": encoded_attachment_content}]
        send_email_using_oauth(os.environ["EMAIL_ALERT_SENDER"],receivers,f"Automated Daily Accuracy Report DS x TDM - {update_date.strftime('%Y-%m-%d')}","HTML",body,attachments)
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