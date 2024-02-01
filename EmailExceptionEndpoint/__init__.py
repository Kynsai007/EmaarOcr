import logging
import os
from . import email_sender

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    exception = req.get_json()
    try:
        value = exception['exception']
        if "emgdevtdmubeocr" in os.environ["EMAIL_EXCEPTION_ENDPOINT"]:
            system = "DEVELOPMENT System"
        else:
            system = "PRODUCTION System"
        logging.info(f"{system} - Emaar OCR Exception Occurred-{value}")
        receipients = [{"emailAddress": {"address": rec}} for rec in os.environ["EMAIL_ALERT_RECIPIENTS"].split(",")]
        body = f"<html><body>{value}</body></html>"
        email_sender.send_email_using_oauth(os.environ['EMAIL_ALERT_SENDER'],receipients,f"{system} - Emaar OCR Exception Occurred","HTML",body)
        return func.HttpResponse(f"Sent")
    except Exception as e:
        return func.HttpResponse(f"Exception {e}")
