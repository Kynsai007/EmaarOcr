import datetime
import logging
import azure.functions as func
import requests

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    try:
        resp = requests.get("https://emgtdmubeocrapi.azurewebsites.net/api/process-invoice?req_id=coldstart")
        logging.info("cold start trigger")
    except Exception as e:
        logging.error(f"Failed to fetch stores due to {e}")
