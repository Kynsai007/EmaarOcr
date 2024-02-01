import datetime,os
import logging

import azure.functions as func
from azure.cosmosdb.table.tableservice import TableService

table_service = TableService(account_name=os.environ['STORAGE_ACCOUNT_NAME'], account_key=os.environ['STORAGE_ACCOUNT_KEY'])


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('Clear Logs Started!')
    
    today = datetime.datetime.now()
    update_date = today - datetime.timedelta(days=3)
    update_date = update_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    marker = None
    while True:
        reqs = table_service.query_entities("RequestResponseInfo",filter=f"Timestamp lt datetime'{update_date}'",marker=marker,num_results=1000)

        for req in reqs:
            try:
                table_service.delete_entity("RequestResponseInfo",req.PartitionKey,req.RowKey)
            except:
                logging.info(f"Delete entity {req.RowKey} Failed")
        if reqs.next_marker is not None and len(reqs.next_marker) > 0:
            marker = reqs.next_marker
        else:
            break

    logging.info('Cleared logs! ran at %s', utc_timestamp)
