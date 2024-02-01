import datetime,aiohttp,os
from posix import environ
import logging,json
from azure.ai.formrecognizer import FormTrainingClient
from azure.core.credentials import AzureKeyCredential
from azure.cosmosdb.table.tableservice import TableService
import azure.functions as func

neendpoint = os.environ['NE_URL']
nekey = os.environ['NE_KEY']

weendpoint = os.environ['WE_URL']
wekey = os.environ['WE_KEY']
source_table_service = TableService(account_name=os.environ['PROD_STORAGE_ACCOUNT'], account_key=os.environ['PROD_STORAGE_KEY'])
target_table_service = TableService(account_name=os.environ['DEV_STORAGE_ACCOUNT'], account_key=os.environ['DEV_STORAGE_KEY'])
                

#Function to get used models in North Europe & West Europe
def getStoredModels():
    global source_table_service
    message = 'failure'
    ne_preview3_models = []
    we_preview3_models = []
    ne_preview3_data = []
    we_preview3_data = []
    try:
        ne_models = source_table_service.query_entities(os.environ['NE_MODELS'])
        for model in ne_models:
            ne_preview3_models.append(model.ModelID)
            ne_preview3_data.append(dict(model))
        we_models = source_table_service.query_entities(os.environ['WE_MODELS'])
        for model in we_models:
            we_preview3_models.append(model.ModelID)
            we_preview3_data.append(dict(model))
        message = 'success'
    except Exception as e:
        logging.info(f"Exception {e}")
        ne_preview3_models = []
        we_preview3_models = []
        message = 'exception'
    return message,ne_preview3_models,we_preview3_models,ne_preview3_data,we_preview3_data

async def main(mytimer: func.TimerRequest) -> None:
    global neendpoint,weendpoint,nekey,wekey
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    try:
        form_training_client_ne = FormTrainingClient(neendpoint, AzureKeyCredential(nekey))
        form_training_client_we = FormTrainingClient(weendpoint, AzureKeyCredential(wekey))

        #List all models from North Europe and West Europe
        custom_models_ne = form_training_client_ne.list_custom_models()
        custom_models_we = form_training_client_we.list_custom_models()

        #Get used models from North Europe and West Europe
        message,ne_stored_models,we_stored_models,ne_stored_data,we_stored_data = getStoredModels()
        logging.info(f"message {message},length of ne_stored_models {len(ne_stored_models)}, length of we_stored_models {len(we_stored_models)}")
        if message == 'success':
            #Clean up Models
            try:
                ne_all_models = []
                we_all_models = []

                #Populate all models from North Europe and West Europe
                for m in custom_models_ne:
                    ne_all_models.append(m.model_id)
                       
                for m in custom_models_we:
                    we_all_models.append(m.model_id)
                    
                logging.info("North Europe: ",len(ne_all_models))
                logging.info("West Europe: ",len(we_all_models))
                logging.info("North Europe used models: ",len(ne_stored_models))
                logging.info("West Europe used models: ",len(we_stored_models))
                
                #Clean up models from North Europe and West Europe which are not used
                for ne_m in ne_all_models:
                    if ne_m not in ne_stored_models:
                        try:
                            form_training_client_ne.delete_model(model_id=ne_m)
                        except Exception as e:
                            logging.info(f"Coud not delete north europe {ne_m}")
                for we_m in we_all_models:
                    if we_m not in we_stored_models:
                        try:
                            form_training_client_we.delete_model(model_id=we_m)
                        except Exception as e:
                            logging.info(f"Coud not delete west europe {we_m}")
            except Exception as e:
                async with aiohttp.ClientSession() as client:
                    async with client.post(os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": "Exception while getting stored models for Azure Table"})) as response:
                        await response.text()
                logging.info(f"Exception during Clean Up of Models {e}")        
            try:
                for ne in ne_stored_data:
                    target_table_service.insert_or_merge_entity(os.environ['NE_BK_MODELS'],ne)
                for we in we_stored_data:
                    target_table_service.insert_or_merge_entity(os.environ['WE_MODELS'],we)
            except Exception as e:
                async with aiohttp.ClientSession() as client:
                    async with client.post(os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": "Exception while getting stored models for Azure Table"})) as response:
                        await response.text()
                logging.info(f"Exception during Back Up of Models {e}")
        else:
            async with aiohttp.ClientSession() as client:
                async with client.post(os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": "Exception while getting stored models for Azure Table"})) as response:
                    await response.text()
            logging.info(f"Exception while getting stored models for Azure Table")    
    except Exception as e:
        logging.info(f"Exception during Clean Up and Backing up of Models {e}")
        async with aiohttp.ClientSession() as client:
            async with client.post(os.environ['EMAIL_EXCEPTION_ENDPOINT'], data=json.dumps({"exception": e})) as response:
                await response.text()
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
