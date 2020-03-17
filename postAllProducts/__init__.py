import logging
import json
import azure.functions as func
from . import views
import os 

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postAllProducts function processing a request.')
        result=[]
        req_body = req.get_json()
        search_data = req_body.get('SearchData')    
        found_data = views.all_products(search_data)
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

    