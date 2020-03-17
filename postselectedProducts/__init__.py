import logging
import json
import azure.functions as func
from . import views
# from postAllProducts import views
import os 

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postselectedProducts function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = views.selected_products(req_body)
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")
    
