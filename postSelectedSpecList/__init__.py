import logging
import json
import azure.functions as func
from . import get_spec_list
# from postAllProducts import views
import os 

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postSelectedSpecList function processing a request.')
        result=[]
        req_body = req.get_json()
        specid_list,namprod_list,specid_details = get_spec_list.find_specid(req_body)
        result = json.dumps(specid_list)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")


    
