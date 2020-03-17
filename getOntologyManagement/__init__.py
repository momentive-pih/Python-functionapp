import logging
import json
import azure.functions as func
import pandas as pd
import os 
import pysolr

solr_url_config="https://52.152.191.13:8983/solr"
solr_ontology=pysolr.Solr(solr_url_config+'/ontology/', timeout=10,verify=False)

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postProductCompliance function processing a request.')
        result=[]
        found_data = get_ontology_details()
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def get_ontology_details():
    try:
        params={"rows":2147483647}
        query=f'*:*'
        ontology_json={}
        ontology_list=[]
        result=[]
        ontolgy_result=list(solr_ontology.search(query,**params))
        for item in ontolgy_result:
            ontology_json["key"]=item.get("ONTOLOGY_KEY","-")
            ontology_json["key_Category"]=item.get("KEY_TYPE","-")
            ontology_json["synonyms"]=item.get("ONTOLOGY_VALUE","-")
            ontology_json["created_By"]=item.get("CREATED_BY","-")
            ontology_json["created_Date"]=item.get("CREATED_DATE","-")
            ontology_json["updated_Date"]=item.get("UPDATED_DATE","-")
            ontology_json["synonyms"]=item.get("ONTOLOGY_VALUE","-")
            ontology_list.append(ontology_json)
            ontology_json={}
        result=[{"ontology_Details":ontology_list}]
        return result
    except Exception as e:
        return result


