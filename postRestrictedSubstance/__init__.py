import logging
import json
import azure.functions as func
import os 
import pysolr
import pandas as pd
solr_url_config="https://52.152.191.13:8983/solr"
#Solar url connection and access
solr_document_variant=pysolr.Solr(solr_url_config+'/sap_document_variant/', timeout=10,verify=False)
solr_unstructure_data=pysolr.Solr(solr_url_config+'/unstructure_processed_data/', timeout=10,verify=False)
solr_product= pysolr.Solr(solr_url_config+"/product_information/", timeout=10,verify=False)
solr_std_composition=pysolr.Solr(solr_url_config+"/sap_standard_composition/",timeout=10,verify=False)
product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","SUBCT"]
solr_product_column = ",".join(product_column)

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postRestrictedSubstance function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_restricted_data_details(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def spec_constructor(req_body):
    try:
        last_specid=''
        namlist=[]
        speclist_data=[]
        spec_body=req_body.get("Spec_id")
        for item in spec_body:           
            spec_details=item.get("name").split(" | ")
            spec_id=spec_details[0]
            namprod=spec_details[1]
            if (last_specid!=spec_id) and last_specid!='':
                namstr=", ".join(namlist)
                speclist_data.append([last_specid,namstr])
                namlist=[]
                namlist.append(namprod)
            else:
                namlist.append(namprod)             
            last_specid=spec_id
        namstr=", ".join(namlist)
        speclist_data.append([last_specid,namstr])
        return speclist_data
    except Exception as e:
        return speclist_data

def querying_solr_data(query,params):
    try:
        df_product_combine=pd.DataFrame()      
        response = solr_product.search(query,**params)
        result = json.dumps(list(response))
        df_product_combine=pd.read_json(result,dtype=str)
        if len(df_product_combine.columns)!=len(product_column):
            dummy=pd.DataFrame([],columns=product_column)
            df_product_combine=pd.concat([df_product_combine,dummy]).fillna("-")
        df_product_combine=df_product_combine.fillna("-")
        return df_product_combine
    except Exception as e:
        return df_product_combine

def get_cas_details_on_selected_spec(product_rspec,params):
    try:
        cas_list=[]
        query=f'TYPE:SUBIDREL && TEXT2:{product_rspec}'
        temp_df=querying_solr_data(query,params)                       
        column_value = list(temp_df["TEXT1"].unique())
        product_list=[data.replace(" ","\ ") for data in column_value]
        product_query=" || ".join(product_list)
        temp_df=pd.DataFrame()
        sub_category="PURE_SUB || REAL_SUB"
        query=f'TYPE:NUMCAS && SUBCT:({sub_category}) && TEXT2:({product_query})'
        temp_df=querying_solr_data(query,params)
        cas_list = list(temp_df["TEXT1"].unique())
        return cas_list
    except Exception as e:
        return cas_list

def  get_restricted_data_details(req_body):
    try:
        speclist_data=spec_constructor(req_body)
        sub_category=req_body.get("Category_details").get("Subcategory")
        cas_list=[]
        gadsl_details=[]
        calprop_details=[]
        for spec,nameprod in speclist_data:
            params={"rows":2147483647,"fl":solr_product_column}
            cas_list=get_cas_details_on_selected_spec(spec,params)
            for cas in cas_list:
                #finding std percentage from composition
                query=f'CAS:{cas}'
                std_wg=''
                params={"rows":2147483647,"fl":"CVALU, CUNIT"}
                weight=list(solr_std_composition.search(query,**params))
                if len(weight)>0:
                    std_wg=str(weight[0].get("CVALU","0"))+" "+str(weight[0].get("CUNIT",""))
                if sub_category=="GADSL":                
                    query=f'CATEGORY:GADSL && IS_RELEVANT:1 && PRODUCT:{cas}'
                    params={"rows":2147483647,"fl":"DATA_EXTRACT"}
                    gadsl=list(solr_unstructure_data.search(query,**params))       
                    if len(gadsl)>0:
                        for item in gadsl:
                            data=json.loads(item.get("DATA_EXTRACT"))
                            gadsl_json={
                                    "substance": data.get("Substance"),
                                    "cas_NO": str(cas),
                                    "class_action": "",
                                    "reason_Code": data.get("Reason Code"),
                                    "source": data.get("Source (Legal requirements, regulations)"),
                                    "reporting_threshold": data.get("Reporting threshold (0.1% unless otherwise stated)"),
                                    "weight_Composition": std_wg,
                                    "spec_Id":str(spec)+" - "+str(nameprod)
                                }
                            gadsl_details.append(gadsl_json)
                    result=gadsl_details
                elif sub_category=="CALPROP":
                    query=f'CATEGORY:CAL-PROP && IS_RELEVANT:1 && PRODUCT:{cas}'
                    params={"rows":2147483647,"fl":"DATA_EXTRACT"}
                    calprop=list(solr_unstructure_data.search(query,**params))       
                    if len(calprop)>0:
                        for item in calprop:
                            data=json.loads(item.get("DATA_EXTRACT"))
                            calprop_json={
                                    "chemical": data.get("Chemical"),
                                    "type_Toxicity": data.get("Type of Toxicity"),
                                    "listing_Mechanism": data.get("Listing Mechanism"),
                                    "cas_NO": str(cas),
                                    "date_Listed": data.get("Date Listed"),
                                    "NSRL_Data": data.get("NSRL or MADL (Ã¦g/day)a"),
                                    "weight_Composition": std_wg,
                                    "spec_Id":str(spec)+" - "+str(nameprod)
                                }
                            calprop_details.append(calprop_json)
                    result=calprop_details
        # if sub_category=="GADSL":
        #     result={"restrictedGASDLData":gadsl_details}
        # if sub_category=="CALPROP":
        #     result={"restrictedCaliforniaData":calprop_details}
        return result
    except Exception as e:
        return []
