import logging
import json
import azure.functions as func
import pandas as pd
import os 
import pysolr
import datetime

solr_url_config="https://52.152.191.13:8983/solr"
solr_product= pysolr.Solr(solr_url_config+"/product_information/", timeout=10,verify=False)
solr_notification_status=pysolr.Solr(solr_url_config+'/sap_notification_status/', timeout=10,verify=False)
solr_unstructure_data=pysolr.Solr(solr_url_config+'/unstructure_processed_data/', timeout=10,verify=False)
solr_document_variant=pysolr.Solr(solr_url_config+'/sap_document_variant/', timeout=10,verify=False)
solr_ghs_labeling_list_data=pysolr.Solr(solr_url_config+'/sap_ghs_labeling_list_data/', timeout=10,verify=False)
product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","SUBCT"]
solr_product_column = ",".join(product_column)
file_access_path="https://clditdevstorpih.blob.core.windows.net/"

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postReportData function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_assigned_ontology_document(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def spec_constructor(req_body):
    try:
        last_specid=''
        namlist=[]
        speclist_data=[]
        speclist_json={}
        total_namprod=[]
        total_spec=[]
        spec_body=req_body.get("Spec_id")
        for item in spec_body:           
            spec_details=item.get("name").split(" | ")
            spec_id=spec_details[0]
            namprod=spec_details[1]
            if spec_id!='':
                total_spec.append(spec_id)
            if (last_specid!=spec_id) and last_specid!='':
                namstr=", ".join(namlist)
                speclist_json[last_specid]=namstr
                speclist_data.append([last_specid,namstr])
                namlist=[]
                namlist.append(namprod)
                total_namprod.append(namprod)            
            else:
                namlist.append(namprod)  
                total_namprod.append(namprod)           
            last_specid=spec_id
        namstr=", ".join(namlist)
        speclist_json[last_specid]=namstr
        speclist_data.append([last_specid,namstr])
        return speclist_data,speclist_json,list(set(total_spec)),list(set(total_namprod))
    except Exception as e:
        return speclist_data,speclist_json,list(set(total_spec)),list(set(total_namprod))

def get_material_details_on_selected_spec(product_rspec,params):
    try:
        query=f'TYPE:MATNBR && TEXT2:({product_rspec})'
        matinfo=solr_product.search(query,**params)
        matstr=[]
        bdt_list=[]
        material_list=[]
        material_details=[]
        for i in list(matinfo):
            bdt=str(i.get("TEXT3")).strip()
            bdt_list.append(bdt)
            matnumber=str(i.get("TEXT1"))
            material_list.append(matnumber)
            desc=str(i.get("TEXT4"))
            matjson={
                        "bdt":bdt,
                        "material_number":matnumber,
                        "description":desc,
                    }
            if bdt:
                bstr=bdt+" - "+matnumber+" - "+desc
                matstr.append(bstr)
            material_details.append(matjson)
        material_list=list(set(material_list))
        bdt_list=list(set(bdt_list))
        return material_list,bdt_list,matstr,material_details
    except Exception as e:
        return [],[],[],[]

def get_assigned_ontology_document(req_body):
    try:
        sub_category=req_body.get("Category_details").get("Subcategory")
        category_dict=["US-FDA","EU-FDA","Toxicology","CIDP","Toxicology-summary"]
        category_dict={
            "US-FDA":"US FDA",
            "EU-FDA":"EU Food Contact",
            "Toxicology-summary":"Toxic Summary",
            "CIDP":"CIDP",
            "Toxicology":"Toxicology"        
        }
        output_json=[]
        cat_json={
            "US-FDA":{
                "US-FDA":[],
                "category":"US-FDA"
            },"EU-FDA":{
                "EU-FDA":[],"category":"EU-FDA"
            },"Toxicology-summary":{
                "Toxicology-summary":[],"category":"Toxicology-summary"
            },"CIDP":{
                "CIDP":[],"category":"CIDP"
            },"Toxicology":{
                "Toxicology":[],"category":"Toxicology"
            }
        }
        otherfields=["file_path","Date","subject","file_name"]
        last_category=''
        last_filename=''
        last_product=''
        last_updated=''    
        params={"rows":2147483647}
        if sub_category=="assigned":
            json_make={}
            speclist_data,speclist_json,total_spec,total_namprod=spec_constructor(req_body) 
            solr_spec=[data.replace(" ","\ ") for data in total_spec]
            spec_query=" || ".join(solr_spec)    
            material_list,bdt_list,matstr,material_details=get_material_details_on_selected_spec(spec_query,params)
            check_product=bdt_list+total_namprod
            product_list=[data.replace(" ","\ ") for data in check_product]
            product_query=" || ".join(product_list)
            category_list=" || ".join(category_dict)
            query=f'CATEGORY:({category_list}) && IS_RELEVANT:1 && PRODUCT:({product_query})'
            result=list(solr_unstructure_data.search(query,**params))
            count=0
            result_dumps = json.dumps(list(result))
            df_ontology=pd.read_json(result_dumps,dtype=str)
            df_ontology=df_ontology.sort_values(by=['CATEGORY','DATA_EXTRACT'])    
            for item in range(len(df_ontology)):
                category=df_ontology.loc[item,"CATEGORY"]
                datastr=json.loads(df_ontology.loc[item,"DATA_EXTRACT"])
                filename=datastr.get("subject","-")
                product=df_ontology.loc[item,"PRODUCT"]
                path=str(datastr.get("file_path","-")).strip()
                date=datastr.get("Date","-")
                updated=df_ontology.loc[item,"UPDATED"]
                recent_date=datetime.datetime.strptime(updated, '%Y-%m-%d %H:%M:%S.%f')
                extract_field={}                   
                if path.lower().endswith("pdf"):
                    count+=1
                    if last_category==category and last_filename==filename and last_product==product:
                        if recent_date>last_updated:
                            cat_json[category][category].pop()
                    json_make["fileName"]=filename
                    json_make["category"]=category
                    json_make["productName"]=product
                    json_make["id"]=count
                    json_make["createdDate"]=date
                    json_make["url"]=file_access_path+path.replace("/dbfs/mnt/","")
                    extract_field["ontologyKey"]=product
                    for efield in datastr:
                        if efield not in otherfields:
                            extract_field[efield]=datastr.get(efield,"-")
                    json_make["Extract_Field"]=extract_field
                    cat_json[category][category].append(json_make)
                    json_make={}
                    last_category=category
                    last_filename=filename
                    last_product=product
                    last_updated=recent_date
        elif sub_category=="unassigned":
            json_make={}
            category_list=" || ".join(category_dict)
            query=f'CATEGORY:({category_list}) && IS_RELEVANT:0'    
            result=list(solr_unstructure_data.search(query,**params))
            result_dumps = json.dumps(list(result))
            df_ontology=pd.read_json(result_dumps,dtype=str)
            df_ontology=df_ontology.sort_values(by=['CATEGORY','DATA_EXTRACT'])
            count=0
            otherfields=["file_path","Date","subject","file_name"]
            for item in range(len(df_ontology)):
                category=df_ontology.loc[item,"CATEGORY"]
                datastr=json.loads(df_ontology.loc[item,"DATA_EXTRACT"])
                filename=datastr.get("subject","-")
                product=df_ontology.loc[item,"PRODUCT"]
                path=str(datastr.get("file_path","-")).strip()
                date=datastr.get("Date","-")
                updated=df_ontology.loc[item,"UPDATED"]
                recent_date=datetime.datetime.strptime(updated, '%Y-%m-%d %H:%M:%S.%f')
                extract_field={}                   
                if path.lower().endswith("pdf"):
                    count+=1
                    if last_category==category and last_filename==filename and last_product==product:
                        if recent_date>last_updated:
                            cat_json[category][category].pop()
                    json_make["fileName"]=filename
                    json_make["category"]=category
                    json_make["productName"]=product
                    json_make["id"]=count
                    json_make["createdDate"]=date
                    json_make["url"]=file_access_path+path.replace("/dbfs/mnt/","")
                    extract_field["ontologyKey"]=product
                    for efield in datastr:
                        if efield not in otherfields:
                            extract_field[efield]=datastr.get(efield,"-")
                    json_make["Extract_Field"]=extract_field
                    cat_json[category][category].append(json_make)
                    json_make={}
                    last_category=category
                    last_filename=filename
                    last_product=product
                    last_updated=recent_date
        for item in category_dict:
            if len(cat_json.get(item).get(item))>0:
                output_json.append(cat_json.get(item))
        return output_json
    except Exception as e:
        return output_json