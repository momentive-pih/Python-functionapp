import logging
import json
import azure.functions as func
import pandas as pd
import os 
import pysolr

solr_url_config="https://52.152.191.13:8983/solr"
solr_product= pysolr.Solr(solr_url_config+"/product_information/", timeout=10,verify=False)
solr_notification_status=pysolr.Solr(solr_url_config+'/sap_notification_status/', timeout=10,verify=False)
solr_unstructure_data=pysolr.Solr(solr_url_config+'/unstructure_processed_data/', timeout=10,verify=False)
solr_document_variant=pysolr.Solr(solr_url_config+'/sap_document_variant/', timeout=10,verify=False)
solr_ghs_labeling_list_data=pysolr.Solr(solr_url_config+'/sap_ghs_labeling_list_data/', timeout=10,verify=False)
solr_ontology=pysolr.Solr(solr_url_config+'/ontology/',timeout=10,verify=False)
solr_registration_tracker=pysolr.Solr(solr_url_config+'/registration_tracker/',timeout=10,verify=False)
product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","SUBCT"]
solr_product_column = ",".join(product_column)
file_access_path="https://clditdevstorpih.blob.core.windows.net/"

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postToxicology function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_toxicology_details(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

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
                namlist=[]
                namlist.append(namprod)
                total_namprod.append(namprod)            
            else:
                namlist.append(namprod)  
                total_namprod.append(namprod)           
            last_specid=spec_id
        namstr=", ".join(namlist)
        speclist_json[last_specid]=namstr
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

def get_cas_details_on_selected_spec(product_rspec,params):
    try:
        cas_list=[]
        query=f'TYPE:SUBIDREL && TEXT2:({product_rspec})'
        temp_df=querying_solr_data(query,params)                       
        column_value = list(temp_df["TEXT1"].unique())
        product_list=[data.replace(" ","\ ") for data in column_value]
        product_query=" || ".join(product_list)
        temp_df=pd.DataFrame()
        sub_category="PURE_SUB || REAL_SUB"
        query=f'TYPE:NUMCAS && SUBCT:({sub_category}) && TEXT2:({product_query})'
        temp_df=querying_solr_data(query,params)
        cas_list = list(temp_df["TEXT1"].unique())
        chemical_list= list(temp_df["TEXT3"].unique())
        return cas_list,chemical_list,column_value
    except Exception as e:
        return cas_list

def get_toxicology_details(req_body):
    try:
        speclist_data,speclist_json,total_spec,total_namlist=spec_constructor(req_body)
        json_make={}
        toxicology_result={}
        json_list=[]
        for specid in speclist_json:
            params={"rows":2147483647,"fl":solr_product_column}
            material_list,bdt_list,matstr,material_details=get_material_details_on_selected_spec(specid,params)
            cas_list,chemical_list,pspec_list=get_cas_details_on_selected_spec(specid,params)
            total_namprod=(speclist_json[specid]).split(", ")
            sub_category=req_body.get("Category_details").get("Subcategory")
            check_product=bdt_list+total_namprod+cas_list
            product_list=[data.replace(" ","\ ") for data in check_product if data!="None"]
            product_query=" || ".join(product_list)
            if sub_category=="Study Title and Date":
                #product_check
                query=f'IS_RELEVANT:1 && PRODUCT:({product_query}) && CATEGORY:Toxicology'
                params={"rows":2147483647}
                result=list(solr_unstructure_data.search(query,**params))
                for data in result:
                    product=data.get("PRODUCT")
                    datastr=json.loads(data.get("DATA_EXTRACT"))
                    json_make["product_Name"]=product
                    json_make["ELA"]=''
                    json_make["spec_Id"]=str(specid)+"-"+speclist_json[specid]
                    json_make["test_Description"]=""
                    json_make["study_Title"]=datastr.get("Study Title","")
                    json_make["final_Report"]=datastr.get("Issue Date","")
                    json_list.append(json_make)
                    json_make={}
                #finding ela/Y number
                query=f'ONTOLOGY_KEY:({product_query})'
                params={"rows":2147483647}
                ela_key_json={}
                ela_key_list=[]
                ela_list=[]
                result=list(solr_ontology.search(query,**params))
                if len(result)>0:
                    result_dumps = json.dumps(result)
                    df_ela=pd.read_json(result_dumps,dtype=str)
                    ela_key_list=list(df_ela[["ONTOLOGY_VALUE","ONTOLOGY_KEY"]])
                    for ela,key in ela_key_list:
                        ela_list.append(ela)
                        ela_key_json[ela]=key
                    ela_list=list(set(ela_list))
                    product_list=[data.replace(" ","\ ") for data in ela_list]
                    product_query=" || ".join(product_list)
                    query=f'IS_RELEVANT:1 && PRODUCT:({product_query}) && CATEGORY:Toxicology'
                    params={"rows":2147483647}
                    result=list(solr_unstructure_data.search(query,**params))
                    for data in result:
                        product=data.get("PRODUCT")
                        datastr=json.loads(data.get("DATA_EXTRACT"))
                        json_make["product_Name"]=ela_key_json[product]
                        json_make["ELA"]=product
                        json_make["spec_Id"]=str(specid)+"-"+speclist_json[specid]
                        json_make["test_Description"]=""
                        json_make["study_Title"]=datastr.get("Study Title","")
                        json_make["final_Report"]=datastr.get("Issue Date","")
                        json_list.append(json_make)
                        json_make={}
                # toxicology_result["study_Title_And_Date"]=json_list
            elif sub_category=="Monthly Toxicology Study List":
                #product_check
                params={"rows":2147483647}
                selant=[]
                silanes=[]
                query=f'IS_RELEVANT:1 && PRODUCT:({product_query}) && CATEGORY:(tox_study_selant || tox_study_silanes)'
                params={"rows":2147483647}
                result=list(solr_unstructure_data.search(query,**params))
                for data in result:
                    if data["CATEGORY"]=="tox_study_silanes":
                        product=data.get("PRODUCT")
                        datastr=json.loads(data.get("DATA_EXTRACT"))
                        json_make["product_Commercial_Name"]=product
                        json_make["spec_Id"]=str(specid)+"-"+speclist_json[specid]
                        json_make["studies"]=datastr.get("Studies","-")
                        json_make["status"]=datastr.get("Status","-")
                        json_make["comments"]=datastr.get("Comments","-")
                        silanes.append(json_make)
                        json_make={}
                    else:
                        product=data.get("PRODUCT")
                        datastr=json.loads(data.get("DATA_EXTRACT"))
                        json_make["spec_Id"]=str(specid)+"-"+speclist_json[specid]
                        json_make["product"]=product
                        json_make["test"]=datastr.get("Test","-")
                        json_make["actions"]=datastr.get("Actions","-")
                        json_make["date"]=datastr.get("date","-")
                        selant.append(json_make)
                        json_make={}
                json_list=[]
                json_make["selant"]=selant
                json_make["silanes"]=silanes
                json_list.append(json_make)
            elif sub_category=="Toxicology Summary":
                params={"rows":2147483647}
                query=f'IS_RELEVANT:1 && PRODUCT:({product_query}) && CATEGORY:(Toxicology-summary)'
                params={"rows":2147483647}
                result=list(solr_unstructure_data.search(query,**params))
                for data in result:
                    product=data.get("PRODUCT")
                    datastr=json.loads(data.get("DATA_EXTRACT"))
                    path=datastr.get("file_path","-")
                    json_make["date_Of_Issue"]=datastr.get("Date","-")
                    json_make["filename"]=file_access_path+path.replace("/dbfs/mnt/","")
                    json_make["spec_Id"]=str(specid)+"-"+speclist_json[specid]
                    json_list.append(json_make)
                    json_make={}
                # toxicology_result["toxicology_Summary"]=json_list
            elif sub_category=="Toxicology Registration Tracker":
                product_mapping={}
                bdt_out=[]
                namprod_out=[]
                namsyn_out=[]
                ontology_out=[]
                params={"rows":2147483647,"fl":solr_product_column}
                query=f'TYPE:NAMPROD && TEXT2:({specid}) && SUBCT:REAL_SUB'
                temp_df=querying_solr_data(query,params) 
                if 'TEXT3' in  list(temp_df.columns): 
                    namsyn_list = list(temp_df["TEXT3"].unique())
                #checking ontology in registration tracker
                result=[]
                total_search=bdt_list+total_namprod+namsyn_list+cas_list
                product_list=[data.replace(" ","\ ") for data in total_search if (data!="None" and data!="-")]
                total_query=" || ".join(product_list)
                query=f'ONTOLOGY_KEY:({total_query})'
                result=list(solr_ontology.search(query,**params))
                result_dumps = json.dumps(result)
                df_ela=pd.read_json(result_dumps,dtype=str)
                ela_key_list=[]
                if "ONTOLOGY_VALUE" in list(df_ela.columns):
                    ela_key_list=list(df_ela["ONTOLOGY_VALUE"].unique())
                for value in bdt_list:
                    if value!='-':
                        product_mapping[value]="BDT"
                for value in total_namprod:
                    if value!='-':
                        product_mapping[value]="NAMPROD"
                for value in namsyn_list:
                    if value!='-':
                        product_mapping[value]="NAMSYN"
                for value in cas_list:
                    if value!='-':
                        product_mapping[value]="CASNUMBER"
                for value in ela_key_list:
                    if value!='-':
                        product_mapping[value]="ONTOLOGY"
                total_search=total_search+ela_key_list
                #checking in registration tracker
                product_list=[data.replace(" ","\ ") for data in total_search if data!="None" and  data!="-"]
                total_query=" || ".join(product_list)
                query=f'PRODUCTNAME:({total_query})'
                params={"rows":2147483647}
                result=list(solr_registration_tracker.search(query,**params))
                json_list=[]
                for data in result:
                    json_make["spec_Id"]=str(specid)+"-"+speclist_json[specid]
                    json_make["product_Name"]=data.get("PRODUCTNAME","-")
                    json_make["product_Type"]=product_mapping.get(data.get("PRODUCTNAME","-"))
                    json_make["country_Name"]=data.get("COUNTRYNAME","-")
                    json_make["tonnage_Band"]=data.get("TONNAGEBAND","-")
                    json_make["study_Type"]=data.get("STUDYTYPE","-")
                    json_make["test_Method"]=data.get("TESTMETHOD","-")
                    json_make["test_Name"]=data.get("TESTNAME","-")
                    json_make["estimated_Timing"]=data.get("ESTIMATEDTIMING","-")
                    json_make["estimated_Cost"]=data.get("ESTIMATEDCOST","-")
                    json_make["new_Estimates"]=data.get("NEWESTIMATES","-")
                    json_list.append(json_make)
                    json_make={}
                # toxicology_result["registartion_Tracker"]=json_list
        return json_list
    except Exception as e:
        
        return []