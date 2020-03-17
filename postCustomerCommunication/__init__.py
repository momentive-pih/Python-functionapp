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
solr_sfdc_data=pysolr.Solr(solr_url_config+'/sfdc_identified_case/', timeout=10,verify=False)
product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","SUBCT"]
solr_product_column = ",".join(product_column)
file_access_path="https://clditdevstorpih.blob.core.windows.net/"

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postCustomerCommunication function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_customer_communication_details(req_body[0])
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

def get_customer_communication_details(req_body):
    try:
        result=[]
        speclist_data=spec_constructor(req_body)
        sub_category=req_body.get("Category_details").get("Subcategory")
        json_make={}
        json_list=[]
        result=[]
        query_category={
            "US FDA Letter":"US-FDA",
            "EU Food Contact":"EU-FDA"
        }
        last_category=''
        last_filename=''
        last_product=''
        last_updated=''          
        params={"rows":2147483647,"fl":solr_product_column}
        speclist_data,speclist_json,total_spec,total_namprod=spec_constructor(req_body) 
        solr_spec=[data.replace(" ","\ ") for data in total_spec]
        spec_query=" || ".join(solr_spec)    
        material_list,bdt_list,matstr,material_details=get_material_details_on_selected_spec(spec_query,params)
        check_product=bdt_list+total_namprod
        product_list=[data.replace(" ","\ ") for data in check_product]
        check_product=[data.replace("/","\/") for data in check_product]
        product_query=" || ".join(product_list)
        if sub_category in query_category:
            otherfields=["file_path","Date","subject","file_name"]
            attribute_category=query_category[sub_category]
            query=f'IS_RELEVANT:1 && PRODUCT:({product_query}) && CATEGORY:{attribute_category}'
            params={"rows":2147483647}
            result=list(solr_unstructure_data.search(query,**params))
            if len(result)>0:
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
                                json_list.pop()
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
                    json_list.append(json_make)
                    json_make={}
                    last_category=category
                    last_filename=filename
                    last_product=product
                    last_updated=recent_date
        elif sub_category=="Communication History":
            cas_list,chemical_list,pspec_list=get_cas_details_on_selected_spec(spec_query,params) 
            total_search_product=cas_list+chemical_list+pspec_list+check_product+material_list+total_spec
            all_product_list=[data.replace(" ","\ ") for data in total_search_product if data is not None ]
            all_product_query=" || ".join(all_product_list)
            params={"rows":2147483647}
            details={}
            json_list=[]
            query=f'MATCHEDVALUE:({all_product_query})'
            result=list(solr_sfdc_data.search(query,**params))
            result_dumps = json.dumps(list(result))
            df_sfdc=pd.read_json(result_dumps,dtype=str)
            caselist = list(df_sfdc["CASENUMBER"].unique())
            for case in caselist:
                details["case_Number"]=case
                df_case_check=df_sfdc[df_sfdc["CASENUMBER"]==case]
                # details["manufacturing_plant"]=df_case_check[0,"manufacturing_plant"]
                if len(df_case_check)>0:
                    details["manufacturing_Plant"]=''
                    details["customer_Name"]=''
                    details["bu"]=''
                    indx_value=df_case_check.index
                    details["key"]=df_case_check.loc[indx_value[0],"MATCHEDVALUE"]
                    details["key_Type"]=df_case_check.loc[indx_value[0],"MATCHEDCATEGORY"]
                    details["topic"]=df_case_check.loc[indx_value[0],"REASON"]
                    details["tier_2_Owner"]=df_case_check.loc[indx_value[0],"SOP_TIER_2_OWNER__C"]
                    details["email_Content"]=[]
                    email_json={}
                    for item,row in df_case_check.iterrows():
                        email_json["contact_Email"]=''
                        email_json["email_Subject"]=row["EMAILSUBJECT"]
                        email_json["attached_Docs"]=''
                        email_json["text_Body"]=row["EMAILBODY"]
                        details["email_Content"].append(email_json)
                        email_json={}
                        break
                    json_list.append(details)
                    break
                    details={}
        
        elif sub_category=="Heavy Metals content":
            for specid in speclist_json:
                cas_list,chemical_list,pspec_list=get_cas_details_on_selected_spec(specid,params) 
                total_namprod=speclist_json[specid].split(", ")
                check_product=bdt_list+total_namprod+cas_list
                check_product=[data.replace("/","\/") for data in check_product]
                product_list=[data.replace(" ","\ ") for data in check_product]
                product_query=" || ".join(product_list)
                query=f'IS_RELEVANT:1 && PRODUCT:({product_query}) && CATEGORY:Heavy\ metals'
                params={"rows":2147483647}
                result=list(solr_unstructure_data.search(query,**params))
                for item in result:
                    product=item.get("PRODUCT","")             
                    datastr=json.loads(json.loads(item.get("DATA_EXTRACT","")))
                    json_make["spec_Id"]=specid+"-"+speclist_json[specid]
                    json_make["product"]=product
                    json_make["aka"]=datastr.get("AKA","-")
                    json_make["batch"]=datastr.get("Batch #","-")
                    json_make["sample"]=datastr.get("Sample #","-")
                    json_make["system"]=datastr.get("System","-")
                    json_make["date"]=datastr.get("Date","-")
                    json_make["aluminium_Al"]=datastr.get("Aluminum (Al)","-")
                    json_make["antimony_Sb"]=datastr.get("Antimony (Sb)","-")
                    json_make["arsenic_As"]=datastr.get("Arsenic (As)","-")
                    json_make["barium_Ba"]=datastr.get("Barium (Ba)","-")
                    json_make["beryllium_Be"]=datastr.get("Beryllium (Be)","-")
                    json_make["boron_B"]=datastr.get("Boron (B)","-")
                    json_make["cadmium_Cd"]=datastr.get("Cadmium (Cd)","-")
                    json_make["calcium_Ca"]=datastr.get("Calcium (Ca)","-")
                    json_make["carbon"]=datastr.get("Carbon","-")
                    json_list.append(json_make)
                    json_make={}

        return json_list
    except Exception as e:
        return json_list
                        

