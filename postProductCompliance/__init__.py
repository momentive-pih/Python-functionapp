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
solr_phrase_translation=pysolr.Solr(solr_url_config+'/sap_phrase_translation/',timeout=10,verify=False)
product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","SUBCT"]
solr_product_column = ",".join(product_column)

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postProductCompliance function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_product_compliance_details(req_body[0])
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

def get_material_details_on_selected_spec(product_rspec,params):
    try:
        query=f'TYPE:MATNBR && TEXT2:{product_rspec}'
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

def get_product_compliance_details(req_body):
    try:
        compliance_details=[]
        result={}
        notification_details=[]
        notify={}
        speclist_data=spec_constructor(req_body)
        sub_category=req_body.get("Category_details").get("Subcategory")
        params={"rows":2147483647}
        response = solr_phrase_translation.search("*:*",**params)
        result = json.dumps(list(response))
        df_phrase_trans=pd.read_json(result,dtype=str) 
        df_phrase_trans["PHRKY"]=df_phrase_trans["PHRKY"].str.strip()
        df_phrase_trans["PTEXT"]=df_phrase_trans["PTEXT"].str.strip()
        for spec,namprod in speclist_data:
            if sub_category=="Notification Status":
                query=f'SUBID:{spec}'
                params={"rows":2147483647,"fl":"NOTIF,ZUSAGE,ADDIN,RLIST"}
                notify_result=list(solr_notification_status.search(query,**params))
                for item in notify_result:
                    notify["regulatory_List"]=str(item.get("RLIST","-")).strip()
                    ntfy_rg=item.get("NOTIF","").strip()
                    ntfy_rg=ntfy_rg.split(";")
                    temp_df=df_phrase_trans[df_phrase_trans["PHRKY"].isin(ntfy_rg)]
                    notify_value=[]
                    if "PTEXT" in list(temp_df.columns):
                        notify_value=list(temp_df["PTEXT"])
                    notify["regulatory_Basis"]=", ".join(notify_value)
                    notify["notification"]=str(item.get("NOTIF","-")).strip()
                    notify["additional_Info"]=str(item.get("ADDIN","-")).strip()
                    notify["usage"]=str(item.get("ZUSAGE","-")).strip()
                    notify["spec_id"]=str(spec)+" - "+str(namprod).strip()
                    notification_details.append(notify)
                    notify={}
            elif sub_category=="AG Registration Status":
                params={"rows":2147483647}
                material_list,bdt_list,matstr,material_details=get_material_details_on_selected_spec(spec,params)
                namlist=namprod.split(", ")
                check_list=namlist+bdt_list
                check_list=list(set(check_list))
                json_make={}
                eu_json_list=[]
                us_json_list=[]
                latin_list=[]
                product_list=[data.replace(" ","\ ") for data in check_list]
                product_query=" || ".join(product_list)
                category_list=["EU_REG_STATUS","US_REG_STATUS","LATAM_REG_STATUS"]
                category_query=" || ".join(category_list)
                query=f'CATEGORY:({category_query}) && IS_RELEVANT:1 && PRODUCT:({product_query})'
                params={"rows":2147483647,"fl":"DATA_EXTRACT,PRODUCT,CATEGORY"}
                ag_result=list(solr_unstructure_data.search(query,**params))
                for item in ag_result:
                    datastr=json.loads(item.get("DATA_EXTRACT"))
                    region=item.get("CATEGORY")
                    if region=="EU_REG_STATUS":                              
                        json_make["product"]=str(item.get("PRODUCT","-")).strip()
                        json_make["country"]=str(datastr.get("Country","-")).strip()
                        json_make["holder"]=str(datastr.get("Holder","-")).strip()
                        json_make["registration"]=str(datastr.get("Registration","-")).strip()
                        json_make["expiry"]=str(datastr.get("Expiry","-")).strip()
                        json_make["status"]=str(datastr.get("Status","-")).strip()
                        json_make["certificate"]=str(datastr.get("Certificate","-")).strip()
                        json_make["spec_id"]=str(spec)+" - "+str(namprod)
                        eu_json_list.append(json_make)
                        json_make={}
                    elif region=="US_REG_STATUS":
                        json_make["product"]=str(item.get("PRODUCT","-")).strip()
                        json_make["EPA_Inert_Product_Listing"]=str(datastr.get("EPA Inert Product Listing","-")).strip()
                        json_make["CA_DPR"]=str(datastr.get("CA DPR","-")).strip()
                        json_make["CP_DA"]=str(datastr.get("CPDA","-")).strip()
                        json_make["WSDA"]=str(datastr.get("WSDA","-")).strip()
                        json_make["OMRI"]=str(datastr.get("OMRI","-")).strip()
                        json_make["OMRI_Reneval_Date"]=str(datastr.get("OMRI Renewal Date","-")).strip()
                        json_make["Canada_OMRI"]=str(datastr.get("Canada OMRI","-")).strip()
                        json_make["PMRA"]=str(datastr.get("PMRA","-")).strip()
                        json_make["spec_id"]=str(spec)+" - "+str(namprod)
                        us_json_list.append(json_make)
                        json_make={}
                    elif region=="LATAM_REG_STATUS":
                        json_make["product"]=str(item.get("PRODUCT","-")).strip()
                        json_make["country"]=str(datastr.get("Country","-")).strip()
                        json_make["registered_Name"]=str(datastr.get("Registered Name","-")).strip()
                        json_make["date_Granted"]=str(datastr.get("Date Granted","-")).strip()
                        json_make["date_Of_Expiry"]=str(datastr.get("Date of Expiry","-")).strip()
                        json_make["registration_Holder"]=str(datastr.get("Holder","-")).strip()
                        json_make["registration_Certificate"]=str(datastr.get("Registration Certificate (Location)","-")).strip()
                        json_make["spec_id"]=str(spec)+" - "+str(namprod)
                        latin_list.append(json_make)
                        json_make={}

        if sub_category=="Notification Status":
            result=notification_details
        if sub_category=="AG Registration Status":
            json_make={}
            json_make["complianceRegistrationEUData"]=eu_json_list
            json_make["complianceRegistrationCanada_Data"]=us_json_list
            json_make["complianceRegistrationLatin_Data"]=latin_list
            result=[json_make]
        return result
    except Exception as e:
        return result
