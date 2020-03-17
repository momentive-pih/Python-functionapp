import logging
import json
import azure.functions as func
import pandas as pd
# from postAllProducts import views
import os 
import pysolr

solr_url_config="https://52.152.191.13:8983/solr"
solr_product= pysolr.Solr(solr_url_config+"/product_information/", timeout=10,verify=False)
solr_notification_status=pysolr.Solr(solr_url_config+'/sap_notification_status/', timeout=10,verify=False)
solr_unstructure_data=pysolr.Solr(solr_url_config+'/unstructure_processed_data/', timeout=10,verify=False)
solr_document_variant=pysolr.Solr(solr_url_config+'/sap_document_variant/', timeout=10,verify=False)
solr_ghs_labeling_list_data=pysolr.Solr(solr_url_config+'/sap_ghs_labeling_list_data/', timeout=10,verify=False)
product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","SUBCT"]
solr_product_column = ",".join(product_column)

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postBasicProperties function processing a request.')
        result=[]
        req_body = req.get_json()       
        basic_details=get_basic_properties_details(req_body)
        result = json.dumps(basic_details)
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

def basic_spec_constructor(req_body):
    try:
        last_specid=''
        namlist=[]
        synlist=[]
        speclist_data=[]
        spec_body=req_body
        spec_list=[]
        for item in spec_body:           
            spec_details=item.get("name").split(" | ")
            spec_list.append(spec_details[0])
        spec_list=list(set(spec_list))
        product_spec=[data.replace(" ","\ ") for data in spec_list]
        spec_query=" || ".join(product_spec)
        params={"rows":2147483647,"fl":solr_product_column}
        query=f'TYPE:NAMPROD && SUBCT:REAL_SUB && TEXT2:({spec_query})'
        specdetails=list(solr_product.search(query,**params))
        for data in specdetails:
            spec_id=data.get("TEXT2","-")
            if (last_specid!=spec_id) and last_specid!='':
                namlist=list(set(namlist))
                synlist=list(set(synlist))
                namstr=",".join(namlist)
                synstr=", ".join(synlist)
                speclist_data.append([last_specid,namstr,synstr])  
                namlist=[]
                synlist=[]
                dat_nam=data.get("TEXT1","")
                dat_syn=data.get("TEXT3","")
                namlist.append(dat_nam)
                synlist.append(dat_syn)
            else:  
                dat_nam=data.get("TEXT1","")
                dat_syn=data.get("TEXT3","")
                namlist.append(dat_nam)
                synlist.append(dat_syn)      
            last_specid = spec_id  
        namlist=list(set(namlist))
        synlist=list(set(synlist))
        namstr=",".join(namlist)
        synstr=", ".join(synlist)
        speclist_data.append([last_specid,namstr,synstr])     
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
        desc_list=[]
        for i in list(matinfo):
            bdt=str(i.get("TEXT3")).strip()
            bdt_list.append(bdt)
            matnumber=str(i.get("TEXT1"))
            material_list.append(matnumber)
            desc=str(i.get("TEXT4"))
            desc_list.append(desc)
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
        return material_list,bdt_list,desc_list,matstr,material_details
    except Exception as e:
        return [],[],[],[],[]

def get_cas_details_on_selected_spec(product_rspec,params):
    try:
        cas_list=[]
        query=f'TYPE:SUBIDREL && TEXT2:{product_rspec}'
        temp_df=querying_solr_data(query,params) 
        column_value = list(temp_df["TEXT1"].unique())
        product_cas=[data.replace(" ","\ ") for data in column_value]
        cas_query=" || ".join(product_cas)
        params={"rows":2147483647,"fl":solr_product_column}
        query=f'TYPE:NUMCAS && SUBCT:(PURE_SUB || REAL_SUB) && TEXT2:({cas_query})'
        casdetails=list(solr_product.search(query,**params))
        return casdetails
    except Exception as e:
        return cas_list

def get_basic_properties_details(req_body):
    try:
        speclist_data = basic_spec_constructor(req_body)
        product_level_details=[]
        material_level_details=[]
        cas_level_details=[]
        product_level_dict={}
        material_level_dict={}
        cas_level_dict={}
        for specid,namprod,synonyms in speclist_data:
            product_level_dict["specId"]=specid
            product_level_dict["prodIdentifiers"]=namprod
            product_level_dict["synonyms"]=synonyms
            params={"rows":2147483647,"fl":"TEXT1,TEXT2,TEXT3,TEXT4"}
            material_list,bdt_list,desc_list,matstr,material_details=get_material_details_on_selected_spec(specid,params)
            len_active_mat=0
            for data in desc_list:
                if data[0] !='^':
                    len_active_mat+=1
            # year=[]
            # kg=0
            # check_material=list(set(material_list))
            # material_dump=[data.replace(" ","\ ") for data in check_material]
            # material_query=" || ".join(material_dump)
            # query=f'CATEGORY:SAP-BW && IS_RELEVANT:1 && PRODUCT:({material_query})'
            # params={"rows":2147483647,"fl":"DATA_EXTRACT,CATEGORY,PRODUCT"}
            # salesinfo=list(solr_unstructure_data.search(query,**params))
            # matlist_kg={}
            # matkg=0
            # last_mat=''
            # for data in salesinfo:
            #     material_number=data.get("PRODUCT","-")   
            #     datastr=json.loads(data.get("DATA_EXTRACT"))
            #     soldyear=str(datastr.get("Fiscal year/period","-"))
            #     kg=kg+int(datastr.get("SALES KG",0))
            #     if last_mat!=material_number and last_mat!='':
            #         matlist_kg[last_mat]=matkg
            #         matkg=0 
            #     else:
            #         matkg+=int(datastr.get("SALES KG",0))       
            #     soldyear=soldyear.split(".")
            #     if len(year)>1:
            #         year.append(int(soldyear[1]))
            #     last_mat=material_number
            # matlist_kg[last_mat]=matkg
            # year.sort()
            product_level_dict["no_Active_Materials"]=len_active_mat
            # if len(year)>1:
            #     product_level_dict["sales_Year"]=str(year[0])+" TO "+str(year[-1])
            # elif len(year)==1:
            #     product_level_dict["sales_Year"]=str(year[0])
            # else:
            #     product_level_dict["sales_Year"]='-'

            # params={"rows":2147483647}
            # query=f'SUBID:{specid}'
            # ghsdata=list(solr_ghs_labeling_list_data.search(query,**params))
            # SignalWord=str(ghsdata[0].get("SIGWD","-")).strip()
            # Pictogram=str(ghsdata[0].get("SYMBL","-")).strip()
            # HStatement=str(ghsdata[0].get("HAZST","-")).strip()
            # product_level_dict["GHS_Information"]=SignalWord+", "+Pictogram+", "+HStatement
            product_level_details.append(product_level_dict)
            product_level_dict={} 

            #materila level details 
            for matjson in material_details:
                material_level_dict["material_Number"]=matjson.get("material_number","-")
                material_level_dict["description"]=matjson.get("description","-")
                material_level_dict["spec_Id"]=str(specid)+" - "+str(namprod)
                material_level_dict["BDT"]=matjson.get("bdt","-")
                matnumber=matjson.get("material_number","-")
                # material_level_dict["sales_Volume"]=str(matlist_kg.get(matnumber,"0"))+" Kg"
                material_level_details.append(material_level_dict)
                material_level_dict={}
            
            #cas level details
            params={"rows":2147483647,"fl":"TEXT1,TEXT2,TEXT3,TEXT4"}
            catlist=get_cas_details_on_selected_spec(specid,params)
            for data in catlist:
                cas_level_dict["cas_Number"]=data.get("TEXT1","-")
                cas_level_dict["chemical_Name"]=data.get("TEXT3","-")
                cas_level_dict["spec_Id"]=str(specid)+" - "+str(namprod)
                cas_level_dict["pure_Spec_Id"]=data.get("TEXT2","-")
                cas_level_details.append(cas_level_dict)
                cas_level_dict={}
        result={}
        result["productLevel"]=product_level_details
        result["materialLevel"]=material_level_details
        result["CASLevel"]=cas_level_details

        return [result]
    except Exception as e:
        return [result]