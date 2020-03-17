import logging
import json
import azure.functions as func
from . import get_spec_list
import pandas as pd
# from postAllProducts import views
import os 
product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","SUBCT"]
solr_product_column = ",".join(product_column)

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postSelectedSpecList function processing a request.')
        result=[]
        req_body = req.get_json()
        if type(req_body) is list:
            specid_list,namprod_list,specid_details = get_spec_list.find_specid(req_body)
            if len(specid_details)>0:
                home_details=home_page_details(specid_list[0])
        elif type(req_body) is dict:
            home_details=home_page_details(req_body)
        result = json.dumps(home_details)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def get_cas_details_on_selected_spec(product_rspec,params):
    try:
        cas_list=[]
        query=f'TYPE:SUBIDREL && TEXT2:{product_rspec}'
        temp_df=get_spec_list.querying_solr_data(query,params)                       
        column_value = list(temp_df["TEXT1"].unique())
        product_list=[data.replace(" ","\ ") for data in column_value]
        product_query=" || ".join(product_list)
        temp_df=pd.DataFrame()
        sub_category="PURE_SUB || REAL_SUB"
        query=f'TYPE:NUMCAS && SUBCT:({sub_category}) && TEXT2:({product_query})'
        temp_df=get_spec_list.querying_solr_data(query,params)
        cas_list = list(temp_df["TEXT1"].unique())
        return cas_list
    except Exception as e:
        return cas_list

def get_material_details_on_selected_spec(product_rspec,params):
    try:
        query=f'TYPE:MATNBR && TEXT2:{product_rspec}'
        matinfo=get_spec_list.solr_product.search(query,**params)
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

def home_page_details(spec_json):
    try:
        home_page_details={}
        material_list=[]
        cas_list=[]
        product_attributes=[]
        bdt_list=[]
        material_details=[]
        product_compliance=[]
        customer_comm=[]
        toxicology=[]
        restricted_sub=[]
        sales_information=[]
        report_data=[]
        home_spec_details=spec_json.get("name").split(" | ")
        home_spec=home_spec_details[0]
        home_namprod=home_spec_details[1]
        namprod_list=[home_namprod]
        params={"rows":2147483647,"fl":solr_product_column}
        #collecting CAS list
        cas_list=get_cas_details_on_selected_spec(home_spec,params)
        #material list      
        material_list,bdt_list,material_details,matstr=get_material_details_on_selected_spec(home_spec,params)
        #all category with value
        all_value_with_category={}
        if (home_spec):
            all_value_with_category["NAMPROD"]=[home_spec]
        if len(bdt_list)>0:
            all_value_with_category["BDT"]=bdt_list
        if len(material_list)>0:
            all_value_with_category["MATNBR"]=material_list
        if len(cas_list)>0:
            all_value_with_category["NUMCAS"]=cas_list            
        # product attributes
        mat_str=''
        if len(material_details)>3:
            mat_str=", ".join(material_details[0:2])  
        else:
            mat_str=", ".join(material_details)              
        product_attributes.append({"image":"https://5.imimg.com/data5/CS/BR/MY-3222221/pharmaceuticals-chemicals-500x500.jpg"})
        product_attributes.append({"Product Identification": str(home_spec)+"-"+str(home_namprod)})
        product_attributes.append({"Material Information":str(mat_str)})
        product_attributes.append({"tab_modal": "compositionModal"})
        home_page_details["Product Attributes"]=product_attributes

        #product compliance
        query=f'SUBID:{home_spec}'
        params={"rows":2147483647,"fl":"RLIST"}
        pcomp=list(get_spec_list.solr_notification_status.search(query,**params))
        country=[]
        for r in pcomp:
            place=r.get("RLIST")
            country.append(place)
        if len(country)>4:
            country=country[:4]
        rlist=", ".join(country)
        product_compliance.append({"image":"https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcS3WDKemmPJYhXsoGknA6nJwlRZTQzuYBY4xmpWAbInraPIJfAT"})
        product_compliance.append({"Negative Regulatory Notification Lists":str(rlist)}) 
        product_compliance.append({"tab_modal": "complianceModal"})          
        
        #ag registartion
        euflag=''
        usflag=''
        ltflag=''
        ag_country_active=[]
        ag_country_inactive=[]
        check_list=namprod_list+bdt_list
        check_list=list(set(check_list))
        product_list=[data.replace(" ","\ ") for data in check_list]
        product_query=" || ".join(product_list)
        category_list=["EU_REG_STATUS","US_REG_STATUS","LATAM_REG_STATUS"]
        region_list=["EU Region","US Canada","Latin America"]
        category_query=" || ".join(category_list)
        query=f'CATEGORY:({category_query}) && IS_RELEVANT:1 && PRODUCT:({product_query})'
        params={"rows":2147483647,"fl":"DATA_EXTRACT,PRODUCT,CATEGORY"}
        ag_result=list(get_spec_list.solr_unstructure_data.search(query,**params))
        for item in ag_result:
            region=item.get("CATEGORY","-")
            if region=="EU_REG_STATUS" and euflag=='':
                euflag='s'
                ag_country_active.append("EU Region")
            elif region=="US_REG_STATUS" and usflag=='':
                usflag='s'
                ag_country_active.append("US Canada")
            elif region=="LATAM_REG_STATUS" and ltflag=='':
                ltflag='s'
                ag_country_active.append("Latin America")
            if len(ag_country_active)>2:
                break
        if len(ag_country_active)!=3:
            for item in region_list:
                if item not in ag_country_active:
                    ag_country_inactive.append(item)

        product_compliance.append({"AG Registration Status - Acitve":", ".join(ag_country_active)})
        product_compliance.append({"AG Registration Status - Not Acitve":", ".join(ag_country_inactive)})
        home_page_details["Product compliance"]=product_compliance
        
        #customer communication
        usflag="No"
        euflag="No"
        category_list=["US-FDA","EU-FDA"]
        category_query=" || ".join(category_list)
        params={"rows":2147483647,"fl":"DATA_EXTRACT,PRODUCT,CATEGORY"}
        query=f'CATEGORY:({category_query}) && IS_RELEVANT:1 && PRODUCT:({product_query})'
        communication_result=list(get_spec_list.solr_unstructure_data.search(query,**params))
        for item in communication_result:
            com_category=item.get("CATEGORY","-")
            if com_category=="US-FDA":
                usflag="Yes"
            elif com_category=="EU-FDA":
                euflag="Yes"
            if usflag=="Yes" and euflag=="Yes":
                break
        customer_comm.append({"image": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQzuuf2CXVDH2fVLuKJRbIqd14LsQSAGaKb7_hgs9HAOtSsQsCL"})
        customer_comm.append({"US FDA Compliance" : usflag})
        customer_comm.append({"EU Food Contact " : euflag})
        customer_comm.append({"Top 3 Heavy Metal compositions":""})
        customer_comm.append({"tab_modal": "communicationModal"})
        home_page_details["Customer Communication"]=customer_comm


        #toxicology
        toxicology.append({ "image" : "https://flaptics.io/images/yu.png"})
        toxicology.append({"Study Titles" : ""})
        toxicology.append({"Toxicology Summary Report Available" : ""})
        toxicology.append({"Pending Monthly Tox Studies": ""})
        toxicology.append({ "tab_modal": "toxicologyModal"})
        home_page_details["Toxicology"]=toxicology

        #restricted_sub
        cas_product_list=[data.replace(" ","\ ") for data in cas_list]
        cas_product_query=" || ".join(cas_product_list)
        category_list=["GADSL","CAL-PROP"]
        category_query=" || ".join(category_list)
        params={"rows":2147483647,"fl":"DATA_EXTRACT,PRODUCT,CATEGORY"}
        query=f'CATEGORY:({category_query}) && IS_RELEVANT:1 && PRODUCT:({cas_product_query})'
        res_sub_result=list(get_spec_list.solr_unstructure_data.search(query,**params))
        gadsl_fg='No'
        cal_fg="No"
        for item in res_sub_result:
            re_category=item.get("CATEGORY","-")
            if re_category=="GADSL":
                gadsl_fg="Yes"
            elif re_category=="CAL-PROP":
                cal_fg="Yes"
            if gadsl_fg=="Yes" and cal_fg=="Yes":
                break
        restricted_sub.append({"image": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQnJXf4wky23vgRlLzdkExEFrkakubrov2OWcG9DTmDA1zA2-U-"})
        restricted_sub.append({"Components Present in GADSL": gadsl_fg})
        restricted_sub.append({"Components Present in Cal Prop 65":cal_fg})
        restricted_sub.append({"tab_modal": "restrictedSubstanceModal" })
        home_page_details["Restricted Substance"]=restricted_sub

        #sales_information
        kg=0
        sales_country=[]
        sales_information.append({"image":"https://medschool.duke.edu/sites/medschool.duke.edu/files/styles/interior_image/public/field/image/reports.jpg?itok=F7UK-zyt"})
        mat_product_list=[data.replace(" ","\ ") for data in material_list]
        mat_product_query=" || ".join(mat_product_list)
        params={"rows":2147483647,"fl":"DATA_EXTRACT,PRODUCT,CATEGORY"}
        query=f'CATEGORY:SAP-BW && IS_RELEVANT:1 && PRODUCT:({mat_product_query})'
        salesinfo=list(get_spec_list.solr_unstructure_data.search(query,**params))
        for data in salesinfo:
            datastr=json.loads(data.get("DATA_EXTRACT","-"))
            sales_country.append(datastr.get('Sold-to Customer Country',"-"))
            year_2019=str(datastr.get('Fiscal year/period',"-")).split(".")
            if len(year_2019)>0 and year_2019[1]=="2019":
                kg=kg+int(datastr.get("SALES KG"))
        sales_country=list(set(sales_country))
        if len(sales_country)<5:
            sold_country=", ".join(sales_country)
        else:
            sold_country=", ".join(sales_country[0:5])
            sold_country=sold_country+" and more.."
        sales_kg=str(kg)+" Kg"
        sales_information.append({"Total sales in 2019" :sales_kg})
        sales_information.append({"Regions where sold" :sold_country})
        sales_information.append({"tab_modal": "salesModal"})
        home_page_details["Sales Information"]=sales_information

        #report data
        report_data.append({ "image":"https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQSReXGibHOlD7Z5nNqD4d4V52CVMmi-fGUEKMH2HE7srV_SzNn_g"})
        report_data.append({"Report Status" :""})
        report_data.append({"tab_modal": "reportModal" })
        home_page_details["Report Data"]=report_data

    except Exception as e:
        logging.error(str(e))
    return home_page_details



    
