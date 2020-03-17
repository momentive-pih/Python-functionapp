import logging
import json
import azure.functions as func
# from . import views
# from postAllProducts import views
import os 
import pysolr
solr_url_config="https://52.152.191.13:8983/solr"
#Solar url connection and access
solr_document_variant=pysolr.Solr(solr_url_config+'/sap_document_variant/', timeout=10,verify=False)
solr_product= pysolr.Solr(solr_url_config+"/product_information/", timeout=10,verify=False)

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postReportData function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_report_data_details(req_body[0])
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

def get_report_data_details(req_body):
    try:
        speclist_data=spec_constructor(req_body)
        report_list=[]
        material_list=[]
        params={"rows":2147483647,"fl":"TEXT1,TEXT2,TEXT3,TEXT4"}
        for spec,namprod in speclist_data:
            material_list,bdt_list,matstr,material_details=get_material_details_on_selected_spec(spec,params)
            material_list=list(set(material_list))
            material_col=", ".join(material_list)
            report_column_str="REPTY,RGVID,LANGU,VERSN,STATS,RELON"
            params={"rows":2147483647,"fl":report_column_str}
            query=f'SUBID:{spec}'
            result = list(solr_document_variant.search(query,**params))           
            for data in result:
                if data.get("LANGU").strip()=="E":
                    date_parse=data.get("RELON").strip()
                    if len(date_parse)==8:
                        date_format=date_parse[6:8]+"-"+date_parse[4:6]+"-"+date_parse[0:4]
                    else:
                        date_format=date_parse
                    report_json={
                        "category":data.get("REPTY").strip(),
                        "generation_Variant":data.get("RGVID").strip(),
                        "language":data.get("LANGU").strip(),
                        "version":data.get("VERSN").strip(),
                        "released_on":date_format,
                        "spec_id":str(spec+" - "+namprod),
                        "material_details":material_col,
                        "status":data.get("STATS").strip(),
                    }
                    report_list.append(report_json)
        return report_list
    except Exception as e:
        print(e)
        return []
