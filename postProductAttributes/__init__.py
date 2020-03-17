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
solr_substance_identifier=pysolr.Solr(solr_url_config+'/sap_substance_identifier/',timeout=10,verify=False)
solr_phrase_translation=pysolr.Solr(solr_url_config+'/sap_phrase_translation/',timeout=10,verify=False)
solr_inci_name=pysolr.Solr(solr_url_config+'/inci_name_prod/',timeout=10,verify=False)
solr_std_composition=pysolr.Solr(solr_url_config+'/sap_standard_composition/',timeout=10,verify=False)
solr_hundrd_composition=pysolr.Solr(solr_url_config+'/sap_hundrd_percent_composition/',timeout=10,verify=False)
solr_legal_composition=pysolr.Solr(solr_url_config+'/sap_legal_composition/',timeout=10,verify=False)
solr_substance_volume_tracking=pysolr.Solr(solr_url_config+'/sap_substance_volume_tracking/',timeout=10,verify=False)
product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","SUBCT"]
solr_product_column = ",".join(product_column)
file_access_path="https://clditdevstorpih.blob.core.windows.net/"
ghs_image_path="https://clditdevstorpih.blob.core.windows.net/momentive-sources-pih/ghs-images-pih/"

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postToxicology function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_product_attributes(req_body[0])
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

def get_cas_details_on_selected_spec(product_rspec,params):
    try:
        cas_list=[]
        chemical_list=[]
        pspec_list=[]
        query=f'TYPE:SUBIDREL && TEXT2:({product_rspec})'
        temp_df=querying_solr_data(query,params)
        column_value=[]
        if "TEXT1" in  list(temp_df.columns):                 
            column_value = list(temp_df["TEXT1"].unique())
        product_list=[data.replace(" ","\ ") for data in column_value]
        product_query=" || ".join(product_list)
        sub_category="PURE_SUB || REAL_SUB"
        if len(product_list)>0:
            query=f'TYPE:NUMCAS && SUBCT:({sub_category}) && TEXT2:({product_query})'
            temp_result=list(solr_product.search(query,**params))
            cas_json_list=[]
            cjson={}
            for row in temp_result:           
                pure_spec=row.get("TEXT2","")
                cjson[pure_spec]={}
                cjson[pure_spec]["cas_number"]=row.get("TEXT1","")
                cjson[pure_spec]["chemical_name"]=row.get("TEXT3","")
                cas_list.append(row.get("TEXT1",""))
                chemical_list.append(row.get("TEXT3",""))
        # cas_list = list(temp_df["TEXT1"].unique())
        # chemical_list= list(temp_df["TEXT3"].unique())
        return cas_list,chemical_list,column_value,cjson
    except Exception as e:
        return [],[],[],[]

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

def get_product_attributes(req_body):
    try:
        speclist_data,speclist_json,total_spec,total_namprod=spec_constructor(req_body)
        json_make={}
        product_attributes_result=[]
        json_list=[]
        params={"rows":2147483647}
        sub_category=req_body.get("Category_details").get("Subcategory")    
        validity=req_body.get("Category_details").get("validity")
        if sub_category=="Basic Information":
            basic_details=[]
            prod_add=[]
            prod_json={}
            for specid in speclist_json:
                material_list,bdt_list,matstr,material_details=get_material_details_on_selected_spec(specid,params)
                total_namprod=(speclist_json[specid]).split(", ")
                json_make["spec_Id"]=specid
                #product level 
                query=f'TYPE:NAMPROD && SUBCT:REAL_SUB && TEXT2:{specid}'
                df_namprod = querying_solr_data(query,params)
                if "TEXT1" in list(df_namprod.columns):
                    namprod=", ".join(list(df_namprod["TEXT1"].unique()))           
                json_make["product_Identification"]=namprod

                query=f'SUBID:{specid}'
                result=list(solr_substance_identifier.search(query,**params))
                idtxt=[]
                for item in result:
                    if item.get("IDCAT").strip()=="NAM" and item.get("IDTYP").strip()=="PROD_RLBL":
                        language=item.get("LANGU","").strip()
                        if language=="E" or language=='':
                            idtxt.append(item.get("IDTXT","-"))
                json_make["relabels"]=", ".join(idtxt)
                basic_details.append(json_make)
                json_make={}
                result=[]
                check_product=bdt_list+total_namprod
                product_list=[data.replace(" ","\ ") for data in check_product if data!="None"]
                product_query=" || ".join(product_list)
                query=f'IS_RELEVANT:1 && PRODUCT:({product_query}) && CATEGORY:Prod-App'
                params={"rows":2147483647}
                result=list(solr_unstructure_data.search(query,**params))
                for data in result:
                    product=data.get("PRODUCT")
                    datastr=json.loads(data.get("DATA_EXTRACT"))
                    path=datastr.get("image_path","-")
                    json_make["product"]=product
                    json_make["prod_App"]=file_access_path+path.replace("/dbfs/mnt/","")
                    json_make["spec_Id"]=specid
                    json_list.append(json_make)
                    json_make={}
                # prod_json[specid]=json_list
                # prod_json["spec_Id"]=specid
                # prod_add.append(prod_json)
            product_attributes_result.append({"basic_details":basic_details})
            product_attributes_result.append({"product_Application":json_list})
        elif sub_category=="GHS Labeling":
            response = solr_phrase_translation.search("*:*",**params)
            result = json.dumps(list(response))
            df_phrase_trans=pd.read_json(result,dtype=str) 
            df_phrase_trans["PHRKY"]=df_phrase_trans["PHRKY"].str.strip()
            df_phrase_trans["PTEXT"]=df_phrase_trans["PTEXT"].str.strip()
            product_list=[data.replace(" ","\ ") for data in total_spec if data!="None"]
            spec_query=" || ".join(product_list)
            query=f'SUBID:({spec_query})'
            params={"rows":2147483647}
            result=list(solr_ghs_labeling_list_data.search(query,**params))
            for data in result:
                specid=data.get("SUBID","").strip()
                ghs_usage=data.get("ZUSAGE","").strip()
                ghs_rg=data.get("REBAS","").strip()
                ghs_rg=ghs_rg.split(";")
                ghs_symbols=data.get("SYMBL","").strip()
                ghs_symbols=ghs_symbols.split(";")
                ghs_sword=data.get("SIGWD","").strip()
                ghs_sword=ghs_sword.split(";")
                ghs_hstatement=data.get("HAZST","").strip()
                ghs_hstatement=ghs_hstatement.split(";")
                ghs_precst=data.get("PRSTG","").strip()
                ghs_precst=ghs_precst.split(";")
                ghs_adinfo=data.get("ADDIN","").strip()
                ghs_adinfo=ghs_adinfo.split(";")
                ghs_remarks=data.get("REMAR","").strip()
                ghs_remarks=ghs_remarks.split(";")
                # temp_df=df_phrase_trans[df_phrase_trans["PHRKY"].isin(ghs_usage)]
                # usage_value=list(temp_df["PTEXT"])
                json_make["usage"]=ghs_usage
                temp_df=df_phrase_trans[df_phrase_trans["PHRKY"].isin(ghs_rg)]
                reg_value=list(temp_df["PTEXT"])
                json_make["regulatory_Basis"]=", ".join(reg_value)
                temp_df=df_phrase_trans[df_phrase_trans["PHRKY"].isin(ghs_symbols)]
                sym_value=list(temp_df["GRAPH"])
                if len(sym_value)>0:
                    json_make["symbols"]=ghs_image_path+sym_value[0]
                else:
                    json_make["symbols"]= None
                temp_df=df_phrase_trans[df_phrase_trans["PHRKY"].isin(ghs_sword)]
                sword_value=list(temp_df["PTEXT"])
                json_make["signal_Word"]=", ".join(sword_value)
                temp_df=df_phrase_trans[df_phrase_trans["PHRKY"].isin(ghs_hstatement)]
                hstat_value=list(temp_df["PTEXT"])
                json_make["hazard_Statements"]=", ".join(hstat_value)
                temp_df=df_phrase_trans[df_phrase_trans["PHRKY"].isin(ghs_precst)]
                prec_value=list(temp_df["PTEXT"])
                json_make["prec_Statements"]=", ".join(prec_value)
                temp_df=df_phrase_trans[df_phrase_trans["PHRKY"].isin(ghs_adinfo)]
                addinfo_value=list(temp_df["PTEXT"])
                temp_df=df_phrase_trans[df_phrase_trans["PHRKY"].isin(ghs_remarks)]
                remark_value=list(temp_df["PTEXT"])
                json_make["additional_Information_remarks"]=(", ".join(addinfo_value))+" "+(", ".join(remark_value))
                json_make["spec_Id"]=specid+" - "+speclist_json[specid]
                json_list.append(json_make)
                json_make={}
            product_attributes_result.append({"ghs_Labeling":json_list})  
        elif sub_category in ["Structures and Formulas","Flow Diagrams"]:
            chem_structure=[]
            molecular_formula=[]
            molecular_weight=[]
            man_flow_dg=[]
            synthesis_dg=[]
            for specid in speclist_json:
                material_list,bdt_list,matstr,material_details=get_material_details_on_selected_spec(specid,params)
                cas_list,chemical_list,pspec_list=get_cas_details_on_selected_spec(specid,params)
                total_namprod=(speclist_json[specid]).split(", ")
                sub_category=req_body.get("Category_details").get("Subcategory")
                check_product=bdt_list+total_namprod+cas_list+chemical_list
                check_product=[data.replace("/","\/") for data in check_product if (data!="None" and data!='-')]
                product_list=[data.replace(" ","\ ") for data in check_product]
                product_query=" || ".join(product_list)  
                query=f'ONTOLOGY_KEY:({product_query})'
                result=list(solr_ontology.search(query,**params))
                result_dumps = json.dumps(result)
                df_ela=pd.read_json(result_dumps,dtype=str)
                ela_key_list=[]
                if "ONTOLOGY_VALUE" in list(df_ela.columns):
                    ela_key_list=list(df_ela["ONTOLOGY_VALUE"].unique())
                check_product=bdt_list+total_namprod+cas_list+ela_key_list+chemical_list
                check_product=[data.replace("/","\/") for data in check_product if (data!="None" and data!='-')]
                product_list=[data.replace(" ","\ ") for data in check_product]
                product_query=" || ".join(product_list)  
                struct_form_query='Chemical\ Structure || molecular\ formula || Molecular-Weight'
                flow_diagram_query='man_flow_diagram || syn_flow_diagram'
                if sub_category=="Structures and Formulas":
                    query=f'IS_RELEVANT:1 && PRODUCT:({product_query}) && CATEGORY:({struct_form_query})'
                else:
                    query=f'IS_RELEVANT:1 && PRODUCT:({product_query}) && CATEGORY:({flow_diagram_query})'
                params={"rows":2147483647}
                result=list(solr_unstructure_data.search(query,**params))             
                for data in result:
                    category=data.get("CATEGORY","")
                    product=data.get("PRODUCT","-")
                    product_type=data.get("PRODUCT_TYPE","-")
                    json_make["product"]=product
                    json_make["product_Type"]=product_type
                    json_make["spec_Id"]=specid+" - "+speclist_json[specid]
                    datastr=json.loads(data.get("DATA_EXTRACT"))
                    if category=="Chemical Structure":
                        path=datastr.get("file_path","-")
                        json_make["file_Path"]=file_access_path+path.replace("/dbfs/mnt/","")   
                        chem_structure.append(json_make)
                        json_make={}
                    elif category=="molecular formula":
                        path=datastr.get("image_path","-")
                        json_make["file_Path"]=file_access_path+path.replace("/dbfs/mnt/","")   
                        molecular_formula.append(json_make)
                        json_make={}
                    elif category=="Molecular-Weight":
                        weight=datastr.get("Molecular Weight","-")
                        # weight=weight.replace("Molecular Weight:","").strip()
                        json_make["moelcular_Weight"]=weight
                        molecular_weight.append(json_make)
                        json_make={}
                    elif category=="man_flow_diagram":
                        path=datastr.get("file_path","-")
                        # weight=weight.replace("Molecular Weight:","").strip()
                        json_make["file_Path"]=file_access_path+path.replace("/dbfs/mnt/","")
                        man_flow_dg.append(json_make)
                        json_make={}
                    elif category=="syn_flow_diagram":
                        path=datastr.get("file_path","-")
                        # weight=weight.replace("Molecular Weight:","").strip()
                        json_make["file_Path"]=file_access_path+path.replace("/dbfs/mnt/","")
                        synthesis_dg.append(json_make)
                        json_make={}
            if sub_category=="Structures and Formulas":        
                product_attributes_result.append({"chemical_Structure":chem_structure})
                product_attributes_result.append({"molecular_Formula":molecular_formula})
                product_attributes_result.append({"molecular_Weight":molecular_weight})
            else:
                product_attributes_result.append({"manufacture_Flow":man_flow_dg})
                product_attributes_result.append({"synthesis_Diagram":synthesis_dg})
        elif sub_category=="Composition":
            if len(total_spec)>0:
                # ###############################
                # sub_category="Standard, 100 % & INCI Composition"
                # usage_flag='s'
                # zusage_value='PUBLIC: REG_WORLD'
                # ##############################    
                spec_id=total_spec[0]
                if len(total_namprod)>0:
                    namprod=speclist_json[spec_id]
                material_list,bdt_list,matstr,material_details=get_material_details_on_selected_spec(spec_id,params)
                cas_list,chemical_list,pspec_list,cas_json=get_cas_details_on_selected_spec(spec_id,params)  
                product_level_json={}
                #product level json
                product_level_json["real_Sub_Number"]=spec_id
                query=f'TYPE:NAMPROD && SUBCT:REAL_SUB && TEXT2:{spec_id}'
                df_namprod = querying_solr_data(query,params)
                if "TEXT1" in list(df_namprod.columns):
                    namprod=", ".join(list(df_namprod["TEXT1"].unique()))
                product_level_json["product_Name"]=namprod
                # query=f'IDCAT:NAM && IDTYP:PROD_RLBL && LANGU:E && SUBID:{spec_id}'
                query=f'SUBID:{spec_id}'
                result=list(solr_substance_identifier.search(query,**params))
                idtxt=[]
                for item in result:
                    if item.get("IDCAT").strip()=="NAM" and item.get("IDTYP").strip()=="PROD_RLBL":
                        language=item.get("LANGU","").strip()
                        if language=="E" or language=='':
                            idtxt.append(item.get("IDTXT","-"))
                product_level_json["relabels"]=", ".join(idtxt)
                result=[]
                display_inci_name=[]
                query=f'SUBID:({spec_id}) && IDTYP:INCI'
                result=list(solr_substance_identifier.search(query,**params))
                result_dumps = json.dumps(result)
                df_inci=pd.read_json(result_dumps,dtype=str)
                df_inci.drop_duplicates(inplace=True)
                if "IDTXT" in list(df_inci.columns):
                    inci_name=list(df_inci["IDTXT"].unique())
                    for iname in inci_name:
                        query=f'INCINAME:"{iname}" && SUBID:{spec_id}'
                        result=list(solr_inci_name.search(query,**params))
                        result_dumps = json.dumps(result)
                        df_bdtxt=pd.read_json(result_dumps,dtype=str)
                        df_bdtxt.drop_duplicates(inplace=True)
                        if "INCINAME" in list(df_bdtxt.columns) and "BDTXT" in list(df_bdtxt.columns):
                            bdtxt_list=df_bdtxt[["BDTXT","INCINAME"]]
                            bdtxt_list=bdtxt_list.drop_duplicates()
                            bdtxt_list=bdtxt_list.values.tolist()
                            for bdtxt,inci in bdtxt_list:
                                temp=bdtxt+" | "+inci
                                display_inci_name.append(temp)                 
                product_level_json["INCI_name"]=", ".join(display_inci_name)
                #material level
                active_material=[]
                all_material=[]
                for item in material_details:
                    material_number=item.get("material_number","-")
                    description=item.get("description","-").strip()
                    bdt=item.get("bdt","-")
                    if description[0] != "^":
                        json_make["material_Number"]=material_number
                        json_make["description"]=description
                        json_make["bdt"]=bdt
                        active_material.append(json_make)
                        json_make={}
                    json_make["material_Number"]=material_number
                    json_make["description"]=description
                    json_make["bdt"]=bdt
                    all_material.append(json_make)
                    json_make={} 
                product_result=[]
                json_make["product_Level"]=product_level_json
                json_make["active_material"]=active_material
                json_make["all_material"]=all_material
                product_result.append(json_make)
                return product_result
        elif sub_category in ["Standard, 100 % & INCI Composition","Legal Composition"]:
                if len(total_spec)>0:  
                    spec_id=total_spec[0]
                    if len(total_namprod)>0:
                        namprod=total_namprod[0]
                    material_list,bdt_list,matstr,material_details=get_material_details_on_selected_spec(spec_id,params)
                    cas_list,chemical_list,pspec_list,cas_json=get_cas_details_on_selected_spec(spec_id,params)  
                    print(validity)
                    if validity is None:
                        std_usage=[]
                        hundrd_usage=[]
                        product_list=[data.replace(" ","\ ") for data in pspec_list if (data!="None" and data!="-")]
                        pspec_query=" || ".join(product_list)
                        query=f'CSUBI:({pspec_query}) && SUBID:({spec_id})'
                        if sub_category=="Standard, 100 % & INCI Composition":
                            result=list(solr_std_composition.search(query,**params))
                            result_dumps = json.dumps(result)
                            df_std=pd.read_json(result_dumps,dtype=str)
                            if "ZUSAGE" in list(df_std.columns):
                                std_usage=list(df_std["ZUSAGE"].unique())
                            result=[]
                            result=list(solr_hundrd_composition.search(query,**params))
                            result_dumps = json.dumps(result)
                            df_hundrd=pd.read_json(result_dumps,dtype=str)
                            if "ZUSAGE" in list(df_hundrd.columns):
                                hundrd_usage=list(df_hundrd["ZUSAGE"].unique())           
                            usage_catgory=std_usage+hundrd_usage
                            json_list=[]
                            for i in list(set(usage_catgory)):
                                json_make["name"]=i
                                json_list.append(json_make)
                                json_make={}
                            return json_list
                        else:
                            result=[]
                            result=list(solr_legal_composition.search(query,**params))
                            result_dumps = json.dumps(result)
                            df_legal=pd.read_json(result_dumps,dtype=str)
                            if "ZUSAGE" in list(df_legal.columns):
                                legal_usage=list(df_legal["ZUSAGE"].unique())
                            json_list=[]
                            for i in list(set(legal_usage)):
                                json_make["name"]=i
                                json_list.append(json_make)
                                json_make={}
                            return json_list              
                    if validity is not None:
                        product_list=[data.replace(" ","\ ") for data in pspec_list if (data!="None" and data!="-")]
                        pspec_query=" || ".join(product_list)
                        zusage_value=validity.replace(":","\:")
                        query=f'CSUBI:({pspec_query}) && ZUSAGE:({zusage_value}) && SUBID:({spec_id})'                       
                        if sub_category=="Standard, 100 % & INCI Composition":
                            std_result=[]
                            hundrd_result=[]
                            inci_result=[]
                            # if sub_category=="Standard, 100 % & INCI Composition":
                            #std composition data
                            std_result=list(solr_std_composition.search(query,**params))
                            hundrd_result=list(solr_hundrd_composition.search(query,**params))
                            total_list=bdt_list+[namprod]
                            product_list=[data.replace(" ","\ ") for data in total_list if (data!="None" and data!="-")]
                            total_query=" || ".join(product_list)
                            params={"rows":2147483647}
                            query=f'CATEGORY:CIDP && IS_RELEVANT:1 && PRODUCT:({total_query}) && -PRODUCT_TYPE:null'
                            inci_result=list(solr_unstructure_data.search(query,**params))  
                            json_make={} 
                            for item in cas_json:
                                #std table
                                std_flag=''
                                hundrd_flag=''
                                inci_flag='' 
                                for std in std_result:
                                    if std.get("CSUBI")==item:
                                        std_flag='s'
                                        json_make["std_Componant_Type"]=std.get("COMPT","-")
                                        json_make["std_value"]=std.get("CVALU","-")
                                        json_make["std_unit"]=std.get("CUNIT","-")
                                for hundrd in hundrd_result:
                                    if hundrd.get("CSUBI")==item:
                                        hundrd_flag='s'
                                        json_make["hundrd_Componant_Type"]=hundrd.get("COMPT","-")
                                        json_make["hundrd_value"]=hundrd.get("CVALU","-")
                                        json_make["hundrd_unit"]=hundrd.get("CUNIT","-")
                                for inci in inci_result:
                                    data=json.loads(inci.get("DATA_EXTRACT"))
                                    inci_cas_number=data.get("CAS Number ")
                                    if inci_cas_number==cas_json.get(item).get("cas_number"):
                                        inci_flag='s'
                                        json_make["inci_Componant_Type"]="Active"
                                        json_make["inci_value_unit"]=data.get("Target Composition","-")
                                if std_flag =='':
                                    json_make["std_Componant_Type"]='-'
                                    json_make["std_value"]='-'
                                    json_make["std_unit"]="-"
                                if hundrd_flag=='':
                                    json_make["hundrd_Componant_Type"]="-"
                                    json_make["hundrd_value"]="-"
                                    json_make["hundrd_unit"]="-"
                                if inci_flag=='':
                                    json_make["inci_Componant_Type"]="-"
                                    json_make["inci_value_unit"]="-"
                                if std_flag=='s' or hundrd_flag=='s' or inci_flag=='s':
                                    json_make["pure_spec_Id"]=item
                                    json_make["cas_Number"]=cas_json.get(item).get("cas_number")
                                    json_make["ingredient_Name"]=cas_json.get(item).get("chemical_name")
                                    json_list.append(json_make)
                                json_make={}
                            return json_list
                        elif sub_category=="Legal Composition":
                            json_list=[]
                            # query=f'CSUBI:({pspec_query}) && ZUSAGE:({zusage_value}) && SUBID:({spec_id})'
                            legal_result=list(solr_legal_composition.search(query,**params))
                            legal_svt_spec=[]
                            legal_comp={}
                            for item in cas_json:           
                                for data in legal_result:
                                    if data.get("CSUBI")==item:
                                        json_make["pure_spec_Id"]=item
                                        json_make["cas_Number"]=cas_json.get(item).get("cas_number")
                                        json_make["ingredient_Name"]=cas_json.get(item).get("chemical_name")
                                        legal_svt_spec.append(item)
                                        json_make["legal_Componant_Type"]=data.get("COMPT","-")
                                        json_make["legal_value"]=data.get("CVALU","-")
                                        json_make["legal_unit"]=data.get("CUNIT","-")
                                        json_list.append(json_make)
                                        json_make={}
                            legal_comp["legal_composition"]=json_list
                            if validity=='REACH: REG_REACH':
                                json_list=[]
                                json_make={}
                                svt_result=[]
                                subid=[data.replace(" ","\ ") for data in legal_svt_spec if (data!="None" and data!="-")]
                                subid_list=" || ".join(subid)
                                if len(legal_svt_spec)>0:
                                    query=f'SUBID:({subid_list})'
                                    svt_result=list(solr_substance_volume_tracking.search(query,**params))
                                presence_id=[]
                                for data in svt_result:
                                    presence_id.append(data.get("SUBID","-"))
                                for sub in list(set(presence_id)):
                                    json_make["pure_spec_Id"]=sub
                                    svt_total_2018=0
                                    svt_total_2019=0
                                    svt_total_2020=0
                                    for data in svt_result:
                                        if sub==data.get("SUBID","-"):
                                            reg_value=data.get("REGLT","-")
                                            reg_year=data.get("QYEAR","-").strip()
                                            if reg_value=="SVT_TE":
                                                if reg_year=="2018":
                                                    json_make["SVT_TE_eight"]=data.get("CUMQT","-")
                                                if reg_year=="2019":
                                                    json_make["SVT_TE_nine"]=data.get("CUMQT","-")
                                                if  reg_year=="2020":
                                                    json_make["SVT_TE_twenty"]=data.get("CUMQT","-")
                                                json_make["amount_limit_SVT_TE"]=data.get("AMTLT","0")
                                            if reg_value=="SVT_AN":
                                                if reg_year=="2018":
                                                    json_make["SVT_AN_eight"]=data.get("CUMQT","-")
                                                if reg_year=="2019":
                                                    json_make["SVT_AN_nine"]=data.get("CUMQT","-")
                                                if  reg_year=="2020":
                                                    json_make["SVT_AN_twenty"]=data.get("CUMQT","-")
                                                print(type(data.get("AMTLT","0")))
                                                json_make["amount_limit_SVT_AN"]=data.get("AMTLT","0")
                                            if reg_value=="SVT_LV":
                                                if reg_year=="2018":
                                                    svt_total_2018+=float(data.get("CUMQT","-"))
                                                    json_make["SVT_LV_eight"]=svt_total_2018
                                                if reg_year=="2019":
                                                    svt_total_2019+=float(data.get("CUMQT","-"))
                                                    json_make["SVT_LV_nine"]=svt_total_2019
                                                if  reg_year=="2020":
                                                    svt_total_2020+=float(data.get("CUMQT","-"))
                                                    json_make["SVT_LV_twenty"]=svt_total_2020
                                                json_make["amount_limit_SVT_LV"]=data.get("AMTLT","0")                   
                                    json_list.append(json_make)
                                    json_make={}
                                total_svt_te_amt=0
                                total_svt_an_amt=0
                                total_svt_lv_amt=0
                                for item in json_list:
                                    total_svt_te_amt=total_svt_te_amt+float(item.get("amount_limit_SVT_TE",0))
                                    total_svt_an_amt=total_svt_an_amt+float(item.get("amount_limit_SVT_AN",0))
                                    total_svt_lv_amt=total_svt_lv_amt+float(item.get("amount_limit_SVT_LV",0))
                                json_make["pure_spec_Id"]="Total"
                                json_make["SVT_TE_eight"]=""
                                json_make["SVT_TE_nine"]=""
                                json_make["SVT_TE_twenty"]=""
                                json_make["amount_limit_SVT_TE"]=str(total_svt_te_amt)
                                json_make["SVT_AN_eight"]=""
                                json_make["SVT_AN_nine"]=""
                                json_make["SVT_AN_twenty"]=""
                                json_make["amount_limit_SVT_AN"]=str(total_svt_an_amt)
                                json_make["SVT_LV_eight"]=""
                                json_make["SVT_LV_nine"]=""
                                json_make["SVT_LV_twenty"]=""
                                json_make["amount_limit_SVT_LV"]=str(total_svt_lv_amt)
                                json_list.append(json_make)
                                json_make={}
                                for item in range(len(json_list)):
                                    if json_list[item].get("SVT_TE_eight") is None:
                                        json_list[item]["SVT_TE_eight"]="-"
                                    if json_list[item].get("SVT_TE_nine") is None:
                                        json_list[item]["SVT_TE_nine"]="-"
                                    if json_list[item].get("SVT_TE_twenty") is None:
                                        json_list[item]["SVT_TE_twenty"]="-"
                                    if json_list[item].get("amount_limit_SVT_TE") is None:
                                        json_list[item]["amount_limit_SVT_TE"]="-"
                                    if json_list[item].get("SVT_AN_eight") is None:
                                        json_list[item]["SVT_AN_eight"]="-"
                                    if json_list[item].get("SVT_AN_nine") is None:
                                        json_list[item]["SVT_AN_nine"]="-"
                                    if json_list[item].get("SVT_AN_twenty") is None:
                                        json_list[item]["SVT_AN_twenty"]="-"
                                    if json_list[item].get("amount_limit_SVT_AN") is None:
                                        json_list[item]["amount_limit_SVT_AN"]="-"
                                    if json_list[item].get("SVT_LV_eight") is None:
                                        json_list[item]["SVT_LV_eight"]="-"
                                    if json_list[item].get("SVT_LV_nine") is None:
                                        json_list[item]["SVT_LV_nine"]="-"
                                    if json_list[item].get("SVT_LV_twenty") is None:
                                        json_list[item]["SVT_LV_twenty"]="-"
                                    if json_list[item].get("amount_limit_SVT_LV") is None:
                                        json_list[item]["amount_limit_SVT_LV"]="-"
                                legal_comp["svt"]=json_list
                                
                            return legal_comp                   
        return product_attributes_result
    except Exception as e:
        return product_attributes_result
