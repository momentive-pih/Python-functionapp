import pandas as pd
import json
import os
import pysolr

# solr_product= pysolr.Solr(os.environ["CONNECTIONSTRINGS:SOLRCONNECTIONSTRING"]+"/product_information/", timeout=10,verify=False)
solr_url_config="https://52.152.191.13:8983/solr"
solr_product= pysolr.Solr(solr_url_config+"/product_information/", timeout=10,verify=False)
solr_notification_status=pysolr.Solr(solr_url_config+'/sap_notification_status/', timeout=10,verify=False)
solr_unstructure_data=pysolr.Solr(solr_url_config+'/unstructure_processed_data/', timeout=10,verify=False)
solr_document_variant=pysolr.Solr(solr_url_config+'/sap_document_variant/', timeout=10,verify=False)
solr_ghs_labeling_list_data=pysolr.Solr(solr_url_config+'/sap_ghs_labeling_list_data/', timeout=10,verify=False)
product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","SUBCT"]
solr_product_column = ",".join(product_column)

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

def find_specid(data_json):
    try:
        count=0
        params={"rows":2147483647,"fl":solr_product_column}
        product_count=0
        material_count=0
        cas_count=0
        column_add=[]
        product_level_flag=''
        material_level_flag=''
        cas_level_flag=''
        add_df=pd.DataFrame()
        specid_details=[]     
        specid_list=[]
        namprod_list=[]
        spec_count=0
        
        def formatting(spec_count,specid_list,namprod_list,specid_details,product_query):
            try:
                query=f'TYPE:NAMPROD && SUBCT:REAL_SUB && TEXT2:({product_query})'
                add_df=querying_solr_data(query,params)
                val_df=add_df[["TEXT2","TEXT1"]]                        
                val_df.drop_duplicates(inplace=True)
                val_df=val_df.values.tolist()
                for product_rspec,product_name in val_df:
                    spec_count=spec_count+1
                    specid_list.append(product_rspec)
                    namprod_list.append(product_name)
                    val_json={"name":(str(product_rspec)+" | "+product_name),"id":spec_count}
                    specid_details.append(val_json)
                return specid_list,namprod_list,specid_details
            except Exception as e:
                print(e)
                return [],[],[]

        for item in data_json:
            search_value = item.get("name")
            search_value_split = search_value.split(" | ")
            search_column = item.get("type")
            search_column_split = search_column.split(" | ")
            search_group = item.get("group").split("(")
            search_group = search_group[0].strip()
            column_add.append(search_column)
            count+=1
            if search_group == "PRODUCT-LEVEL":
                product_level_flag = 's'
                product_count = count
                product_rspec = search_value_split[search_column_split.index("REAL-SPECID")]
                product_name = search_value_split[search_column_split.index("NAM PROD")]
            if search_group == "MATERIAL-LEVEL":
                material_level_flag = 's'
                material_count = count
                material_number = search_value_split[search_column_split.index("MATERIAL NUMBER")]
            if search_group == "CAS-LEVEL":
                cas_level_flag = 's'
                cas_count = count
                cas_pspec = search_value_split[search_column_split.index("PURE-SPECID")]                        
        if product_level_flag=='s' and product_count==1:
            spec_count=spec_count+1
            product_query=product_rspec
            specid_list,namprod_list,specid_details=formatting(spec_count,specid_list,namprod_list,specid_details,product_query)
        elif material_level_flag =='s' and material_count==1:
            if product_level_flag =='':
                query=f'TYPE:MATNBR && TEXT1:{material_number}'
                temp_df=querying_solr_data(query,params)
                column_value = temp_df["TEXT2"].unique()
                product_list=[data.replace(" ","\ ") for data in list(column_value)]
                product_query=" || ".join(product_list)      
                specid_list,namprod_list,specid_details=formatting(spec_count,specid_list,namprod_list,specid_details,product_query)
            else:
                spec_count=spec_count+1
                product_query=product_rspec
                specid_list,namprod_list,specid_details=formatting(spec_count,specid_list,namprod_list,specid_details,product_query)
        elif cas_level_flag=='s' and cas_count==1:
            if product_level_flag =='' and material_level_flag=='':
                query=f'TYPE:SUBIDREL && TEXT1:{cas_pspec}'
                temp_df=querying_solr_data(query,params)
                column_value = temp_df["TEXT2"].unique()
                product_list=[data.replace(" ","\ ") for data in list(column_value)]
                product_query=" || ".join(product_list)
                specid_list,namprod_list,specid_details=formatting(spec_count,specid_list,namprod_list,specid_details,product_query)
            elif material_level_flag=='s' and product_level_flag=='':
                query=f'TYPE:MATNBR && TEXT1:{material_number}'
                temp_df=querying_solr_data(query,params)
                column_value = temp_df["TEXT2"].unique()
                product_list=[data.replace(" ","\ ") for data in list(column_value)]
                product_query=" || ".join(product_list)      
                specid_list,namprod_list,specid_details=formatting(spec_count,specid_list,namprod_list,specid_details,product_query) 
            elif product_level_flag =='s':
                spec_count=spec_count+1
                product_query=product_rspec
                specid_list,namprod_list,specid_details=formatting(spec_count,specid_list,namprod_list,specid_details,product_query)                                
        return specid_details,specid_list,namprod_list   
    except Exception as e:
        print(e)
        return specid_details,specid_list,namprod_list
