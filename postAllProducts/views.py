import json
import re
import pandas as pd
import pysolr
from  . import settings as config
solr_product=config.solr_product
solr_notification_status=config.solr_notification_status
solr_unstructure_data=config.solr_unstructure_data
solr_document_variant=config.solr_document_variant
junk_column=config.junk_column
product_column=config.product_column
product_nam_category=config.product_nam_category
product_rspec_category = config.product_rspec_category
product_namsyn_category = config.product_namsyn_category
material_number_category = config.material_number_category
material_bdt_category = config.material_bdt_category
cas_number_category = config.cas_number_category
cas_pspec_category = config.cas_pspec_category
cas_chemical_category = config.cas_chemical_category
category_with_key=config.category_with_key
category_type = config.category_type
search_category = config.search_category
selected_categories=config.selected_categories
selected_data_json = {}
# db_url=db_config.get("URL")
# product_tb=db_config.get("product_core")
spec_count=[]
real_spec_list=[]
spec_id_list=[]
cas_list=[]
material_list=[]
selected_spec_id=[]
home_default_spec_id=[]
selected_material_details=[]

def product_level_creation(product_df,product_category_map,type,subct,key,level_name,filter_flag="no"):
    try:
        json_list=[]
        if filter_flag=="no":
            if type !='' and subct !='':
                temp_df=product_df[(product_df["TYPE"]==type) & (product_df["SUBCT"]==subct)]
            else:
                temp_df=product_df[(product_df["TYPE"]==type)]
        else:
            temp_df=product_df
        
        temp_df.drop_duplicates(inplace=True)
        temp_df=temp_df.fillna("-")
        total_count=0
        display_category=''
        json_category=''
        extract_column=[]
        for column,category in product_category_map:
            
            extract_column.append(column) 
            try:
                col_count=list(temp_df[column].unique())
            except KeyError:
                temp_df[column]="-"
                col_count=list(temp_df[column].unique())

            if '-' in col_count:
                col_count = list(filter(('-').__ne__, col_count))
            category_count = len(col_count)
            total_count+=category_count
            display_category+=category+" - "+str(category_count)+" | "
            json_category+= category+" | " 

            # #saving cas number globally
            # if column=="TEXT1" and category=="CAS NUMBER":
            #     cas_list=cas_list+list(temp_df[column].unique())
                    
        display_category=display_category[:-3] 
        json_category=json_category[:-3]       
        temp_df=temp_df[extract_column].values.tolist()
        for value1,value2,value3 in temp_df:
            if ("obsolete" not in value1.lower() and "obsolete" not in value2.lower() and "obsolete" not in value3.lower()):
                value = str(value1).strip() + " | "+str(value2).strip()+" | "+str(value3).strip()
                out_dict={"name":value,"type":json_category,"key":key,"group":level_name+" ("+display_category+")"+" - "+str(total_count) }
                json_list.append(out_dict)
        # print(json_list)
        return json_list
    except Exception as e:
        return json_list

def querying_solr_data(query,params):
    try:
        df_product_combine=pd.DataFrame()
        response = solr_product.search(query,**params)
        result = json.dumps(list(response))
        df_product_combine=pd.read_json(result,dtype=str)
        df_product_combine=df_product_combine.fillna("-")
        return df_product_combine
    except Exception as e:
        return df_product_combine


def all_products(data):
    try:
        all_product_list=[]
        search=''
        search_split=''
        search_key=''
        search_value=''
        key_flag=''
        search=data
        params={"rows":2147483647}
        if "*" in search:
            key_flag='s'
            search_split=search.split('*')
            search_key=search_split[0]+"*"
            search_value = search_split[1].strip()
        else:
            search_value = search                            
        search_value = search_value.replace(" ","\ ")                    
        all_product_list=[]
        if key_flag=='s':
            for key,category,base1,base2,level,combination_category in category_with_key:
                if key==search_key.upper():                                                  
                    if len(search_value)>0:                             
                        query=f'{category}:{search_value}*'
                        df_product_combine=querying_solr_data(query,params)
                    else:
                        query=f'TYPE:{base1}'
                        df_product_combine=querying_solr_data(query,params)
                    all_product_list=all_product_list+product_level_creation(df_product_combine,combination_category,base1,base2,key,level)                                  
        elif len(search)>=2:
            for item in search_category:
                query=f'{item}:{search_value}*'
                df_product_combine=querying_solr_data(query,params)
                if len(df_product_combine)>0:
                    if item=="TEXT2": 
                        #for real specid 
                        all_product_list=all_product_list+product_level_creation(df_product_combine,product_rspec_category,"NAMPROD","REAL_SUB","RSPEC*","PRODUCT-LEVEL")
                        #cas level details    
                        all_product_list=all_product_list+product_level_creation(df_product_combine,cas_pspec_category,"NUMCAS","PURE_SUB","PSEPC*","CAS-LEVEL")
                    elif item=="TEXT1":
                        for ctype in category_type:
                            if ctype=="MATNBR":
                                all_product_list=all_product_list+product_level_creation(df_product_combine,material_number_category,"MATNBR",'',"MAT*","MATERIAL-LEVEL")
                            elif ctype=="NUMCAS":
                                all_product_list=all_product_list+product_level_creation(df_product_combine,cas_number_category,"NUMCAS","PURE_SUB","CAS*","CAS-LEVEL")
                            else:
                                all_product_list=all_product_list+product_level_creation(df_product_combine,product_nam_category,"NAMPROD","REAL_SUB","NAM*","PRODUCT-LEVEL")
                    else:
                        for ctype in category_type:
                            if ctype == "MATNBR":
                                all_product_list=all_product_list+product_level_creation(df_product_combine,material_bdt_category,"MATNBR",'',"BDT*","MATERIAL-LEVEL")
                            elif ctype == "NAMPROD":
                                all_product_list=all_product_list+product_level_creation(df_product_combine,product_namsyn_category,"NAMPROD","REAL_SUB","SYN*","PRODUCT-LEVEL")
                            else:
                                all_product_list=all_product_list+product_level_creation(df_product_combine,cas_chemical_category,"NUMCAS","PURE_SUB","CHEMICAL*","CAS-LEVEL")
            
        return all_product_list
    except Exception as e:
        print(e)
        return []

