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
                    
        display_category=display_category[:-3] 
        json_category=json_category[:-3]       
        temp_df=temp_df[extract_column].values.tolist()
        for value1,value2,value3 in temp_df:
            value = str(value1).strip() + " | "+str(value2).strip()+" | "+str(value3).strip()
            out_dict={"name":value,"type":json_category,"key":key,"group":level_name+" ("+display_category+")"+" - "+str(total_count) }
            json_list.append(out_dict)
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

def selected_products(data_json):
    try:
        searched_product_list=[]
        count=0
        params={"rows":2147483647}
        product_count=0
        material_count=0
        cas_count=0
        column_add=[]
        product_level_flag=''
        material_level_flag=''
        cas_level_flag=''
        add_df=pd.DataFrame()
        print("selectedproducts",data_json)
        if len(data_json)<=2:
            for item in data_json:
                search_value = item.get("name")
                search_value_split = search_value.split(" | ")
                search_column = item.get("type")
                search_key = item.get("key")
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
                if material_level_flag=='' and cas_level_flag=='':                     
                    query=f'TYPE:MATNBR && TEXT2:{product_rspec}'
                    temp_df=querying_solr_data(query,params)
                    #to find material level details
                    searched_product_list=searched_product_list+product_level_creation(temp_df,material_number_category,"","","MAT*","MATERIAL-LEVEL","yes")
                    #to find cas level details
                    query=f'TYPE:SUBIDREL && TEXT2:{product_rspec}'
                    temp_df=querying_solr_data(query,params)                       
                    column_value = temp_df["TEXT1"].unique()
                    temp_df=pd.DataFrame()
                    for item in column_value: 
                        query=f'TYPE:NUMCAS && SUBCT:PURE_SUB && TEXT2:{item}'
                        add_df=querying_solr_data(query,params)
                        temp_df=pd.concat([temp_df,add_df])
                    #real spec will act as pure spec componant
                    query=f'TYPE:NUMCAS && TEXT2:{product_rspec}'
                    add_df=querying_solr_data(query,params)
                    temp_df=pd.concat([temp_df,add_df])
                    searched_product_list=searched_product_list+product_level_creation(temp_df,cas_number_category,"","","CAS*","CAS-LEVEL","yes")
                    
                elif material_level_flag=='s' and material_count==2 and cas_level_flag=='':
                    #to find cas level details
                    query=f'TYPE:SUBIDREL && TEXT2:{product_rspec}'
                    temp_df=querying_solr_data(query,params)
                    column_value = temp_df["TEXT1"].unique()
                    temp_df=pd.DataFrame()
                    for item in column_value: 
                        query=f'TYPE:NUMCAS && SUBCT:PURE_SUB && TEXT2:{item}'
                        add_df=querying_solr_data(query,params) 
                        temp_df=pd.concat([temp_df,add_df])
                    #real spec will act as pure spec componant 
                    query=f'TYPE:NUMCAS && TEXT2:{product_rspec}'
                    add_df=querying_solr_data(query,params)
                    temp_df=pd.concat([temp_df,add_df])
                    searched_product_list=searched_product_list+product_level_creation(temp_df,cas_number_category,"NUMCAS","PURE_SUB","CAS*","CAS-LEVEL","yes")
                    
                elif cas_level_flag=='s' and cas_count==2 and material_level_flag=='':
                    query=f'TYPE:MATNBR && TEXT2:{product_rspec}'
                    temp_df=querying_solr_data(query,params)
                    #to find material level details
                    searched_product_list=searched_product_list+product_level_creation(temp_df,material_number_category,"","","MAT*","MATERIAL-LEVEL","yes")
            elif material_level_flag =='s' and material_count==1:
                if product_level_flag =='' and cas_level_flag=='':
                    query=f'TYPE:MATNBR && TEXT1:{material_number}'
                    temp_df=querying_solr_data(query,params)
                    column_value = temp_df["TEXT2"].unique()
                    temp_df=pd.DataFrame()
                    for item in column_value:
                        # product level details
                        query=f'TYPE:NAMPROD && SUBCT:REAL_SUB && TEXT2:{item}'
                        add_df=querying_solr_data(query,params)
                        temp_df=pd.concat([temp_df,add_df])
                    searched_product_list=searched_product_list+product_level_creation(temp_df,product_rspec_category,"","","RSPEC*","PRODUCT-LEVEL","yes")                          
                        #cas level details
                    for item in column_value:
                        query=f'TYPE:SUBIDREL && TEXT2:{item}'
                        temp_df=querying_solr_data(query,params)
                        sub_column_value = temp_df["TEXT1"].unique()
                        temp_df=pd.DataFrame()
                        for element in sub_column_value: 
                            query=f'TYPE:NUMCAS && TEXT2:{element} && SUBCT:PURE_SUB'
                            add_df=querying_solr_data(query,params) 
                            temp_df=pd.concat([temp_df,add_df])
                    searched_product_list=searched_product_list+product_level_creation(temp_df,cas_number_category,"","","CAS*","CAS-LEVEL","yes")                         
                        
                elif product_level_flag =='s' and product_count ==2 and cas_level_flag=='':
                    query=f'TYPE:SUBIDREL && TEXT2:{product_rspec}'
                    temp_df=querying_solr_data(query,params) 
                    sub_column_value = temp_df["TEXT1"].unique()
                    temp_df=pd.DataFrame()
                    for element in sub_column_value:
                        query=f'TYPE:NUMCAS && TEXT2:{element} && SUBCT:PURE_SUB'
                        add_df=querying_solr_data(query,params)   
                        temp_df=pd.concat([temp_df,add_df])
                    searched_product_list=searched_product_list+product_level_creation(temp_df,cas_number_category,"","","CAS*","CAS-LEVEL","yes")                         
                        
                elif cas_level_flag=='s' and cas_count==2 and product_level_flag=='':
                    query=f'TYPE:MATNBR && TEXT1:{material_number}'
                    temp_df=querying_solr_data(query,params)
                    column_value = temp_df["TEXT2"].unique()
                    temp_df=pd.DataFrame()
                    for item in column_value:
                        query=f'TYPE:NAMPROD && SUBCT:REAL_SUB && TEXT2:{item}'
                        add_df=querying_solr_data(query,params)
                        # add_df=edit_df[(edit_df["TYPE"]=="NAMPROD") & (edit_df["SUBCT"]=="REAL_SUB") & (edit_df["TEXT2"]==item)]
                        temp_df=pd.concat([temp_df,add_df])
                    searched_product_list=searched_product_list+product_level_creation(temp_df,product_rspec_category,"","","RSPEC*","PRODUCT-LEVEL","yes")                                                  
            elif cas_level_flag=='s' and cas_count==1:
                if product_level_flag =='' and material_level_flag=='':
                    query=f'TYPE:SUBIDREL && TEXT1:{cas_pspec}'
                    temp_df=querying_solr_data(query,params)
                    column_value = temp_df["TEXT2"].unique()
                    temp_df=pd.DataFrame()
                    for item in column_value:
                        query=f'TYPE:NAMPROD && SUBCT:REAL_SUB && TEXT2:{item}'
                        add_df=querying_solr_data(query,params)
                        temp_df=pd.concat([temp_df,add_df])
                    #same pure-spec will be act as real-spec
                    query=f'TYPE:NAMPROD && TEXT2:{cas_pspec}'
                    add_df=querying_solr_data(query,params)
                    temp_df=pd.concat([temp_df,add_df])
                    searched_product_list=searched_product_list+product_level_creation(temp_df,product_rspec_category,"","","RSPEC*","PRODUCT-LEVEL","yes")
                    temp_df=pd.DataFrame()
                    for item in column_value:
                        query=f'TYPE:MATNBR && TEXT2:{item}'
                        add_df=querying_solr_data(query,params)                          
                        temp_df=pd.concat([temp_df,add_df])
                    searched_product_list=searched_product_list+product_level_creation(temp_df,material_number_category,"","","MAT*","MATERIAL-LEVEL","yes")

                elif product_level_flag =='s' and product_count ==2 and material_level_flag=='':
                    query=f'TYPE:MATNBR && TEXT2:{product_rspec}'
                    temp_df=querying_solr_data(query,params)
                    searched_product_list=searched_product_list+product_level_creation(temp_df,material_number_category,"","","MAT*","MATERIAL-LEVEL","yes")

                elif material_level_flag=='s' and material_count==2 and product_level_flag=='':
                    query=f'TYPE:MATNBR && TEXT1:{material_number}'
                    temp_df=querying_solr_data(query,params)
                    column_value = temp_df["TEXT2"].unique()
                    temp_df=pd.DataFrame()
                    for item in column_value:
                        # product level details
                        query=f'TYPE:NAMPROD && SUBCT:REAL_SUB && TEXT2:{item}'
                        add_df=querying_solr_data(query,params)
                        temp_df=pd.concat([temp_df,add_df]) 
                    searched_product_list=searched_product_list+product_level_creation(temp_df,product_rspec_category,"","","RSPEC*","PRODUCT-LEVEL","yes")                          
        return searched_product_list     
    except Exception as e:
        print(e)
        return searched_product_list
 