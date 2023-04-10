#!/usr/bin/env python
# coding: utf-8

# In[61]:


import pandas as pd
import os,random,re,numexpr, string ,nltk,ast,urllib.parse,urllib,sqlalchemy,datetime,json,pyodbc
from nltk.tokenize import wordpunct_tokenize
from sqlalchemy import create_engine


# In[37]:


def isNumeric(num):
    try:
        val=float(num)
        return True
    except:
        return False
    
def isPunctuation(lit):
    try:
        if lit in string.punctuation:#['*','/','+','-','(',')']:
            return True        
    except:
        return False
    
def is_valid_lit(val):
    try:
        if not isPunctuation(val) and not isNumeric(val):
            return True
        else:
            return False
    except:
        return False

def return_tokenized_String(each_string):
    val_list=wordpunct_tokenize(each_string)
    new_val_list=[]
    for each_str in val_list:
        if '.' in each_str and len(str(each_str).strip())>1:
            split_by_dot=each_str.split('.')
            cleand_str=[]
            for each_character in split_by_dot:
                if str(each_character).strip()!='':
                    cleand_str.append(each_character)
            if len(cleand_str)>0:
                cleand_str.append('.')
            val_list.remove(each_str)
            new_val_list.append(cleand_str)
    List_flat=[]
    for i in range(len(new_val_list)):
        for j in range (len(new_val_list[i])):
            List_flat.append(new_val_list[i][j])
    return val_list+List_flat

def convert_list_to_list_str(listt):
    new_list=[]
    for each in listt:
        new_list.append(str(each))
    return new_list
        
    
def add_Space_with_symbol(stringg):
    for punc in string.punctuation:
        stringg=stringg.replace(str(punc),' '+str(punc)+' ')
    return stringg

def remove_Space_with_symbol(stringg):
    for punc in string.punctuation:
        stringg=stringg.replace(' '+str(punc),str(punc))
        stringg=stringg.replace(str(punc)+' ',str(punc))
    return stringg


def is_valid_char(charr):
    if charr.isalpha() or charr.isdigit() or charr in string.punctuation or charr==' ':
        return True
    else:
        return False
    
    
def get_valid_char_str(strr):
    strr=str(strr)
    valid_str=''
    for charr in strr:
        if is_valid_char(charr):
            valid_str+=charr
    return valid_str

def clean_multiplication(val):
    try:
        return str(val).lower().replace(' x ',' * ')
    except:
        return None
    
def replace_comma_bet_digits(val):
    try:
        return re.sub('(?<=\d),(?=\d)', '.', str(val))
    except:
        return None
    
def clean_addition(val):
    try:
        return str(val).lower().replace(' plus ',' + ')
    except:
        return None
    
def formula_sheet_test_cases(val):
    val=clean_multiplication(val)
    val=replace_comma_bet_digits(val)
    val=clean_addition(val)
    return val


# In[38]:


xls = pd.ExcelFile(r"C:\Users\Krushna_Kadam\Downloads\Solvent_formulas.xlsx")
Solvent_formulas = pd.read_excel(xls, 'Alcohols')
new_header = Solvent_formulas.iloc[0] #grab the first row for the header
Solvent_formulas = Solvent_formulas[1:] #take the data less the header row
Solvent_formulas.columns = new_header 
Solvent_formulas

non_formula_cols=['NaN','ID Generator','TipsCode','MaterialDesc','City','Region','Delivery type','Incumbent 1','Incumbent 2']
formula_cols_list=[i for i in Solvent_formulas.columns if i not in non_formula_cols]
formulas_df=Solvent_formulas[formula_cols_list]
all_formula_list=[]

for i in formulas_df.columns:
    all_formula_list.append(list(set(list(formulas_df[i]))))
    
names_list=all_formula_list

List_flat=[]
for i in range(len(names_list)):
    for j in range (len(names_list[i])):
        List_flat.append(names_list[i][j])

unique_data=list(set(List_flat))

cleaned_unique_data =[]

for ele in unique_data:
    if ele and ele != '.' and ele !='No' and str(ele)!='nan' and str(ele)!='Recommended Formula cost drivers' and str(ele)!="List of indexes" and str(ele)!="Prefer Fixed Price (6m - 1 Year)" and len(str(ele))!=1:
        
        cleaned_unique_data.append(ele)
        
cleaned_unique_data


# In[39]:


file_path=r"D:\AlixPartners\April\Axalta\Axalta Esters RFP responses (1)\Axalta Esters Global RFP 2023 GREENCHEM.xlsx"
xls = pd.ExcelFile(file_path)
raw_rfp_file = pd.read_excel(xls, 'Proposal Sheet')
Supplier=raw_rfp_file['Unnamed: 3'][0]
raw_rfp_file.columns = raw_rfp_file.iloc[3]
raw_rfp_file=raw_rfp_file[3:]
raw_rfp_file=raw_rfp_file[['ID Generator','Recommended Formula cost drivers']].drop_duplicates()
raw_rfp_file['vendor_name']=Supplier
raw_rfp_file

cleaned_unique_data=list(set(raw_rfp_file['Recommended Formula cost drivers']))
cleaned_unique_data
# ID Generator
#Supplier
#Recommended Formula cost drivers 


# In[40]:


main_dictt={}

elements=[]
combi_str=""


# cleaned_unique_data=['x3+y3+z3=k']
for each_string in cleaned_unique_data:
    elements=[]
    combi_str=''
    each_string_backup_original=each_string
    
    each_string=get_valid_char_str(each_string)
    each_string_backup=each_string
    
    each_string=formula_sheet_test_cases(each_string)
    each_string=add_Space_with_symbol(each_string)
    tokenized_string=return_tokenized_String(each_string)
    for itr in range(len(tokenized_string)):
        if is_valid_lit(tokenized_string[itr]) and tokenized_string[itr] not in elements and str(tokenized_string[itr]).strip()!='.':
            combi_str+=tokenized_string[itr]+' '
        else:
            if len(combi_str.strip())>0 and combi_str.strip() not in elements:
                elements.append(combi_str.strip())
            combi_str=''
            
        if len(combi_str.strip())>0 and itr+1==len(tokenized_string) and combi_str.strip() not in elements:
            elements.append(combi_str.strip()) 
    
    innner_dict={}
    for ele in elements:
        listt=[]
        listt.append(random.randint(5,20))
        listt.append(len(str(ele)))
        innner_dict[ele]=listt
    
    backup_dictt={}
    backup_dictt['elements']=elements
    backup_dictt['original_name']=each_string_backup_original
    
    innner_dict[str(each_string_backup)+('for_backup_purpose_only')] = backup_dictt
            
    main_dictt[each_string_backup]=innner_dict
    
main_dictt    


# In[81]:


for each in main_dictt:
    print('\nActual    :',main_dictt[each][str(each)+'for_backup_purpose_only']['original_name'])
    each_clone=each
    listt=main_dictt[each][str(each)+'for_backup_purpose_only']['elements']
    listt=sorted(listt, key=len, reverse=True)
    
    each_clone_2=each_clone
    each_clone=add_Space_with_symbol(str(each_clone)).replace('  ',' ')
    
    for each_value in listt:
        each_clone=each_clone.replace(add_Space_with_symbol(each_value).replace('  ',' '),str(main_dictt[each][each_value][0]))
        each_clone_2=each_clone_2.lower().strip().replace(each_value.strip(),str(main_dictt[each][each_value][1]))
    print('Cleaned   :',each_clone,'\ncalculated:', each_clone_2)


# In[60]:


lit_list=[]
for each in main_dictt:
    inner_list=[]
    inside_list = list(main_dictt[each].keys())
    inside_list.remove(each+'for_backup_purpose_only')
    inner_list.append(inside_list)
    inner_list.append(each)
    lit_list.append(inner_list)

names_list=lit_list
names_list
lit_df=pd.DataFrame(names_list,columns=['literals','raw_formula'])
lit_df=lit_df.explode('literals')
lit_df
# lit_df.to_csv('new_file.csv',index=False)


# In[56]:


# raw_rfp_file.rename(columns = {'Recommended Formula cost drivers':'raw_formula'},inplace=True)
merge_final_df=lit_df.merge(raw_rfp_file,on='raw_formula',how='inner')
merge_final_df.rename(columns = {'literals':'compound_name','ID Generator':'id_generator'},inplace=True)
merge_final_df


# In[59]:


user_name='gtoadmin'
password='Admin@gt0' 
Database_Name='gtosqldbdev' 
Server_Name='gtosqlserverdev.database.windows.net'

class database:
    def __init__(self , SQLUser , SQLPassword , DatabaseName , ServerName ):
        driver="{ODBC Driver 17 for SQL Server}"
        self.conn = pyodbc.connect(f'''DRIVER={driver}; Server={ServerName }; 
                                UID={SQLUser}; PWD={SQLPassword}; DataBase={DatabaseName}''')
    
        server = ServerName
        database = DatabaseName
        user = SQLUser
        password = f"{SQLPassword}"
        driver = "{ODBC Driver 17 for SQL Server}"
        params = urllib.parse.quote_plus(f"""Driver={driver};
                                        Server=tcp:{server},1433;
                                        Database={database};
                                        Uid={user};Pwd={password};
                                        Encrypt=yes;
                                        TrustServerCertificate=no;
                                        Connection Timeout=30;""")
        self.conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(params)
        
    # Fetching the data from the selected table using SQL query
    def read_table(self,query):
        RawData= pd.read_sql_query(query, self.conn)
        return RawData
    
    def execute_query(self,query):
        cursor=self.conn.cursor()
        cursor.execute(query)
        cursor.commit()
        
    def close_conn_eng(self):
        self.conn.close()
    def insert_data(self, df, table_name ,schema_name):
        try :
            self.engine1 = create_engine(self.conn_str, fast_executemany=True)
            df.to_sql(table_name, con = self.engine1 ,if_exists='append', index=False,schema=schema_name)
        except Exception as e :
            print(e)    
            
new_dbobj=database(user_name , password , Database_Name , Server_Name)
# new_dbobj.insert_data(merge_final_df,'tbl_formula_compound','chempulse_rfp')


# In[58]:





# In[ ]:




