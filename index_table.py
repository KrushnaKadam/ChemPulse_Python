#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install openpyxl')


# In[2]:


import pandas as pd
import pyodbc
import pandas as pd
import pyodbc
import urllib.parse
import urllib
import sqlalchemy
import datetime
import pandas as pd
import requests
import json,re,os
from pathlib import Path
from sqlalchemy import create_engine
import numpy as np
from scipy import stats
import os.path
import datetime
import ast

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
        self.conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(
            params)
        
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


# In[3]:


import pandas as pd
import numpy as np
import math 
xls = pd.ExcelFile(r"D:\AlixPartners\March\Axalta\RFP\RFP\RFI_index_data\IHS Feedstock Updation Workbook_ Global Market Analysis V2.xlsx")
df1 = pd.read_excel(xls, 'Refreshable File')

cnt=0
for i in (df1.columns):
    df1[i][0]=cnt
    cnt+=1
    
df1_selected=df1[0:15]

s = df1.xs(0)
s.name = 15
df1_selected=df1_selected.append(s)

df1_selected=df1_selected[9:16]

method=df1_selected.transpose()[3:]
new_header = method.iloc[0] #grab the first row for the header
method = method[1:] #take the data less the header row
method.columns = new_header 
method=method.reset_index()
method.dropna(axis = 0, how = 'all', inplace = True)
method.rename(columns = {3:'rank'}, inplace = True)
method=method[['Geography','Product','Grade','Concept','Terms','Unit','rank']]
method.dropna(subset=['Geography','Product','Grade','Concept','Terms','Unit'], how='all', inplace = True)
method.rename(columns = {'Geography':'geographic_region','Product':'product_name','Grade':'quality_grade','Concept':'concept','Terms':'terms','Unit':'unit'}, inplace = True)

db_data_method=new_dbobj.read_table('select * from chempulse_rfp.tbl_index_methods')
db_data_method['flag_db']=0
method['flag_local']=0
all_data_joined=method.merge(db_data_method,on=['geographic_region','product_name','quality_grade','concept','terms','unit'], how='left')
all_data_joined=all_data_joined[(all_data_joined['flag_db']!=0)&(all_data_joined['flag_local']==0)]
method_to_db=all_data_joined[['geographic_region','product_name','quality_grade','concept','terms','unit']]
method_to_db


# In[4]:


new_dbobj.insert_data(method_to_db,'tbl_index_methods','chempulse_rfp')


# In[5]:


df1_2nd=df1[15:]
new_row = df1.xs(0)

df1_2nd.dropna(subset=["You can refresh this spreadsheet in 'Data' menu."],inplace=True)
df1_2nd.dropna(axis=1, how='all',inplace=True)

df1_2nd=df1_2nd.append(new_row)
df1_2nd["You can refresh this spreadsheet in 'Data' menu."][0]=None

def bind_rank(val,rank):
    if not df1_2nd[rank][0]:
        return val
    
    if val and not math.isnan(val):
        listt=[]
        listt.append(val)
        listt.append(df1_2nd[rank][0])
        return listt
    return None

for i in df1_2nd.columns:
    df1_2nd[i]=df1_2nd.apply(lambda x: bind_rank(x[i], i), axis=1)

df1_2nd=df1_2nd.drop(0)
df1_2nd.dropna(axis=1, how='all',inplace=True)
df1_2nd['combined']= df1_2nd.values.tolist()


def remove_first(listt):
    return listt[1:]

df1_2nd['combined']=df1_2nd['combined'].apply(remove_first)
df1_2nd=df1_2nd[["You can refresh this spreadsheet in 'Data' menu.","combined"]]

exploded_df=df1_2nd.explode('combined')
exploded_df = exploded_df[exploded_df['combined'].notna()]
exploded_df[['price','rank']] = pd.DataFrame(exploded_df.combined.tolist(), index= exploded_df.index)

exploded_df.rename(columns = {"You can refresh this spreadsheet in 'Data' menu.":"index_date"}, inplace = True)
exploded_df['year'] = pd.DatetimeIndex(exploded_df['index_date']).year
exploded_df['month'] = pd.DatetimeIndex(exploded_df['index_date']).month

exploded_df=exploded_df[['index_date','price','year','month','rank']]

all_data=exploded_df.merge(method,on='rank',how='left')

db_data_method=new_dbobj.read_table('select * from chempulse_rfp.tbl_index_methods')
join_id=all_data.merge(db_data_method,on=['geographic_region','product_name','quality_grade','concept','terms','unit'], how='left')
join_id=join_id[['index_date','price','year','month','id']]
join_id.rename(columns = {'id':'method_id'}, inplace = True)

db_data_method=new_dbobj.read_table('select * from chempulse_rfp.tbl_index_price')
db_data_method.drop(['index_date'], axis=1,inplace=True)
db_data_method['flag_db']=0
join_id['flag_local']=0
join_id_to_insert=join_id.merge(db_data_method,on=['price', 'year', 'month', 'method_id'],how='left')
join_id_to_insert=join_id_to_insert[(join_id_to_insert['flag_db']!=0)&(join_id_to_insert['flag_local']==0)]
join_id_to_insert=join_id_to_insert[['index_date','price', 'year', 'month', 'method_id']]
join_id_to_insert


# In[6]:


new_dbobj.insert_data(join_id_to_insert,'tbl_index_price','chempulse_rfp')


# In[ ]:





# In[ ]:




