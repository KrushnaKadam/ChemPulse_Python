#!/usr/bin/env python
# coding: utf-8

# In[2]:


import requests,ast,re,time
from string import digits
mapsPrimaryKey='O9sxzPqSj-hKU-1ECdcIYwZikxOQao-7HACsAMD2ltw'


# In[80]:


def azure_maps_lat_long(address):
    try:
        address_copy=address
        flag=0
        
        url=f'https://atlas.microsoft.com/search/address/json?&subscription-key={mapsPrimaryKey}&api-version=1.0&language=en-US&query='+str(address)
        addr_listtt=ast.literal_eval(requests.get(url).text)['results']
        
        address=address.replace('#','').upper()
        unwanted_words=['NO.1','Road','RD','MAIN' 'PHONE:','Main', 'Fax:','Floor','PHONE','STREET','TEL','van','telephone','no','PREMISES','CONTACT','#', 'company','ECONOMIC', 'DEVELOPMENT' ,'ZONE','&']
        unwanted_words=[word.upper() for word in unwanted_words]
        list_address=address.split()
        for word in unwanted_words:
            address=address.replace(word,'')
#             if word.upper().strip() in unwanted_words:
#                 address=address.replace(word,'')
           
        text=address
        remove_digits = str.maketrans('', '', digits)
        text = text.translate(remove_digits)
        text=text.replace('.', '')
#         text =re.sub(r"\b[a-zA-Z]\b", "", text)
        text=text.replace('/', ' ')
        text=text.replace("-", ' ')
        text=text.replace("?", '')
        text=text.replace("Â", '')
        text=text.replace("Ã", '')
        text=text.replace(",", ', ')
#         text=text.replace('Ã??Ã?Â¼','u') 
        address=text
        
        if len(addr_listtt)==0:
            url=f'https://atlas.microsoft.com/search/address/json?&subscription-key={mapsPrimaryKey}&api-version=1.0&language=en-US&query='+str(address)
            addr_listtt=ast.literal_eval(requests.get(url).text)['results']
            
        if len(addr_listtt)==0:
            address=address.upper()
            address_list=address.split()
#             print(address_list)
            if address_list[0]=='OFFICE':
                address=address.replace('OFFICE','')                
            for word in address_list:
                if len(word)<=3:
                    address=address.replace(word,'')
            url=f'https://atlas.microsoft.com/search/address/json?&subscription-key={mapsPrimaryKey}&api-version=1.0&language=en-US&query='+str(address)
            addr_listtt=ast.literal_eval(requests.get(url).text)['results']
        
        
        if len(addr_listtt)==0:
            address=address.replace(address.split()[0],'')
            address_list=address.split()
            for word in address_list:
                if len(word)<=3:
                    address=address.replace(word,'')

            url=f'https://atlas.microsoft.com/search/address/json?&subscription-key={mapsPrimaryKey}&api-version=1.0&language=en-US&query='+str(address)
            addr_listtt=ast.literal_eval(requests.get(url).text)['results']

              
        if len(addr_listtt)==0:
            address_list=address.split()
            if len(address_list)>5:
                for bigram in range(len(address_list)-4):
                    addr=str(address_list[bigram])+' '+str(address_list[bigram+1])+' '+str(address_list[bigram+2])+' '+str(address_list[bigram+3])+' '+str(address_list[bigram+4])
                    url=f'https://atlas.microsoft.com/search/address/json?&subscription-key={mapsPrimaryKey}&api-version=1.0&language=en-US&query='+str(addr)
                    addr_listtt=ast.literal_eval(requests.get(url).text)['results']
                    if len(addr_listtt)>0:
                        address=addr
                        break

          
        if len(addr_listtt)==0:
            address_list=address.split()
            if len(address_list)>4:
                for bigram in range(len(address_list)-3):
                    addr=str(address_list[bigram])+' '+str(address_list[bigram+1])+' '+str(address_list[bigram+2])+' '+str(address_list[bigram+3])                        
                    url=f'https://atlas.microsoft.com/search/address/json?&subscription-key={mapsPrimaryKey}&api-version=1.0&language=en-US&query='+str(addr)
                    addr_listtt=ast.literal_eval(requests.get(url).text)['results']
                    if len(addr_listtt)>0:
                        address=addr
                        break


        if len(addr_listtt)==0:
            address_list=address.split()
            if len(address_list)>3:
                for bigram in range(len(address_list)-2):
                    addr=str(address_list[bigram])+' '+str(address_list[bigram+1])+' '+str(address_list[bigram+2])

                    url=f'https://atlas.microsoft.com/search/address/json?&subscription-key={mapsPrimaryKey}&api-version=1.0&language=en-US&query='+str(addr)
                    addr_listtt=ast.literal_eval(requests.get(url).text)['results']
                    if len(addr_listtt)>0:
                        address=addr
                        break

        if len(addr_listtt)==0:
            address_list=address.split()
            for bigram in range(len(address_list)-1):
                addr=str(address_list[bigram])+' '+str(address_list[bigram+1])

                url=f'https://atlas.microsoft.com/search/address/json?&subscription-key={mapsPrimaryKey}&api-version=1.0&language=en-US&query='+str(addr)
                addr_listtt=ast.literal_eval(requests.get(url).text)['results']
                if len(addr_listtt)>0:
                    address=addr
                    break

        if len(addr_listtt)==0:
            address_list=address.split()                
            for itr in range(len(address_list)-1):
                url=f'https://atlas.microsoft.com/search/address/json?&subscription-key={mapsPrimaryKey}&api-version=1.0&language=en-US&query='+str(address_list[itr])+' '+str(address_list[itr+1])
                addr_listtt=ast.literal_eval(requests.get(url).text)['results']
                if len(addr_listtt)>0:
                    address=str(address_list[itr])+' '+str(address_list[itr+1])
                    break

        if len(addr_listtt)==0:
            for word in address_list:
                url=f'https://atlas.microsoft.com/search/address/json?&subscription-key={mapsPrimaryKey}&api-version=1.0&language=en-US&query='+str(word)
                addr_listtt=ast.literal_eval(requests.get(url).text)['results']
                if len(addr_listtt)>0:
                    address=word
                    break
        
        maxx=0.0
        cnt=0
        best_index=0
        
        for itr in addr_listtt:
            if float(itr['score'])>maxx:
                maxx=float(itr['score'])
#             if float(itr['matchConfidence']['score'])>maxx:
#                 maxx=float(itr['matchConfidence']['score'])
                best_index=cnt
            cnt+=1

        try:
            state=addr_listtt[best_index]['address']['countrySubdivision'][0:49]
            state=''.join([i if ord(i) < 128 else '' for i in state])
        except:
            state=None
        try:
            country=addr_listtt[best_index]['address']['country'][0:49]
            country=''.join([i if ord(i) < 128 else '' for i in country])
        except:
            country=None
        try:
            country_code=addr_listtt[best_index]['address']['countryCode'][0:49]
            country_code=''.join([i if ord(i) < 128 else '' for i in country_code])
        except:
            country_code=None
        try:
            city=addr_listtt[best_index]['address']['countrySecondarySubdivision'][0:49]
            city=''.join([i if ord(i) < 128 else '' for i in city])
        except:
            city=None
        try:
            lat=addr_listtt[best_index]['position']['lat']
        except:
            lat=None
        try:
            lon=addr_listtt[best_index]['position']['lon']
        except:
            lon=None
        
        address=str(address.replace(';',''))
        address=''.join([i if ord(i) < 128 else '' for i in address])

        addr_dict={'city':city,'state':state,'country':country,'country_code':country_code,'lat':lat,'lon':lon,'address2':address}
        
        input_address=re.sub(r'[^A-Za-z0-9 ]+', '', address_copy)
        
        input_address=''.join([i for i in input_address if not i.isdigit()])
        input_address=input_address.lower()
        address_list=input_address.split()
        
        
        if len(address_list)<4:
            flag=0
            for i in addr_dict:
                addr_dict[i]=str(addr_dict[i]).lower().strip()
            
            address2_copy=addr_dict['address2']
            addr_dict['address2']=None
                            
            for i in address_list:
                if i.strip().lower() in addr_dict.values() or ' '.join([str(elem) for elem in address_list]) in addr_dict.values():
                    flag=1
                    addr_dict['address2']=address2_copy
                    break
            if flag==0:
                addr_dict={'city':None,'state':None,'country':None,'country_code':None,'lat':None,'lon':None,'address2':None}
                    
        addr_keys=list(addr_dict.keys())
        
        for key in addr_keys:
            addr_dict['azure_map_'+str(key)]=addr_dict[key]
            addr_dict.pop(key)
            
        print(addr_dict)
                    
        return addr_dict
    except Exception as e:
        return None

# pd_address_df['azure_maps_output']=pd_address_df['address'].apply(azure_maps_lat_long)

# print(pd_address_df)

# pandas_df = pd.concat([pd_address_df, pd_address_df["azure_maps_output"].apply(pd.Series)], axis=1)
# pandas_df=pandas_df.drop('azure_maps_output',axis=1)
# pandas_df


# In[81]:


azure_maps_lat_long('india')


        


# In[ ]:




