
# coding: utf-8

# In[1]:


import requests
import json
import math
import time
import os
import platform
import logging
import subprocess
from subprocess import Popen, PIPE
import pandas as pd
from dateutil.relativedelta import relativedelta
import datetime
from datetime import timedelta
from dateutil import tz
import dateutil.parser as dp
from google.cloud import bigquery
from google.cloud import storage


# In[2]:


def gendates(shoptimezone, min_date,max_date,rfreq):
    to_zone = tz.gettz(shoptimezone)
    dateranges = pd.date_range(start=min_date, end=max_date, freq=rfreq, tz=to_zone)
    dateranges = dateranges.union([min_date,max_date])
    dfdateranges = pd.DataFrame(dateranges)
    dfdateranges.columns=['start_date']
    dfdateranges['end_date'] = dfdateranges.start_date.shift(-1)
    dfdateranges = dfdateranges[:-1]
    dfdateranges['end_date'] = dfdateranges['end_date'] + datetime.timedelta(milliseconds=1)
    return dfdateranges


# In[3]:


def getcurtimeinshoptz(sz):
    from_zone = tz.tzlocal()
    to_zone = tz.gettz(sz)
    utc = datetime.datetime.now()
    utc = utc.replace(tzinfo=from_zone)
    currentshopdate = utc.astimezone(to_zone)
    return currentshopdate


# In[4]:


def getconvtimeinshoptz(sz, t):
    to_zone = tz.gettz(sz)
    currentshopdate = t.astimezone(to_zone)
    return currentshopdate


# In[5]:


def getcountandpages(countrurl, headers, cntparams):
    totalcnt = requests.get(countrurl, headers = headers, params = cntparams).json()['count']   
    nopages = math.ceil(totalcnt/limit) + 1
    return totalcnt,nopages


# In[6]:


def gettimezone(shoptimeurl, headers, cntparams):
    response = requests.get(shoptimeurl, headers = headers, params = cntparams).json()
    df = pd.DataFrame(response['shop'], index=[0])
    return df['iana_timezone'][0]


# In[26]:


def getfirstcreationdate(shopifycode, pageurl, headers, params):
    params.update({'order' : 'created_at asc'})
    response = requests.get(pageurl, headers = headers, params = params).json()
    df = pd.DataFrame(response[shopifycode])
    min_date = min(df['created_at'])
    print(min_date)
    return min_date


# In[30]:


def getshopifydates(rtype, rfreq, min_date, max_date, shopifycode, shoptimezone, countrurl, headers, cntparams, pageurl, params):
    if (rtype == runtype[0]) or (rtype == runtype[1] and min_date is None and max_date is None):
        currentshopdate = getcurtimeinshoptz(shoptimezone)
        to_zone = tz.gettz(shoptimezone)
        min_date_str = getfirstcreationdate(shopifycode, pageurl, headers, params)
        min_date = dp.parse(min_date_str)
        min_date = min_date.replace(tzinfo=to_zone)
        dates = gendates(shoptimezone, min_date, currentshopdate, rfreq)
    elif rtype == runtype[1]:
        if max_date is None:    
            dates = gendates(shoptimezone, min_date, currentshopdate, rfreq)
        else:    
            dates = gendates(shoptimezone, min_date, max_date, rfreq)
    return dates


# In[9]:


def pushtojson(dfcontents, dest_file_name):
    dfcontents.to_json(dest_file_name,orient="records",lines=True)


# In[10]:


def requestshopifydata(shopifycode, pageurl, params): 
    response = requests.get(pageurl, headers = headers, params = params)
    df = pd.DataFrame(response.json()[shopifycode])
    return df


# In[11]:


def getclient_details(client_name):
    client = bigquery.Client()
    query = """
        select * from sarasdata.client_details
        WHERE client_name = @client_name
        ORDER BY client_id DESC;
        """
    query_params = [
        bigquery.ScalarQueryParameter('client_name', 'STRING', client_name)
    ]
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    query_job = client.query(query, job_config=job_config)

    query_job.result()  # Wait for job to complete

    # Print the results.
    destination_table_ref = query_job.destination
    table = client.get_table(destination_table_ref)
    table_data = None
    for row in client.list_rows(table):
        table_data = row
    return table_data


# In[12]:


def getvendor_details(vendor_name):
    client = bigquery.Client()
    query = """
        select * from sarasdata.vendor_details
        WHERE vendor_name = @vendor_name
        ORDER BY vendor_id DESC;
        """
    query_params = [
        bigquery.ScalarQueryParameter('vendor_name', 'STRING', vendor_name)
    ]
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    query_job = client.query(query, job_config=job_config)

    query_job.result()  # Wait for job to complete

    # Print the results.
    destination_table_ref = query_job.destination
    table = client.get_table(destination_table_ref)
    table_data = None
    for row in client.list_rows(table):
        table_data = row
    return table_data


# In[13]:


def getclient_shopify_entitilements(client_id):
    client = bigquery.Client()
    query = """
        select * from sarasdata.client_shopify_entitilements
        WHERE client_id = @client_id
        ORDER BY client_id DESC;
        """
    query_params = [
        bigquery.ScalarQueryParameter('client_id', 'INTEGER', client_id)
    ]
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    query_job = client.query(query, job_config=job_config)

    query_job.result()  # Wait for job to complete

    # Print the results.
    destination_table_ref = query_job.destination
    table = client.get_table(destination_table_ref)
    table_data = None
    for row in client.list_rows(table):
        table_data = row
    return table_data


# In[14]:


def getlastupdateddate(dataset_name, table_name):
    client = bigquery.Client()
    query = "select max(updated_at) max_updated_dt from " + dataset_id + "." + table_name + ";"
    job_config = bigquery.QueryJobConfig()
    query_job = client.query(query, job_config=job_config)

    query_job.result()  # Wait for job to complete

    # Print the results.
    destination_table_ref = query_job.destination
    table = client.get_table(destination_table_ref)
    table_data = None
    for row in client.list_rows(table):
        table_data = row
    return table_data


# In[15]:


def deleteexistingrows(dataset_name, table_name, ids):
    client = bigquery.Client()

    query = "delete from " + dataset_id + "." + table_name + " where id in UNNEST(@ids);"
    query_params = [
        bigquery.ArrayQueryParameter('ids', 'INTEGER', ids)
    ]

    print(query)
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    job_config.query_parameters = query_params
    query_job = client.query(query, job_config=job_config)

    query_job.result()  # Wait for job to complete
    
    # Print the results.
    destination_table_ref = query_job.destination
    table = client.get_table(destination_table_ref)
    table_data = None
    for row in client.list_rows(table):
        table_data = row
    return table_data


# In[16]:


def updateloadtracker(dataset_name, table_name, ids):
    client = bigquery.Client()

    query = "delete from " + dataset_id + "." + table_name + " where id in UNNEST(@ids);"
    query_params = [
        bigquery.ArrayQueryParameter('ids', 'INTEGER', ids)
    ]

    print(query)
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    job_config.query_parameters = query_params
    query_job = client.query(query, job_config=job_config)

    query_job.result()  # Wait for job to complete
    
    # Print the results.
    destination_table_ref = query_job.destination
    table = client.get_table(destination_table_ref)
    table_data = None
    for row in client.list_rows(table):
        table_data = row
    return table_data


# In[17]:


def loadlocalfiletogooglestorage(batfile, source_file_name, dest_file_name):
    pass_arg=[]
    pass_arg.append(batfile)
    pass_arg.append(source_file_name)
    pass_arg.append(dest_file_name)
    p = Popen(pass_arg, stdout=PIPE, stderr=PIPE)
    output, errors = p.communicate()
    p.wait() # wait for process to terminate
    print(output)
    print(errors)


# In[18]:


def loadfiletobigquery(file_name, dataset_id, table_name, delimitertype, loadtype, skipheader):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_name)
    job_config = bigquery.LoadJobConfig()
    if skipheader is not None:
        job_config.skip_leading_rows = skipheader
    job_config.source_format = delimitertype
    if delimitertype == bigquery.SourceFormat.CSV:
        job_config.autodetect = True
    job_config.write_disposition = loadtype

    load_job = client.load_table_from_uri(
        file_name,
        table_ref,
        job_config=job_config)  # API request

    assert load_job.job_type == 'load'

    load_job.result()  # Waits for table load to complete.

    assert load_job.state == 'DONE'


# In[19]:


app_path = os.getcwd()
gspath = 'gs://sarasmaster'
os.chdir(os.getcwd())
filesep = '\\' if platform.system() == 'Windows' else '/'
gssep = '/'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "creds" + filesep + "sarasmaster-524142bf5547.json"
gcspath = 'C:\\Users\\kabhi\\AppData\\Local\\Google\\Cloud SDK\\google-cloud-sdk\\bin'
os.environ["PATH"] += os.pathsep + gcspath
batfile = app_path + filesep + 'movetogcs.bat' if platform.system() == 'Windows' else app_path + filesep + 'movetogcs.sh'
delimitertype = 'NEWLINE_DELIMITED_JSON'
loadtype = 'WRITE_APPEND'


# In[27]:


client_details = getclient_details('Kopari Beauty')
client_shopify_entitilements = getclient_shopify_entitilements(client_details.client_id)
shopifyurl= client_shopify_entitilements.shop_url
cloud_storage_dir = client_shopify_entitilements.cloud_storage_dir
access_token = client_shopify_entitilements.access_token
project_id = client_details.project_id
dataset_id = client_shopify_entitilements.dataset_id
pageno = 1
limit = 250

filepath = app_path + filesep + client_shopify_entitilements.cloud_storage_dir + filesep + 'shopify'
gcspath = gspath + gssep + client_shopify_entitilements.cloud_storage_dir + gssep + 'shopify'
logpath = app_path + filesep + client_shopify_entitilements.cloud_storage_dir + filesep + 'logs'
hdlr = logging.FileHandler(logpath + filesep + 'shopify_' + datetime.datetime.now().strftime('%Y%m%d') + '.log')
logger = logging.getLogger(__name__)
print(logpath + filesep + 'shopify_' + datetime.datetime.now().strftime('%Y%m%d') + '.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)
logger.info('Start Shopify Load Process')

status = 'any'
runtype = ['full','incremental']
headers = {'content-type' : 'application/json', 'X-Shopify-Access-Token' : access_token}
urlparams = {'limit': limit, 'status' : status}
cnturlparams = {'status' : status}
dot = '.'
skipheader = None
shopurl = shopifyurl + '/admin/shop.json'
shopifycodes = {
    'shopifycodes': ['orders', 'customers', 'products'],
    'pageurl': [shopifyurl + '/admin/orders.json', shopifyurl + '/admin/customers.json', shopifyurl + '/admin/products.json'],
    'countrurl': [shopifyurl + '/admin/orders/count.json', shopifyurl + '/admin/customers/count.json', shopifyurl + '/admin/products/count.json'],
    'dest_file_name': [filepath + filesep + 'orders' + filesep + 'orders', filepath + filesep + 'customers' + filesep + 'customers', filepath + filesep + 'products' + filesep + 'products'],
    'gs_file_path': [gcspath + gssep + 'orders', gcspath + gssep + 'customers', gcspath + gssep + 'products'],
    'gs_file_name': [gcspath + gssep + 'orders' + gssep + 'orders', gcspath + gssep + 'customers' + gssep + 'customers', gcspath + gssep + 'products' + gssep + 'products'],
    'dest_table_name': ['shopify_orders', 'shopify_customers', 'shopify_products'],
    'dest_file_type': ['json', 'json', 'json']
}

dfshopifycodes = pd.DataFrame(shopifycodes)


# In[28]:


shoptimezone = gettimezone(shopurl, headers, cnturlparams)
currentshopdate = getcurtimeinshoptz(shoptimezone)
runmode = runtype[1]
runfreq = 'D'
for row_index,row in dfshopifycodes.iterrows():
    lastshopdate = getlastupdateddate(dataset_id, row['dest_table_name']).max_updated_dt
    if lastshopdate is not None:
        lastshopdate = getconvtimeinshoptz(shoptimezone, lastshopdate)
        lastshopdate = lastshopdate + datetime.timedelta(milliseconds=1)
        start_date = lastshopdate
        end_date = currentshopdate
    else:
        start_date = None
        end_date = None
        
    dates = getshopifydates(runmode,runfreq,start_date,end_date,row['shopifycodes'], shoptimezone, row['countrurl'], headers, cnturlparams, row['pageurl'], urlparams)
    ids = []
    localfilelist = []
    gcsfilelist = []
    for dates_index, dates_row in dates.iterrows():
        cntparams = cnturlparams
        cntparams.update({'updated_at_min' : dates_row['start_date'],'updated_at_max' : dates_row['end_date']})
        totalcnt,nopages = getcountandpages(row['countrurl'], headers, cntparams)
        print("Table Name:" + row['shopifycodes'])
        print("Total Count:" + str(totalcnt))
        print("Total Pages:" + str(nopages))
        print("Start Date:" + dates_row['start_date'].strftime('%Y%m%d%H%M%S'))
        print("End Date:" + dates_row['end_date'].strftime('%Y%m%d%H%M%S'))
        df = pd.DataFrame()
        for i in range(1,nopages):
            params = urlparams
            params.update({'page': i,'updated_at_min' : dates_row['start_date'],'updated_at_max' : dates_row['end_date']})
            df1 = requestshopifydata(row['shopifycodes'], row['pageurl'], params)
            df=df.append(df1,ignore_index=True)
            time.sleep(1)
        ids.extend(df['id'].tolist()) if df.shape[0] > 0 else ids
        localfilename = row['dest_file_name'] + '_' + dates_row['start_date'].strftime('%Y%m%d') + dot + row['dest_file_type']
        gcsfilename = row['gs_file_name'] + '_' + dates_row['start_date'].strftime('%Y%m%d') + dot + row['dest_file_type']
        localfilelist.append(localfilename)
        gcsfilelist.append(gcsfilename)
        pushtojson(df, localfilename)
    
    print("Number of ids to be checked for delete:" + str(len(ids)))
    #deleteexistingrows(dataset_id, row['dest_table_name'], ids)        
    for localfilename in localfilelist:
        loadlocalfiletogooglestorage(batfile, localfilename, row['gs_file_path'])
        #os.remove(localfilename)
    #for gcsfilename in gcsfilelist:
        #loadfiletobigquery(gcsfilename, dataset_id, row['dest_table_name'], delimitertype, loadtype, skipheader)

