
# coding: utf-8

# In[1]:


from mws import mws
from io import StringIO
import pandas as pd
from xml.etree import ElementTree
import os
import platform
import re
from dateutil.relativedelta import relativedelta
import datetime
import time
import logging
import subprocess
from subprocess import Popen, PIPE
from google.cloud import bigquery
from google.cloud import storage


# In[2]:


app_path = os.getcwd()
gspath = 'gs://sarasmaster'
os.chdir(os.getcwd())
filesep = '\\' if platform.system() == 'Windows' else '/'
gssep = '/'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "creds" + filesep + "sarasmaster-524142bf5547.json"
gcspath = 'C:\\Users\\kabhi\\AppData\\Local\\Google\\Cloud SDK\\google-cloud-sdk\\bin'
os.environ["PATH"] += os.pathsep + gcspath
batfile = app_path + filesep + 'movetogcs.bat' if platform.system() == 'Windows' else app_path + filesep + 'movetogcs.sh'


# In[3]:


def gendates(min_date,max_date,rfreq):
    dateranges = pd.date_range(start=min_date, end=max_date, freq=rfreq)
    dateranges = dateranges.union([min_date,max_date])
    dfdateranges = pd.DataFrame(dateranges)
    dfdateranges.columns=['start_date']
    dfdateranges['end_date'] = dfdateranges.start_date.shift(-1)
    dfdateranges = dfdateranges[:-1]
    dfdateranges['start_date'] = dfdateranges['start_date'] + datetime.timedelta(seconds=1)
    return dfdateranges


# In[4]:


def getlastupdateddate(dataset_name, table_name):
    client = bigquery.Client()
    query = "select max(LastUpdateDate) max_updated_dt from " + dataset_id + "." + table_name + ";"
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


# In[5]:


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


# In[6]:


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


# In[7]:


def getclient_entitilements(client_id):
    client = bigquery.Client()
    query = """
        select * from sarasdata.client_amazonmws_entitlements
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


# In[8]:


def createloadtracker(dataset_id,table_name,file_names,date_from,date_to):
    load_id = uuid.uuid4()
    dfnew = pd.DataFrame(columns=['load_id','dataset_id','table_name','file_names',
    'date_from','date_to','loaded_to_bigquery','bigquery_load_date','creation_date',
    'update_date','load_script_version','load_script_file_name'])
    row = dict()
    row['load_id'] = str(load_id)
    row['dataset_id'] = dataset_id
    row['table_name'] = table_name
    row['file_names'] = file_names
    row['date_from'] = date_from.replace(tzinfo=None)
    row['date_to'] = date_to.replace(tzinfo=None)
    row['loaded_to_bigquery'] = False
    row['bigquery_load_date'] = None
    row['creation_date'] = datetime.datetime.now()
    row['update_date'] = datetime.datetime.now()
    row['load_script_version'] = 'v1'
    row['load_script_file_name'] = 'shopifyextract.py'
    #row_s = pd.Series(row)    
    #print(row_s)
    #dfnew = dfnew.append(row_s,ignore_index=True)
    return row


# In[9]:


def requestreport(report_type, start_date, end_date):
    x = mws.Reports(access_key=access_key, secret_key=secret_key, account_id=merchant_id, auth_token=auth_token)
    report = x.request_report(report_type=report_type,start_date=start_date,end_date=end_date)
    return report


# In[10]:


def getreportrequestid(report):
    xmlstring = re.sub(' xmlns="[^"]+"', '', report.original, count=1)
    tree = ElementTree.fromstring(xmlstring)
    reporttype = tree.findtext('.//ReportType')
    reportrequestid = tree.findtext('.//ReportRequestId')
    return reporttype,reportrequestid  


# In[11]:


def getreportrequeststatus(reporttype,reportrequestid):
    x = mws.Reports(access_key=access_key, secret_key=secret_key, account_id=merchant_id, auth_token=auth_token)
    reportrequeststatus = x.get_report_request_list(reportrequestid,reporttype)
    return reportrequeststatus


# In[12]:


def checkreportrequeststatus(reportrequeststatus):
    xmlstring = re.sub(' xmlns="[^"]+"', '', reportrequeststatus.original, count=1)
    tree = ElementTree.fromstring(xmlstring)
    reportrequestid = tree.findtext('.//ReportRequestId')
    reportprocessingstatus = tree.findtext('.//ReportProcessingStatus')
    if reportprocessingstatus == '_IN_PROGRESS_' or reportprocessingstatus == '_SUBMITTED_':
        checkreportrequeststatus =  1
    elif reportprocessingstatus == '_DONE_':
        checkreportrequeststatus =  2
    else:
        checkreportrequeststatus =  3
    return checkreportrequeststatus


# In[13]:


def getreportgeneratedid(reportrequeststatus):
    xmlstring = re.sub(' xmlns="[^"]+"', '', reportrequeststatus.original, count=1)
    tree = ElementTree.fromstring(xmlstring)
    reporttype = tree.findtext('.//ReportType')
    reportrequestid = tree.findtext('.//ReportRequestId')
    reportprocessingstatus = tree.findtext('.//ReportProcessingStatus')
    generatedreportid = tree.findtext('.//GeneratedReportId')
    return reporttype,reportrequestid,reportprocessingstatus,generatedreportid


# In[14]:


def getreport(generatedreportid,start_date,end_date):
    app_path = os.getcwd()
    os.chdir(os.getcwd())
    filepath = app_path + filesep + 'kopari' + filesep + 'amazonmws'+ filesep
    reportid = generatedreportid
    x = mws.Reports(access_key=access_key, secret_key=secret_key, account_id=merchant_id, auth_token=auth_token)
    report = x.get_report(report_id=reportid)
    df = pd.read_csv(StringIO(report.original.decode('ISO-8859-1')), sep="\t")
    source_file_name = filepath + 'orders_' + start_date + '_' + end_date + '.csv'
    dest_file_name = gspath + gssep + 'kopari' + gssep + 'amazonmws'
    df.to_csv(source_file_name)
    loadlocalfiletogooglestorage(batfile, source_file_name, dest_file_name)


# In[15]:


def runreports():
    for dates_index, dates_row in dates.iterrows():
        start_date_str = dates_row['start_date'].strftime('%Y%m%d')
        end_date_str = dates_row['end_date'].strftime('%Y%m%d')
        start_date = dates_row['start_date'].isoformat()
        end_date = dates_row['end_date'].isoformat()
        report = requestreport(report_type, start_date, end_date)
        reporttype,reportrequestid = getreportrequestid(report)
        time.sleep(30)
        reportrequeststatus = getreportrequeststatus(reporttype,reportrequestid)
        reporttype,reportrequestid,reportprocessingstatus,generatedreportid = getreportgeneratedid(reportrequeststatus)
        proceednext = False
        while True:
            if checkreportrequeststatus(reportrequeststatus) == 1:
                time.sleep(30)
                reportrequeststatus = getreportrequeststatus(reporttype,reportrequestid)
            elif checkreportrequeststatus(reportrequeststatus) == 2:
                reporttype,reportrequestid,reportprocessingstatus,generatedreportid = getreportgeneratedid(reportrequeststatus)
                getreport(generatedreportid,start_date_str,end_date_str)
                time.sleep(60)
                proceednext = True
                break
            else:
                print(reportrequeststatus.original)
                break


# In[16]:


def getnexttoken(orders):
    xmlstring = re.sub(' xmlns="[^"]+"', '', orders.original, count=1)
    tree = ElementTree.fromstring(xmlstring)
    nexttoken = tree.findtext('.//NextToken')
    return nexttoken


# In[17]:


def listordersbynexttoken(nexttoken):
    x = mws.Orders(access_key=access_key, secret_key=secret_key, account_id=merchant_id, auth_token=auth_token)
    orders = x.list_orders_by_next_token(nexttoken)
    return orders   


# In[18]:


def writetofile(orders,localfilename):
    f =  open(localfilename, "w", encoding="utf-8")
    f.write(orders.original)
    f.close()


# In[19]:


def listorders(start_date,end_date,filepath,start_date_str,end_date_str):
    app_path = os.getcwd()
    os.chdir(os.getcwd())
    pageno = 1
    localfilename = filepath + filesep + 'orders_'  + start_date_str + str(pageno) + '.xml'
    x = mws.Orders(access_key=access_key, secret_key=secret_key, account_id=merchant_id, auth_token=auth_token)
    orders = x.list_orders(marketplaceids=marketplaceId,created_after=start_date)
    writetofile(orders,localfilename)
    nexttoken = getnexttoken(orders)
    pageno = pageno + 1
    print(nexttoken)
    while True:
        if nexttoken is None:
            break
        else:
            time.sleep(60)
            orders = listordersbynexttoken(nexttoken)
            localfilename = filepath + filesep + 'orders_'  + start_date_str + str(pageno) + '.xml'
            pageno = pageno + 1
            writetofile(orders,localfilename)            
            nexttoken = getnexttoken(orders)
            print(nexttoken)


# In[20]:


def listorderitems(orderids,filepath):
    for orderid in orderids:
        x = mws.Orders(access_key=access_key, secret_key=secret_key, account_id=merchant_id, auth_token=auth_token)
        orderitem = x.list_order_items(orderid)
        localfilename = filepath + filesep + 'orderitem_' + str(orderid) + '.xml'
        writetofile(orderitem,localfilename)
        time.sleep(3)


# In[21]:


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


# In[22]:


def extractorderids(filepath):
    orderids=[]
    for filename in os.listdir(filepath):
        if not filename.endswith('.xml'): continue
        fullname = os.path.join(filepath, filename)
        linestring = open(fullname, 'r', encoding="utf-8").read()
        xmlstring = re.sub(' xmlns="[^"]+"', '', linestring, count=1)
        tree = ElementTree.fromstring(xmlstring)
        for elt in tree.iter('Order'):
            AmazonOrderId = elt.findtext('AmazonOrderId')
            orderids.append(AmazonOrderId)
    return orderids


# In[23]:


def convertorderxmltocsv(filepath,dest_file_name):
    df = pd.DataFrame(columns=('LatestShipDate','OrderType','PurchaseDate','AmazonOrderId','BuyerEmail',
    'IsReplacementOrder','LastUpdateDate','NumberOfItemsShipped','ShipServiceLevel','OrderStatus',
    'SalesChannel','IsBusinessOrder','NumberOfItemsUnshipped','PaymentMethodDetail','BuyerName',
    'CurrencyCode','Amount','IsPremiumOrder','EarliestShipDate','MarketplaceId','FulfillmentChannel',
    'PaymentMethod','City','PostalCode','StateOrRegion','CountryCode','Name','AddressLine1',
    'AddressLine2','IsPrime','ShipmentServiceLevelCategory','SellerOrderId','CreatedBefore','NextToken','RequestId'))
    for filename in os.listdir(filepath):
        if not filename.endswith('.xml') or filename.startswith('orderitem'): continue
        fullname = os.path.join(filepath, filename)
        linestring = open(fullname, 'r', encoding="utf-8").read()
        xmlstring = re.sub(' xmlns="[^"]+"', '', linestring, count=1)
        tree = ElementTree.fromstring(xmlstring)
        for elt in tree.iter('Order'):
            row = dict()
            for item in elt.iter():
                row[item.tag] = item.text
            row_s = pd.Series(row)      
            df = df.append(row_s, ignore_index=True)
    df.to_csv(dest_file_name, index=False)


# In[24]:


def convertorderitemsxmltocsv(filepath,dest_file_name):
    df = pd.DataFrame(columns=('AmazonOrderId','QuantityOrdered','Title','PromotionDiscountAmount','PromotionDiscountCurrencyCode','IsGift',
    'ASIN','SellerSKU','OrderItemId','NumberOfItemsOrdered','QuantityShipped','ItemPriceAmount','ItemPriceCurrencyCode','ItemTaxCurrencyCode','ItemTaxAmount'))
    for filename in os.listdir(filepath):
        if not filename.endswith('.xml'): continue
        fullname = os.path.join(filepath, filename)
        print(fullname)
        linestring = open(fullname, 'r', encoding="utf-8").read()
        xmlstring = re.sub(' xmlns="[^"]+"', '', linestring, count=1)
        tree = ElementTree.fromstring(xmlstring)    
        AmazonOrderId = tree.findtext('.//AmazonOrderId')
        for elt in tree.iter('OrderItem'):
            row = dict()
            row['AmazonOrderId'] = AmazonOrderId
            row['QuantityOrdered'] = elt.findtext('QuantityOrdered')
            row['Title'] = elt.findtext('Title')
            row['PromotionDiscountAmount'] = elt.findtext('.//PromotionDiscount/Amount')
            row['PromotionDiscountCurrencyCode'] = elt.findtext('.//PromotionDiscount/CurrencyCode')
            row['IsGift'] = elt.findtext('IsGift')
            row['ASIN'] = elt.findtext('ASIN')
            row['SellerSKU'] = elt.findtext('SellerSKU')
            row['OrderItemId'] = elt.findtext('OrderItemId')
            row['NumberOfItemsOrdered'] = elt.findtext('.//ProductInfo/NumberOfItems')
            row['QuantityShipped'] = elt.findtext('QuantityShipped')
            row['ItemPriceCurrencyCode'] = elt.findtext('.//ItemPrice/CurrencyCode')
            row['ItemPriceAmount'] = elt.findtext('.//ItemPrice/Amount')
            row['ItemTaxAmount'] = elt.findtext('.//ItemTax/Amount')
            row['ItemTaxCurrencyCode'] = elt.findtext('.//ItemTax/CurrencyCode')
            row_s = pd.Series(row)    
        df = df.append(row_s, ignore_index=True)
    df.to_csv(dest_file_name, index=False)    


# In[25]:


client_details = getclient_details('Kopari Beauty')
project_id = client_details.project_id
client_entitilements = getclient_entitilements(client_details.client_id)
cloud_storage_dir = client_entitilements.cloud_storage_dir
access_key = 'AKIAJUWRQBUUD5QWDOFQ' #replace with your access key
merchant_id = client_entitilements.seller_id
secret_key = '913IqapBjkEV5+vSwtrsJHOYEl1ROH92h5+MPZKf'
auth_token = client_entitilements.mws_auth_token
marketplaceId = client_entitilements.marketplace_id_com
dataset_id = client_entitilements.dataset_id
lastupdatedate = getlastupdateddate(dataset_id, 'amazonmws_orders').max_updated_dt
filepath = app_path + filesep + cloud_storage_dir + filesep + 'amazonmws'
orderspath = filepath + filesep + 'orders' + filesep + 'inbox'
orderitemsspath = filepath + filesep + 'orderitems' + filesep + 'inbox'
gsorderspath = gspath + gssep + cloud_storage_dir + gssep + 'amazonmws' + gssep + 'orders' + gssep + 'inbox'
gsorderitemsspath = gspath + gssep + cloud_storage_dir + gssep + 'amazonmws' + gssep + 'orderitems' + gssep + 'inbox'
print(gsorderspath)
print(gsorderitemsspath)


# In[26]:


end_date = datetime.datetime.now().replace(hour=0, minute=0, second=0,microsecond=0)
start_date = lastupdatedate.isoformat()
start_date_str = lastupdatedate.strftime('%Y%m%d')
end_date_str = end_date.strftime('%Y%m%d')
ordersdestfilename = orderspath + filesep + 'orders_' + start_date_str + '.csv'
orderitemssdestfilename = orderitemsspath + filesep + 'orderitems_' + start_date_str + '.csv'
listorders(start_date,end_date,orderspath,start_date_str,end_date_str)
orderids = extractorderids(orderspath)
listorderitems(orderids,orderitemsspath)
convertorderxmltocsv(orderspath,ordersdestfilename)
convertorderitemsxmltocsv(orderitemsspath,orderitemssdestfilename)
loadlocalfiletogooglestorage(batfile, ordersdestfilename, gsorderspath)
loadlocalfiletogooglestorage(batfile, orderitemssdestfilename, gsorderitemsspath)

