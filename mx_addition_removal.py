import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq
import tqdm
from datetime import timedelta,datetime,date,time
import time

today = (datetime.now()).strftime('%Y-%m-%d')

home_directory = '/home/'
# home_directory = "C:/Users/magicpin/Downloads/"
bq_auth = f'{home_directory}magicpin-analytics-growth-team.json'
gsheet_auth = f'{home_directory}gsheet_creds.json'



sheet_url = "https://docs.google.com/spreadsheets/d/1gLn3VudY25eRqgurHcBKyLpe5asH8atCCd8nRtqtFJc"

credentials_bq = service_account.Credentials.from_service_account_file(bq_auth)

project_id_bq = 'magicpin-14cba'
client_bq = bigquery.Client(credentials=credentials_bq, project=project_id_bq)
def big_qu_df(query):
  rdf = pd.DataFrame(pandas_gbq.read_gbq(query, project_id=project_id_bq,credentials=credentials_bq))
  return rdf

def delete(mid):
    delete_data = """delete from  `magicpin-analytics.locality_frame.quality_merchants_fixed_new` where mid in """ + str(
        tuple(mid)) + """;"""
    query_job = client_bq.query(delete_data)
    return query_job.result()
def func(mid):
    delete(mid)
    print('Deleted MIDs')
    query = """BEGIN INSERT INTO `magicpin-analytics.locality_frame.quality_merchants_fixed_new` 
    SELECT a.*, l.name AS cluster_locality_name, l.mega_city, cash_gmv, null as ownership, 
    null as monthly_sales, null as res_chain_url,NULL as rating FROM ( SELECT m.id AS mid, m.merchant_name AS mx_name, m.locality_id, 
    m.locality, IFNULL(l.cluster_id,l.id) AS cluster_locality_id, m.highlight1, m.highlight3, 
    CASE WHEN CASE WHEN TRIM(z.delivery_review_count) = '' THEN NULL WHEN 
    REGEXP_CONTAINS(z.delivery_review_count, r'[0-9]+K$') THEN SAFE_CAST(REPLACE(REPLACE(z.delivery_review_count, 'K', ''), ',', '') 
    AS FLOAT64) * 1000 ELSE SAFE_CAST(REPLACE(z.delivery_review_count, ',', '') AS FLOAT64) 
    END IS NULL THEN SAFE_CAST(g.review_count AS FLOAT64) ELSE CASE WHEN TRIM(z.delivery_review_count) = '' THEN NULL WHEN 
    REGEXP_CONTAINS(z.delivery_review_count, r'[0-9]+K$') THEN SAFE_CAST(REPLACE(REPLACE(z.delivery_review_count, 'K', ''), ',', '') AS FLOAT64) * 1000 
    ELSE SAFE_CAST(REPLACE(z.delivery_review_count, ',', '') AS FLOAT64) END END AS delivery_review_count, CASE WHEN m.id IN 
    ( SELECT ac.merchant_id_c AS merchant_id FROM wallet_latest.accounts_cstm ac LEFT JOIN wallet_latest.accounts a ON ac.id_c = a.id 
    LEFT JOIN wallet_latest.users u ON a.assigned_user_id = u.id LEFT JOIN wallet_latest.users ua ON ac.user_id_c = ua.id 
    LEFT JOIN wallet_latest.merchant m ON m.id = ac.merchant_id_c LEFT JOIN wallet_latest.category c ON m.category_id = c.id 
    LEFT JOIN wallet_latest.users uc ON ac.user_id1_c = uc.id WHERE a.deleted = 0 AND ua.user_name IN ('hgmv@magicpin.in')) 
    THEN 'mid-market' WHEN IFNULL(m.merchant_priority,0) = 1 THEN 'Brand' ELSE 'Local' END AS mx_type, 
    CAST(REGEXP_EXTRACT(REPLACE(cft, ',', ''), r'(\d+)') AS INT64)  AS cft 
    FROM `magicpin-14cba.wallet_latest.merchant` m LEFT JOIN `magicpin-14cba.wallet_latest.locality` l ON m.locality_id = l.id 
    LEFT JOIN `magicpin-14cba.mysql_mcp.zomato_store` z ON m.id = z.magic_id LEFT JOIN 
    `magicpin-14cba.mysql_mcp.google_maps_store` g ON m.google_place_id = g.id WHERE m.id IN 
    """+str(tuple(mid))+""") AS a 
    LEFT JOIN `magicpin-14cba.wallet_latest.locality` l ON a.cluster_locality_id = l.id 
    LEFT JOIN ( SELECT merchant_id, SUM(CASE WHEN dt.bill_size > 1000000 THEN 1000000 ELSE dt.bill_size END ) 
    AS cash_gmv FROM `magicpin-14cba.wallet_latest.deal_transaction` dt WHERE DATE(created_at) >= 
    CURRENT_DATE('Asia/Kolkata') - 400 AND UPPER(status) IN 
    ('REWARDED', 'FUNDED_GRACE_DISAPPROVE', 'GRACE_DISAPPROVE', 'DISPUTED_DISAPPROVED')
     GROUP BY 1) AS c ON a.mid = c.merchant_id; END"""

    print(query)
    query_job = client_bq.query(query)
    return query_job.result()

def gs_reader(sheet_name,col,sheet_url):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
    gc = gspread.authorize(credentials)
    sht2 = gc.open_by_url(sheet_url)
    sht = sht2.worksheet(sheet_name)
    if col ==0:
        c= get_as_dataframe(sht,evaluate_formulas=True)
        return c
    else:
        c= get_as_dataframe(sht,usecols=col,evaluate_formulas=True)
        return c
def gs_writer(sheet_name,dataframe,sheet_url, resize_bit):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
    gc = gspread.authorize(credentials)
    sht2 = gc.open_by_url(sheet_url)
    sht = sht2.worksheet(sheet_name)
    set_with_dataframe(sht,dataframe,resize = resize_bit)

def upload_to_bq(df):
          pd.set_option("display.max_columns",100)
          project_id_bq = 'magicpin-analytics'
          credentials = service_account.Credentials.from_service_account_file(bq_auth)
          pandas_gbq.to_gbq(df, 'locality_frame.addto_locality_frame',project_id_bq, chunksize=90000,credentials=credentials, if_exists='append')#For First Time replace append 




def owner_update(data):
    data=data.dropna()
    print(data.head())
    data.info()
    data['mid']=data['mid'].fillna(0).astype(int)
    data['status']=data['status'].astype(str)
    data['ownership']=data['ownership'].astype(str)
    data.loc[:,"updated_at"]=pd.to_datetime(today)

    query="""select count(*) as data_count from `magicpin-analytics.locality_frame.addto_locality_frame` where date(updated_at)=current_date;"""
    feed_back=big_qu_df(query)
    feed_back["data_count"]=feed_back["data_count"].astype(int)
    delete_sql="""delete from `magicpin-analytics.locality_frame.addto_locality_frame` where date(updated_at)=current_date;"""
    if feed_back["data_count"].any()>0:
        print(f"Data is already uploaded, deleting {str(today)} data")
        big_qu_df(delete_sql)
        time.sleep(5)
        print("replacing_data")
        upload_to_bq(data)
    else:
        print(f"Uploading data for {str(today)}")
        upload_to_bq(data)

    sql="""UPDATE `magicpin-analytics.locality_frame.quality_merchants_fixed_new` AS qmf
SET qmf.ownership = temp.ownership
FROM (
    SELECT
        mid,
        ownership
    FROM
        `magicpin-analytics.locality_frame.addto_locality_frame`
    WHERE date(updated_at)=(select max(date(updated_at)) from `magicpin-analytics.locality_frame.addto_locality_frame` )
    GROUP BY
        all
) AS temp
WHERE qmf.mid = temp.mid;"""
    print("updating_ownership")
    big_qu_df(sql)


def main():
    def addition():
        data = gs_reader('Addition', 0, sheet_url)[['mid', 'status','ownership']]
        data= data.drop_duplicates(subset="mid",keep='last')
        data_new = data
        data['mid'] = data['mid'].fillna(0).astype(int)
        done = data[(data['mid'] > 0) & (data['status'].str.lower() == 'done')]
        addition = data[(data['mid'] > 0) & (data['status'].str.lower() != 'done')][['mid','ownership']]
        add_mids = addition['mid'].tolist()
        try:
            func(add_mids)
            addition['status'] = 'Done'
            addition_df = pd.DataFrame(addition)
            final_addition_data = pd.concat([done, addition_df], ignore_index=True)
            final_addition_data=final_addition_data[['mid', 'status','ownership']]
            gs_writer('Addition', final_addition_data, sheet_url, True)
        except Exception as e:
            print(e)
        
        try:
            print("updating owner tem table")
            print(data_new.head())
            owner_update(data_new)
        except Exception as e:
            print(e)
        
    def remove():
        print("removing..")
        data = gs_reader('Remove', 0, sheet_url)[['mid','reason','status']]
        print(data.head())
        data['mid'] = data['mid'].fillna(0).astype(int)
        done = data[(data['mid'] > 0) & (data['status'].str.lower() == 'done')]
        addition = data[(data['mid'] > 0) & (data['status'].str.lower() != 'done')][['mid','reason']]
        add_mids = addition['mid'].tolist()
        delete(add_mids)
        addition['status'] = 'Done'
        addition_df = pd.DataFrame(addition)
        final_addition_data = pd.concat([done, addition_df], ignore_index=True)
        gs_writer('Remove',final_addition_data,sheet_url,True)
    try:
        addition()
    except Exception as e:
        print(e)
    try:
        remove()
    except Exception as e:
        print(e)
main()

