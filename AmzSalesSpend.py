import pandas as pd
import numpy as np
import os
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('/content/drive/MyDrive/Projects/GoogleSheet/emerald-cab-384306-b8566336d0b0.json', scope)
client = gspread.authorize(creds)
gsKyari= client.open('Kyari Advertisment SP,SD&SB(July-Sep2023)')
kyarisheet=gsKyari.worksheet('SP&SD')
kyari_all_record=kyarisheet.get_all_records()
kyari_spend=pd.DataFrame(kyari_all_record)
gsb2c= client.open('AllPortalsales')
kyarib2csheet=gsb2c.worksheet('B2C-Sales')
b2c_all_record=kyarib2csheet.get_all_records()
b2c_df=pd.DataFrame(b2c_all_record)
gsSKU= client.open('Select._Master_Data_2023')
SKUsheet=gsSKU.worksheet('sku_parent')
SKU_all_record=SKUsheet.get_all_records()
SKU_df=pd.DataFrame(SKU_all_record)
kyarigrp=kyari_spend.groupby(['Date', 'Month', "ItemName ",'ASIN','Parent SKU', 'Child SKU'])[['Impressions', 'Clicks', 'Spend', 'Units','Ads Sales']].sum().reset_index().copy()
b2csorteddf=b2c_df.loc[(b2c_df['Brand']=='KYARI')&(b2c_df['Portal']!='Shopify')&(b2c_df['Portal']!='Flipkart')].copy()
b2cgrp=b2csorteddf.groupby(['Date','Month', 'ASIN', 'Parent SKU','Child SKU',"ItemName "])[['Units','Net_Sales','total_sales']].sum().reset_index().copy()
b2cgrp["Date"]=pd.to_datetime(b2cgrp["Date"],format="%d/%m/%Y").dt.date
kyarispendwithb2c=kyarigrp.merge(b2cgrp,left_on=['Date','Month',"ASIN","Parent SKU","Child SKU","ItemName "],right_on=['Date','Month',"ASIN","Parent SKU","Child SKU","ItemName "],how='outer').copy()
kyarispendwithb2c.rename({"Units_x":"Ad Unit","Units_y":"Total Units","Net_Sales":"Net Sales","total_sales":"Total Sales","Child SKU_x":"Child SKU"},axis=1,inplace=True)
b2cgrp['Net_Sales'] = b2cgrp['Net_Sales'].replace('', np.nan)
b2cgrp['total_sales'] = b2cgrp['total_sales'].replace('', np.nan)
b2cgrp['Units'] = b2cgrp['Units'].replace('', np.nan)
b2cgrp['Units'] = b2cgrp['Units'].fillna(0).astype(int)
b2cgrp['Net_Sales'] = b2cgrp['Net_Sales'].astype(float)
b2cgrp['total_sales'] = b2cgrp['total_sales'].astype(float)
finalsorted=kyarispendwithb2c[['Date', 'Month', 'ItemName ', 'ASIN', 'Parent SKU', 'Child SKU',
       'Impressions', 'Clicks', 'Spend', 'Ad Unit', 'Ads Sales',
       'Total Units', 'Net Sales', 'Total Sales']].copy()
sorted_cat=SKU_df[["Child SKU","Portfolio","Parent Category","Child Category","Material","Packs","Shape","Colour"]].copy()
final_b2c=finalsorted.merge(sorted_cat,on=['Child SKU'],how='left').drop_duplicates(ignore_index=True)
final_b2c["Date"]=pd.to_datetime(final_b2c["Date"])
final_b2c['Week Number']="W-"+final_b2c["Date"].dt.strftime("%U")
final_b2c["Date"]=final_b2c["Date"].dt.date
gsB2c = client.open('Amz_Sales&SpendReport2023')
sheetb2c=gsB2c.worksheet('RA-SDSP')
sheetb2c.clear()
set_with_dataframe(sheetb2c,final_b2c)
kyariParentgrp=kyarigrp.groupby(['Date',"Month","Parent SKU","Child SKU"])[['Impressions','Clicks','Spend','Units','Ads Sales']].sum().reset_index().copy()
kyarisbsheet=gsKyari.worksheet('SB')
sb_all_record=kyarisbsheet.get_all_records()
sb_df=pd.DataFrame(sb_all_record)
sbsort_df=sb_df[['Date', 'Month',"Parent SKU", 'Child SKU', 'Impressions', 'Clicks', 'Spend',
       'Units', 'Ads Sales']].copy()
concatkyari=pd.concat([sbsort_df,kyariParentgrp],ignore_index=False)
concatpdf=concatkyari.groupby(['Date', 'Month', "Parent SKU",'Child SKU'])[['Impressions', 'Clicks', 'Spend','Units', 'Ads Sales']].sum().reset_index()
b2cgrpP=b2cgrp.groupby(['Date', 'Month','Child SKU'])[['Units', 'Net_Sales', 'total_sales']].sum().reset_index()
b2cparent=concatpdf.merge(b2cgrpP,on=['Date', 'Month','Child SKU'],how='outer').drop_duplicates(ignore_index=True)
b2cparent.rename({"Units_x":"Ad Unit","Units_y":"Total Units","Net_Sales":"Net Sales","total_sales":"Total Sales","Child SKU_x":"Child SKU"},axis=1,inplace=True)
parent_df=SKU_df[['Child SKU',"Portfolio",'Parent Category','Child Category']].drop_duplicates(ignore_index=True)
b2cparentm=b2cparent.merge(parent_df,on=['Child SKU'],how='left').drop_duplicates(ignore_index=True)
b2cparentm["Date"]=pd.to_datetime(b2cparentm["Date"])
b2cparentm['Week Number']="W-"+b2cparentm["Date"].dt.strftime("%U")
b2cparentm["Date"]=b2cparentm["Date"].dt.date
gsamz = client.open('Amz_Sales&SpendReport2023')
sheetAmz =gsamz.worksheet('RA-ALL')
sheetAmz.clear()
set_with_dataframe(sheetAmz,b2cparentm)
