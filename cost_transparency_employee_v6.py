from __future__ import print_function
import smtplib
import base64
import pandas as pd
import numpy as np
import pyodbc
import mimetypes
import matplotlib
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
from email.mime.image import MIMEImage
from email import encoders
from collections import OrderedDict
import numpy as np
import random
import pandas as pd
import datetime 
import dateutil.relativedelta

# Database connection information/Email credentials
###use kerberos auth by running kinit on terminal
server = ''
database = ''
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes')
cursor = cnxn.cursor()

mailuser= 'noreply-costtransparancy@irdeto.onmicrosoft.com'
smtpsrv = ""
password = ''

# Employee successfactors information
employee_raw=pd.read_sql("""
            select [User Name], 
            [Employee Number] AS [Irdeto Employee ID], 
            [Preferred Name],
            [Family Name], 
            [ManagerID],
            [Geozone]
            from test_successfactors
            where [Source Date]=(SELECT MAX([Source Date]) FROM test_successfactors);""",cnxn)
employee_raw['Full Name']=employee_raw['Preferred Name']+' '+employee_raw['Family Name']
employee_info=employee_raw.copy()
employee_info['Full Name']=employee_info['Full Name'].str.title()

# List of users to send email to
users=employee_info['User Name'].tolist()

# Define file names for this round
##file names are based on relative dates
##t is for current date and proceeds with t-n
##e.g. t2 = t - 2 = current month - 2 months

#Local Directory - change this to the folder you have all the files in
local_directory = "C:\\Users\\minjun.choi\\Desktop\\Cost_Transparency\\"

#define dates for phone
now = datetime.datetime.today()
#############
###########now = datetime.datetime(2019, 12, 31, hour=4, minute=0, second=0, microsecond=0, tzinfo=None, fold=0)

for i in range(1,13):
     globals()['month_t%s' % i] = now + dateutil.relativedelta.relativedelta(months=-i)

#define dates for Rydoo - last three months
rydoo_months = f"({month_t3.strftime('%m')},{month_t2.strftime('%m')},{month_t1.strftime('%m')})"

#define file for Egencia - file name is last month
egencia_file = 'EGENCIA_'+month_t1.strftime("%m_%Y")+'.xls'

#define months for email inline text message
current_months = f"{month_t3.strftime('%B')}, {month_t2.strftime('%B')} and {month_t1.strftime('%B')}"
current_months_vf = f"{month_t4.strftime('%B')}, {month_t3.strftime('%B')} and {month_t2.strftime('%B')}"
last_month_text=month_t1.strftime("%B")

# Phone data definitions from Excel inputs
#NL phone data
##NL phone data needs correction for DEC round - extra code with additional files will be removed next round
for i in range(2,5):
    globals()['vf%s' % i] = pd.read_excel(local_directory+'NL_'+(now+ dateutil.relativedelta.relativedelta(months=-i)).strftime("%m_%Y")+'.xlsx',skiprows=2)
    globals()['vf%s' % i] = globals()['vf%s' % i].dropna(thresh=20)
    globals()['vf%s' % i]['Month'] = (now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%m")+". "+(now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%B")

vodafone_nl=pd.concat([vf2,vf3,vf4], axis=0, ignore_index=True)
vodafone_nl['Service number'] = vodafone_nl['Service number'].astype('str').str.split(".", n = 1, expand = True)[0].astype('str')

#Request form for reconciliation
vf_RE = pd.read_excel(local_directory+'IRDETO VTR-Services_Template_Uploaded_08-JAN-2020_v5.xlsx',skiprows=5)
vf_RE = vf_RE.rename(columns = {8458991234:'Service number','John':'first Name','Smith':'Surname'})
vf_RE['Full Name RE'] = vf_RE['first Name']+ ' ' + vf_RE['Surname']
vf_RE['Service number'] = vf_RE['Service number'].astype('str')

#Merging to get actual names
vodafone_nl = vodafone_nl.merge(vf_RE[['Service number','Full Name RE']], on='Service number', how='left')
employee_info['Full Name1'] = employee_info['Full Name'].str.lower()
vodafone_nl['Full Name RE'] = vodafone_nl['Full Name RE'].str.lower()
vodafone_nl['Full Name RE'] = vodafone_nl['Full Name RE'].replace('dongle ', '', regex=True)

#Sepecific Error Because of Error in Data Warehouse
vodafone_nl['Full Name RE'] = vodafone_nl['Full Name RE'].replace('daniel martin', 'daniel martin martin', regex=True)

#Merging with Successfactors
vodafone_nl = vodafone_nl.set_index('Full Name RE').join(employee_info.set_index('Full Name1')).reset_index()

#DF
vodafone_nl['User Name'] = vodafone_nl['User Name'].str.replace('com-1','com')
vodafone_nl['Total (ex. Tax)']=vodafone_nl['Total (ex. Tax)'].astype(np.float64)
vodafone_nl['Total costs in EUR']=vodafone_nl['Total (ex. Tax)']
vodafone_display=['User Name',               
         'Month','In Bundle (EUR)',
         'Out of Bundle (EUR)',
         'Total costs in EUR',
         'Invoice Label']
vodafone_nl['Invoice Label'] = vodafone_nl['Invoice Label'].str.title()
vodafone_nl['In Bundle (EUR)'] = np.where(vodafone_nl['Invoice Label'].str.contains("Device Access Fee|Red Bundle Fee"), vodafone_nl['Total costs in EUR'], 0)
vodafone_nl['Out of Bundle (EUR)'] = np.where(vodafone_nl['Invoice Label'].str.contains("Device Access Fee|Red Bundle Fee")==False, vodafone_nl['Total costs in EUR'], 0)
vf_concat = vodafone_nl[['index','User Name','Month','In Bundle (EUR)','Out of Bundle (EUR)','Total costs in EUR','ManagerID','Service number']]

#USA phone data

for i in range(1,4):
    globals()['usa%s' % i] = pd.read_excel(local_directory+'USA_'+(now+ dateutil.relativedelta.relativedelta(months=-i)).strftime("%m_%Y")+'.xlsx',skiprows=2)
    globals()['usa%s' % i]['Month'] = (now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%m")+". "+(now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%B")

usa_total=pd.concat([usa1,usa2,usa3], axis=0, ignore_index=True)

usa_total['\nUser Name']=usa_total['\nUser Name'].str.lower()

usa_total=usa_total.rename(columns={"\nTotal Current Charges": "Total Costs in USD", 
                                    "\nMonthly Access Charges": "Monthly Access Charges", 
                                    "\nTotal Data Usage Charges (Excluding Roaming)": "Total Data Usage Charges (Excluding Roaming)",
                                    '\nTotal Feature Charges': 'Total Feature Charges',
                                    '\nTotal Service Level Equipment Charges':'Total Service Level Equipment Charges',
                                    '\nTotal Service Level Other Charges and Credits':'Total Service Level Other Charges and Credits',
                                    '\nTotal Taxes Surcharges and Regulatory Fees':'Total Taxes Surcharges and Regulatory Fees',
                                    'Total Taxes \nSurcharges and Regulatory Fees':'Total Taxes Surcharges and Regulatory Fees',
                                    '\nTotal LD Charges':'Total LD Charges'})

employee_info2=employee_raw.copy()
employee_info2['Name']=employee_info2['Full Name']
employee_info2['Full Name']=employee_info['Full Name'].str.lower()
usa_total=usa_total.set_index('\nUser Name').join(employee_info2.set_index('Full Name')).reset_index()
#name is different from successfactors we couldn't match them. Like Bryan

usa_display=['User Name',
             'Month',
             'Total Costs in USD',
             'Monthly Access Charges', 
             'Total Data Usage Charges (Excluding Roaming)', 
             'Total Feature Charges', 
             'Total Service Level Equipment Charges',
             'Total Service Level Other Charges and Credits', 
             'Total Taxes Surcharges and Regulatory Fees','Total LD Charges']

usa_phone=usa_total[usa_display].fillna(0)

#INDIA phone data
for i in range(1,4):
    globals()['india%s' % i] = pd.read_excel(local_directory+'IND_'+(now+ dateutil.relativedelta.relativedelta(months=-i)).strftime("%m_%Y")+'.xlsx',header=3)
    globals()['india%s' % i] = globals()['india%s' % i].rename(columns={'Name':'Full Name','Monthaly Plan+18% GST':'Monthly Plan Cost'})
    globals()['india%s' % i]['Total Billing Amount'] = globals()['india%s' % i].iloc[ : , 4 ]
    globals()['india%s' % i]['Month'] = (now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%m")+". "+(now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%B")
india_raw=pd.concat([india1,india2, india3])
india_raw['Total Billing Amount']=india_raw['Total Billing Amount'].astype(np.float64).round(2)
india_phone=india_raw.copy().merge(employee_info[['Full Name','User Name','ManagerID']], on='Full Name', how='left')
india_display=['Full Name','Monthly Plan Cost','Total Billing Amount','Month']



#CANADA phone data
for i in range(1,4):
    globals()['canada_raw_%s' % i] = pd.read_excel(local_directory+'CAN_'+(now+ dateutil.relativedelta.relativedelta(months=-i)).strftime("%m_%Y")+'.xlsx',skiprows=1).round(2)
    globals()['canada_raw_%s' % i]['Full Name']=globals()['canada_raw_%s' % i]['User Name'].str.title()
    globals()['canada_raw_%s' % i]=globals()['canada_raw_%s' % i].drop(columns='User Name')
    globals()['canada_raw_%s' % i]['Month'] = (now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%m")+". "+(now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%B")

canada_phone=pd.concat([canada_raw_1,canada_raw_2,canada_raw_3], axis=0, ignore_index=True)
canada_phone=canada_phone.copy().merge(employee_info[['Full Name','User Name','ManagerID']], on='Full Name', how='left')
#canada_phone['Total Charges (with tax) (CAD)']=canada_phone['Total Charges (with tax)']
#canada_phone = canada_phone.rename(columns={"Monthly Service Fee": "In Bundle", "Out of bundle cost":"Out of Bundle","Total":"Total costs in CAD"})
canada_display=['User Name',
                'In Bundle (CAD)',
                'Out of Bundle (CAD)',
                'Tax',
                'Total costs in CAD',
                'Month']
canada_phone['Out of Bundle (CAD)'] = canada_phone.apply(lambda row: row['Total Current Charges Taxable'] - row['Monthly Service Fee'] , axis=1)
canada_phone['In Bundle (CAD)'] = canada_phone['Monthly Service Fee']
canada_phone['Tax'] = canada_phone['HST']
canada_phone['Total costs in CAD'] = canada_phone.apply(lambda row: row['Out of Bundle (CAD)'] + row['In Bundle (CAD)'] + row['Tax'], axis=1)


#UK phone data
for i in range(2,5):
    globals()['uk_raw_%s' % i] = pd.read_excel(local_directory+'UK_'+(now+ dateutil.relativedelta.relativedelta(months=-i)).strftime("%m_%Y")+'.xlsx',header=5)
    globals()['uk_raw_%s' % i]=globals()['uk_raw_%s' % i].fillna(0)
    globals()['uk_raw_%s' % i]['Month'] = (now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%m")+". "+(now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%B")

uk_raw=pd.concat([uk_raw_2,uk_raw_3,uk_raw_4], axis=0,ignore_index=True)
uk_phone=uk_raw.copy()

uk_phone=uk_phone.rename(columns={'Total charges (£)':'Total costs in GBP'})
uk_phone['In Bundle (GBP)'] = uk_phone.apply(lambda row: row['Recurring charges (£)'] + row['Credits (£)'], axis=1)
uk_phone['Out of Bundle (GBP)'] = uk_phone.apply(lambda row: row['Other charges (£)'] + row['Usage charges (£)'], axis=1)
uk_phone['User name'] = uk_phone['User name'].replace('REF: WILL LAWTON','REF: WILLIAM LAWTON',regex=True)
uk_phone['User name'] = uk_phone['User name'].replace('REF: JAVID KAHN','REF: JAVID KAZIM KHAN',regex=True)
uk_phone['User name'] = uk_phone['User name'].replace('REF: JAVID KHAN','REF: JAVID KAZIM KHAN',regex=True)
uk_phone['User name'] = uk_phone['User name'].replace('REF: IT STOCK','REF: Diogenes Cruz de Arcelino',regex=True)
uk_phone['Full Name1'] = uk_phone['User name'].replace("REF: ","",regex=True).str.lower()
uk_phone = uk_phone.merge(employee_info[['Full Name1','User Name','ManagerID']], on='Full Name1', how='left')
uk_phone = uk_phone[['Month','Phone number','User Name','In Bundle (GBP)','Out of Bundle (GBP)','Total costs in GBP']]

#FRANCE phone data
for i in range(1,4):
    globals()['france_raw_%s' % i] = pd.read_excel(local_directory+'FRA_'+(now+ dateutil.relativedelta.relativedelta(months=-i)).strftime("%m_%Y")+'.xlsx')
    globals()['france_raw_%s' % i]['Month'] = (now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%m")+". "+(now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%B")
france_raw=pd.concat([france_raw_1,france_raw_2,france_raw_3], axis=0,ignore_index=True)

france_raw=france_raw.rename(columns={"Nom de l'utilisateur": "Full Name", "Abronnemement en cours": "Subscription Package", "Total EUR HT": "Total costs in EUR","Ligne":"Mobile Number"})
france_raw['Total costs in EUR']=france_raw['Total costs in EUR'].replace({',':'.'}, regex=True)
france_raw['Total costs in EUR']=france_raw['Total costs in EUR'].replace({'€':''}, regex=True)
france_raw['Total costs in EUR']=france_raw['Total costs in EUR'].astype(float)
france_raw['Abos et forfaits']=france_raw['Abos et forfaits'].replace({',':'.'}, regex=True)
france_raw['Abos et forfaits']=france_raw['Abos et forfaits'].replace({'€':''}, regex=True)
france_raw['Abos et forfaits']=france_raw['Abos et forfaits'].astype(float)
france_raw['Total Remises']=france_raw['Total Remises'].replace({',':'.'}, regex=True)
france_raw['Total Remises']=france_raw['Total Remises'].replace({'€':''}, regex=True)
france_raw['Total Remises']=france_raw['Total Remises'].astype(float)
france_raw['Montant total des consommations']=france_raw['Montant total des consommations'].replace({',':'.'}, regex=True)
france_raw['Montant total des consommations']=france_raw['Montant total des consommations'].replace({'€':''}, regex=True)
france_raw['Montant total des consommations']=france_raw['Montant total des consommations'].astype(float)
france_raw['Services et Options']=france_raw['Services et Options'].replace({',':'.'}, regex=True)
france_raw['Services et Options']=france_raw['Services et Options'].replace({'€':''}, regex=True)
france_raw['Services et Options']=france_raw['Services et Options'].astype(float)
france_raw['Autres prestations']=france_raw['Autres prestations'].replace({',':'.'}, regex=True)
france_raw['Autres prestations']=france_raw['Autres prestations'].replace({'€':''}, regex=True)
france_raw['Autres prestations']=france_raw['Autres prestations'].astype(float)
france_raw['Full Name'] = france_raw['Full Name'].str.replace(" ", '')
france_raw['Full Name'] = france_raw['Full Name'].str.split("_", n = 1, expand = True)[0].str.title()+' '+france_raw['Full Name'].str.split("_", n = 1, expand = True)[1].str.title()

france_phone=france_raw.copy().merge(employee_info[['Full Name','User Name','ManagerID']], on='Full Name', how='left')
france_phone['In Bundle (EUR)'] = france_phone.apply(lambda row: row['Abos et forfaits'] + row['Services et Options'] + row['Total Remises'], axis=1)
france_phone['Out of Bundle (EUR)'] = france_phone.apply(lambda row: row['Montant total des consommations'] + row['Autres prestations'], axis=1)
france_display=['Full Name','Month','In Bundle (EUR)','Out of Bundle (EUR)','Total costs in EUR','User Name']
france_display1=['Full Name','Month','In Bundle (EUR)','Out of Bundle (EUR)','Total costs in EUR','User Name','Mobile Number']
france_phone['TF'] = france_phone['Full Name'].str.contains('Irdeto')

france_phone['User name'] = france_phone.apply(lambda row: 'thalia.zafeiropoulou@irdeto.com' if row['TF'] is True else row['User Name'],axis=1)
france_phone = france_phone.drop(columns = 'User Name')
france_phone = france_phone.rename(columns = {'User name':'User Name'})
france_phone['Managerid'] = france_phone.apply(lambda row: 'a' if row['TF'] is True else row['ManagerID'],axis=1)
france_phone = france_phone.drop(columns = 'ManagerID')
france_phone = france_phone.rename(columns = {'Managerid':'ManagerID'})

#SOUTH AFRICA phone data
for i in range(1,4):
    globals()['sa_raw%s' % i] = pd.read_excel(local_directory+'SA_'+(now+ dateutil.relativedelta.relativedelta(months=-i)).strftime("%m_%Y")+'.xlsx', header=4)
    globals()['sa_raw%s' % i]['Month'] = (now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%m")+". "+(now + dateutil.relativedelta.relativedelta(months=-i)).strftime("%B")

sa_raw=pd.concat([sa_raw1,sa_raw2,sa_raw3],axis=0,ignore_index=True)

sa_raw['Full Name']=sa_raw['Name']+' '+sa_raw['Surname']
sa_raw.drop(sa_raw.tail(2).index,inplace=True)
sa_raw = sa_raw.fillna(0)
sa_raw = sa_raw.drop(columns='Unnamed: 9')
sa_raw['Total Incl VAT']=sa_raw['Total Incl VAT'].astype(float).round(2)
sa_phone=sa_raw.copy().merge(employee_info[['Full Name','User Name','ManagerID']], on='Full Name', how='left')

sa_display = ['Month',
              'Full Name',
              'Service Provider',
              'Basic Package Price', 
              'Data Booster Bundle', 
              'Service fees',
              'Additional Data Costs', 
              'Additional Call Costs - Intnl calls',
              'Handset Fee', 
              'Total - Excluding VAT', 
              'VAT', 
              'Total Incl VAT']

sa = sa_phone
sa['In Bundle (ZAR)'] = sa.apply(lambda row: row['Basic Package Price'] + row['Data Booster Bundle'] + row['Service fees'] + row['Handset Fee'], axis=1)
sa['Out of Bundle (ZAR)'] = sa.apply(lambda row: row['Additional Data Costs'] + row['Additional Call Costs / Intnl calls'], axis=1)
sa = sa.rename(columns={'Total Incl VAT':'Total Costs in ZAR','VAT':'Tax'})
sa['Region'] = 'South Africa'
sa['Currency'] = 'ZAR'
sa = sa[['Month','User Name','In Bundle (ZAR)','Out of Bundle (ZAR)','Tax','Total Costs in ZAR','Region','Currency']]


# Egencia excel file definition
egencia_raw=pd.read_excel(local_directory+egencia_file,lines=True)
egencia_raw=egencia_raw[:-2]
egencia_raw['Transaction Month'] = egencia_raw['Transaction month'].astype(np.int64)
egencia_raw['Transaction Month'] = egencia_raw['Transaction Month'].replace({1:'01. January',2:'02. Feburary',3:'03. March',4:'04. April',5:'05. May',6:'06. June',7:'07. July',8:'08. August',9:'09. September',10:'10. October', 11:'11. November', 12:'12. December'})
egencia_raw['Transaction date/time']=pd.to_datetime(egencia_raw['Transaction date/time'], errors='coerce')
egencia_raw['User Name']=egencia_raw['Traveler email address']
egencia_raw=egencia_raw.merge(employee_info[['User Name','ManagerID']], on='User Name', how='left')
egencia_raw=egencia_raw.drop_duplicates()


# Sending email loop for all employees
for user in users:
    uid=employee_info[employee_info['User Name']==user]['Irdeto Employee ID'].values[0]
    index = employee_info[employee_info['User Name']==user].index.values
    print(user,uid,index)
     
    # Opening banner and message
    html="<img src='banner2.png' alt='' style='width: 100%'>"
    text="\n\Irdeto"
    html=html+'<br/><br/>Dear '+ str(employee_info[employee_info['User Name']==user]['Preferred Name'].values[0]) +','
    text=text+'\n\n\Dear '+ str(employee_info[employee_info['User Name']==user]['Preferred Name'].values[0]) +','
    html=html+'<br/><br/> Below you can find the total expense and travel costs breakdown for '+ current_months+' (except for Vodafone NL and UK, which is for '+current_months_vf+').'
    text=text+'\n\n\ Below you can find the total expense and travel costs breakdown for '+ current_months+' (except for Vodafone NL and UK, which is for '+current_months_vf+').'
 
    # Phone data
    #No Table for empty data for phone bill
    if vf_concat[vf_concat['User Name']==user].empty==True\
    and usa_phone[usa_phone['User Name']==user].empty==True\
    and canada_phone[canada_phone['User Name']==user].empty==True\
    and france_phone[france_phone['User Name']==user].empty==True\
    and uk_phone[uk_phone['User Name']==user].empty==True\
    and india_phone[india_phone['User Name']==user].empty==True\
    and sa_phone[sa_phone['User Name']==user].empty==True:
        html=html+'<br/><br/><b>Your phone data is not available yet. Please check with your provider for more details.</b>'
        text=text+'\n\Your phone data is not available yet. Please check with your provider for more details.\n'
    else:
        if vf_concat[vf_concat['User Name']==user].empty==False:
            vf_bill_indv=vf_concat[vf_concat['User Name']==user].groupby(['Month']).sum().round(2)
            vf_bill_indv['Status']='Your Costs'

            vf_average=vf_concat[vf_concat['User Name'].isin(employee_info[employee_info['ManagerID']==employee_info[employee_info['User Name']==user]['ManagerID'].values[0]]['User Name'])].groupby(['Month','User Name']).sum().groupby('Month').mean().round(2)
            vf_average['Status']='Your Peer Team Average'

            vf_table = pd.concat([vf_bill_indv,vf_average])
            vf_table = vf_table.pivot_table(vf_table, index= ['Month','Status']).reindex(columns=['In Bundle (EUR)','Out of Bundle (EUR)','Total costs in EUR'])
            vf_table = vf_table.T

            #Vodafone NL table
            html=html+'<br/><br/>Your phone ( ' +str(vf_concat[vf_concat['User Name']==user]['Service number'].values[0])+ ' ) costs for the past months (EUR) :<br/><br/>'
            html=html+vf_table.to_html().replace('<tr>','<tr style="text-align: right;">')
            text=text+'<br/><br/>Your phone ( ' +str(vf_concat[vf_concat['User Name']==user]['Service number'].values[0])+ ' ) costs for the past months (EUR) :<br/><br/>'
            text=text+vf_table.to_string().replace('<tr>','<tr style="text-align: right;">')
        else:
            pass
        if canada_phone[canada_phone['User Name']==user].empty==False:
            canada_bill=canada_phone[canada_phone['User Name']==user][canada_display].round(2)
            canada_bill['Type']='Your Costs'
            canada_average=canada_phone[canada_phone['User Name'].isin(employee_info[employee_info['ManagerID']==employee_info[employee_info['User Name']==user]['ManagerID'].values[0]]['User Name'])][canada_display].groupby(['User Name','Month']).mean()
            canada_average=canada_average.reset_index()
            canada_average['Type']='Your Peer Team Average'
            canada_table = pd.concat([canada_bill,canada_average], axis=0, join='inner', ignore_index=False, keys=None, levels=None, names=None, copy=False)
            canada_table = canada_table.pivot_table(canada_table, index=['Month','Type']).reindex(columns=['In Bundle (CAD)','Out of Bundle (CAD)','Tax','Total costs in CAD']).T.round(2).fillna(0)
            html=html+'<br/><br/>Your phone ( '+str(canada_phone[canada_phone['User Name']==user]['User Number'].values[0])+' ) costs for the past months (CAD):<br/><br/>'+canada_table.to_html().replace('<tr>','<tr style="text-align: right;">')
            text=text+'\n\n\Your phone ( '+str(canada_phone[canada_phone['User Name']==user]['User Number'].values[0])+' ) costs for the past months (CAD):<br/><br/>'+canada_table.to_string().replace('<tr>','<tr style="text-align: right;">')
        else:
            pass
        if france_phone[france_phone['User Name']==user].empty==False:
            if user == 'thalia.zafeiropoulou@irdeto.com':
                france_table = france_phone[france_phone['User Name']==user][france_display1].groupby(['Month','Mobile Number']).sum().round(2)             
                html=html+'<br/><br/>Your phone ( 769276034 ) costs for the past months (EUR):<br/><br/>'+france_table.to_html().replace('<tr>','<tr style="text-align: right;">')
                text=text+'\n\n\Your phone ( 769276034 ) costs for the past months (EUR):<br/><br/>'+france_table.to_string().replace('<tr>','<tr style="text-align: right;">')

            else:
                france_bill=france_phone[france_phone['User Name']==user][france_display].groupby('Month').sum().round(2)
                france_bill['Type']='Your Costs'
                france_average = france_phone[france_phone['User Name'].isin(employee_info[employee_info['ManagerID']==employee_info[employee_info['User Name']==user]['ManagerID'].values[0]]['User Name'])][france_display].groupby('Month').mean().round(2)
                france_average['Type'] = 'Your Peer Team Average'
                france_table = pd.concat([france_bill,france_average]) 
                france_table = france_table.pivot_table(france_table, index=['Month','Type']).T.round(2)
                html=html+'<br/><br/>Your phone ( '+str(france_phone[france_phone['User Name']==user]['Mobile Number'].values[0])+' ) costs for the past months (EUR):<br/><br/>'+france_table.to_html().replace('<tr>','<tr style="text-align: right;">')
                text=text+'\n\n\Your phone ( '+str(france_phone[france_phone['User Name']==user]['Mobile Number'].values[0])+' ) costs for the past months (EUR):<br/><br/>'+france_table.to_string().replace('<tr>','<tr style="text-align: right;">')
        else:
            pass
        if uk_phone[uk_phone['User Name']==user].empty==False:
            uk_bill=uk_phone[uk_phone['User Name']==user].round(2)
            uk_bill['Type']='Your Costs'
            uk_average=uk_phone[uk_phone['User Name'].isin(employee_info[employee_info['ManagerID']==employee_info[employee_info['User Name']==user]['ManagerID'].values[0]]['User Name'])].groupby('Month').mean().reset_index()
            uk_average['Type']='Your Peer Team Average'    
            uk_table = pd.concat([uk_bill,uk_average], axis=0, join='outer', ignore_index=False, keys=None, levels=None, names=None, copy=False)
            uk_table = uk_table.pivot_table(uk_table, index=['Month','Type']).reindex(columns=['In Bundle (GBP)','Out of Bundle (GBP)','Total costs in GBP']).T
            html=html+'<br/><br/>Your phone ( '+str(uk_phone[uk_phone['User Name']==user]['Phone number'].values[0])+' ) costs for the past months (GBP):<br/><br/>'+uk_table.to_html().replace('<tr>','<tr style="text-align: right;">')
            text=text+'\n\n\our phone ( '+str(uk_phone[uk_phone['User Name']==user]['Phone number'].values[0])+' ) costs for the past months (GBP):<br/><br/>'+uk_table.to_string().replace('<tr>','<tr style="text-align: right;">')
        else:
            pass
        if sa[sa['User Name']==user].empty==False:
            sa_bill_indv=sa[sa['User Name']==user].groupby('Month').sum().round(2)
            sa_bill_indv[' ']='Your Costs'
            sa_average=sa[sa['User Name'].isin(employee_info[employee_info['ManagerID']==employee_info[employee_info['User Name']==user]['ManagerID'].values[0]]['User Name'])].groupby('Month').mean().round(2)
            sa_average[' ']='Your Team Average'
            sa_table = pd.concat([sa_bill_indv,sa_average], axis=0, join='outer', ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, copy=True)
            sa_table = sa_table.reset_index().sort_values(['Month',' '], ascending=True).set_index(['Month',' ']).T.round(2)
            html=html+'<br/><br/>Your phone costs for the past months (ZAR):<br/><br/>'+sa_table.to_html().replace('<tr>','<tr style="text-align: right;">')
            text=text+'\n\n\Your phone costs for the past months (ZAR):<br/><br/>'+sa_table.to_string().replace('<tr>','<tr style="text-align: right;">')
        else:
            pass
        if usa_phone[usa_phone['User Name']==user].empty==False:
            usa_bill=usa_phone[usa_phone['User Name']==user].groupby('Month').sum().round(2)
            usa_bill['Type']='Your Costs'
            usa_average=usa_phone[usa_phone['User Name'].isin(employee_info[employee_info['ManagerID']==employee_info[employee_info['User Name']==user]['ManagerID'].values[0]]['User Name'])].groupby('Month').mean().round(2)
            usa_average['Type']='Your Peer Team Average'      
            usa_table = pd.concat([usa_bill,usa_average], axis=0, join='outer', ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, copy=True)
            usa_table = usa_table.pivot_table(usa_table, index= ['Month','Type']).reindex(columns=['Monthly Access Charges', 'Total Data Usage Charges (Excluding Roaming)', 'Total Feature Charges', 'Total Service Level Equipment Charges','Total Service Level Other Charges and Credits','Total LD Charges', 'Total Taxes Surcharges and Regulatory Fees','Total Costs in USD'])
            usa_table = usa_table.loc[:, (usa_bill != 0).any(axis=0)]
            usa_table = usa_table.reset_index().sort_values(['Month','Type'], ascending=[True, True]).set_index(['Month','Type']).T
            html=html+'<br/><br/>Your phone ( '+str(usa_total[usa_total['User Name']==user]['\nWireless Number'].values[0])+' ) costs for the past months (USD):<br/><br/>'+usa_table.to_html().replace('<tr>','<tr style="text-align: right;">')
            text=text+'\n\n\Your phone ( '+str(usa_total[usa_total['User Name']==user]['\nWireless Number'].values[0])+' ) costs for the past months (USD):<br/><br/>'+usa_table.to_string().replace('<tr>','<tr style="text-align: right;">')
        else:
            pass
        if india_phone[india_phone['User Name']==user].empty==False:
            india_bill=india_phone[india_phone['User Name']==user][india_display].round(2).groupby('Month').sum().round(2)
            india_bill['Type']='1. Your Costs'
            india_average=india_phone[india_phone['User Name'].isin(employee_info[employee_info['ManagerID']==employee_info[employee_info['User Name']==user]['ManagerID'].values[0]]['User Name'])][india_display].groupby('Month').mean().round(2)
            india_average['Type']='2. Your Peer Team Average'
            india_table = pd.concat([india_bill,india_average], axis=0, join='inner', ignore_index=False, keys=None, levels=None, names=None, copy=False)
            india_table = india_table.pivot_table(india_table, index=['Month','Type']).T.round(2)
            html=html+'<br/><br/>Your phone ( '+str(india_phone[india_phone['User Name']==user]['Mobile No.'].values[0][13:])+' ) costs for the past months (INR):<br/><br/>'+india_table.to_html().replace('<tr>','<tr style="text-align: right;">')
            text=text+'\n\n\Your phone ( '+str(india_phone[india_phone['User Name']==user]['Mobile No.'].values[0][13:])+' ) costs for the past months (INR):<br/><br/>'+india_table.to_string().replace('<tr>','<tr style="text-align: right;">')
        else:
            pass
            
    # Rydoo expenses
    #indv rydoo expenses creation
    rydoo_expenses_raw=pd.read_sql("""
            select distinct tcs.[XPDREFERENCE],
                   bri.[AMOUNTMST] AS [Your Expenses (USD)],
                   sf.[User Name] As [Email Address],
                   DATEPART(month, date_table.ENDDATE) Month,
                   bri.[LEDGERACCOUNTDESCR] as [Category]
            from test_successfactors sf
                left join DATASTORE.DSL.TCS_XPENDITURE_XMLDATA tcs on sf.[User Name]=tcs.EMAIL
                left join DW_IRDETO_DM.BRI.DAX_Transactions_Source_Extended bri on tcs.XPDREFERENCE=bri.DOCUMENTNUM
                left join DSL.LEDGERTRANS ledger on bri.DOCUMENTNUM=ledger.DOCUMENTNUM
                left join DATASTORE.DSL.TCS_XPENDITURE_IMPORTLOG date_table on tcs.IMPORTID=date_table.IMPORTID
            where sf.[User Name]='"""+user+"""'
                and DATEPART(month, date_table.ENDDATE) in """+rydoo_months+"""
                and DATEPART(year, date_table.ENDDATE)='2019'
                and [Source Date]=(SELECT MAX([Source Date]) FROM test_successfactors)
                and bri.LEDGERACCOUNTNUMBER not in ('25001','90005');""",cnxn)
    rydoo_expenses_raw['Month'] = rydoo_expenses_raw['Month'].replace({1:'01. January',2:'02. Feburary',3:'03. March',4:'04. April',5:'05. May',6:'06. June',7:'07. July',8:'08. August',9:'09. September',10:'10. October', 11:'11. November', 12:'12. December'})    
    rydoo_expenses_indv = rydoo_expenses_raw.drop_duplicates('XPDREFERENCE').drop(columns='XPDREFERENCE').groupby(['Month','Category']).sum()
    #rydoo expenses N/A condition
    if rydoo_expenses_indv.empty == True:
        html=html+'<br/><br/><b>There are no Rydoo expenses to report on for this period</b>.'
        text=text+'\n\n\<b>There are no Rydoo expenses to report on for this period\n\.'
    #rydoo expenses in text
    else:
        html=html+'<br/><br/> Your Rydoo expenses reported in the last three months:<br/><br/>'
        text=text+'\n\n\ Your Rydoo expenses reported in the last three months:\n\n'
        #avg rydoo expenses creation
        rydoo_expenses_avg=pd.read_sql("""
            select distinct tcs.[XPDREFERENCE],
                   bri.[AMOUNTMST] AS [Your Peer Team Average (USD)],
                   sf.[User Name] As [Email Address],
                   DATEPART(month, date_table.ENDDATE) Month,
                   bri.[LEDGERACCOUNTDESCR] as [Category]
            from test_successfactors sf
                left join DATASTORE.DSL.TCS_XPENDITURE_XMLDATA tcs on sf.[User Name]=tcs.EMAIL
                left join DW_IRDETO_DM.BRI.DAX_Transactions_Source_Extended bri on tcs.XPDREFERENCE=bri.DOCUMENTNUM
                left join DSL.LEDGERTRANS ledger on bri.DOCUMENTNUM=ledger.DOCUMENTNUM
                left join DATASTORE.DSL.TCS_XPENDITURE_IMPORTLOG date_table on tcs.IMPORTID=date_table.IMPORTID
            where sf.ManagerID='"""+employee_info[employee_info['User Name']==user]['ManagerID'].values[0]+"""'
                and DATEPART(month, date_table.ENDDATE) in """+rydoo_months+"""
                and DATEPART(year, date_table.ENDDATE)='2019'
                and [Source Date]=(SELECT MAX([Source Date]) FROM test_successfactors)
                and bri.LEDGERACCOUNTNUMBER not in ('25001','90005');""",cnxn)          
        rydoo_expenses_avg['Month'] = rydoo_expenses_avg['Month'].replace({1:'01. January',2:'02. Feburary',3:'03. March',4:'04. April',5:'05. May',6:'06. June',7:'07. July',8:'08. August',9:'09. September',10:'10. October', 11:'11. November', 12:'12. December'})
        rydoo_expenses_avg = rydoo_expenses_avg.drop_duplicates('XPDREFERENCE').drop(columns='XPDREFERENCE')
        rydoo_expenses_avg = rydoo_expenses_avg.groupby(['Email Address','Month','Category']).sum()
        rydoo_expenses_avg = rydoo_expenses_avg.groupby(['Month','Category']).mean().round(2)
        
        #rydoo expenses table creation
        rydoo_table = rydoo_expenses_indv.join(rydoo_expenses_avg, how='outer').fillna(0).round(2)
        index = rydoo_table.reset_index()[rydoo_table.reset_index()['Month'].isin(rydoo_table.reset_index().groupby('Month').sum()[rydoo_table.reset_index().groupby('Month').sum()['Your Expenses (USD)'] == 0].reset_index()['Month'].tolist())].index
        rydoo_table.reset_index().drop(index)
        rydoo_table = rydoo_table.reset_index().drop(index).groupby(['Month','Category']).sum()
        html=html+rydoo_table.to_html().replace('<tr>','<tr style="text-align: right;">')+'<br/>Please use this '+'<a href="https://expense.rydoo.com/personal/expenses">link</a>'+' for a detailed overview of your personal expenses in Rydoo.'
        text=text+rydoo_table.to_string().replace('<tr>','<tr style="text-align: right;">')+'<br/>Please use this '+'<a href="https://expense.rydoo.com/personal/expenses">link</a>'+' for a detailed overview of your personal expenses in Rydoo.'

    # Egencia bill
    if egencia_raw[egencia_raw['Traveler email address']==user].groupby(['Line of business','Transaction Month','Traveler email address']).sum()['Transaction amount ($)'].groupby(['Line of business','Transaction Month']).sum().reset_index().pivot(index='Line of business',columns='Transaction Month').fillna(0).empty == True:
        html=html+ '<br/><br/><b>There are no Egencia expenses to report on for this period</b>.'
        text=text+ '\n\n\<b>There are no Egencia expenses to report on for this period</b>.'
    else:
        #remove months with indv total = 0
        egencia_new=egencia_raw[egencia_raw['Traveler email address']==user][['Line of business','Transaction Month','Transaction amount ($)']]
        egencia_new=egencia_new.groupby(['Line of business','Transaction Month']).sum()
        egencia_new=egencia_new.rename(columns={"Transaction amount ($)": "1. Your Costs (USD)"})
        #new avg egencia
        egencia_avg_new=egencia_raw[egencia_raw['Traveler email address'].isin(employee_info[employee_info['ManagerID']==employee_info[employee_info['User Name']==user]['ManagerID'].values[0]]['User Name'])]
        egencia_avg_new=egencia_avg_new[['Line of business','Transaction Month','Transaction amount ($)']]
        egencia_avg_new=egencia_avg_new.groupby(['Line of business','Transaction Month']).sum()
        egencia_avg_new=egencia_avg_new.rename(columns={"Transaction amount ($)": "2. Your Peer Team Average (USD)"})
        #new merged egencia
        egencia_table_new=egencia_new.join(egencia_avg_new, how='outer').fillna(0).round(2)
        egencia_index = egencia_table_new.reset_index()[egencia_table_new.reset_index()['Transaction Month'].isin(egencia_table_new.reset_index().groupby('Transaction Month').sum()[egencia_table_new.reset_index().groupby('Transaction Month').sum()['1. Your Costs (USD)'] == 0].reset_index()['Transaction Month'].tolist())].index
        egencia_table = egencia_table_new.reset_index().drop(egencia_index).groupby(['Transaction Month','Line of business']).sum()
        html=html+'<br/><br/>Your Egencia expenses for the past three month:<br/><br/>'+egencia_table.to_html().replace('<tr>','<tr style="text-align: right;">')
        text=text+'\n\Your Egencia expenses for the past three month:<br/><br/>'+egencia_table.to_string().replace('<tr>','<tr style="text-align: right;">')
        html=html+'<br/>Please refer to your personal Egencia portal for a detailed overview of all your transactions.'
        text=text+'\n\Please refer to your personal Egencia portal for a detailed overview of all your transactions.'
    
    # Ending messageex
    ##travel policy links and explanation
    Expense_link='<a href="https://irdeto.sharepoint.com/sites/c_pp/BMS%20Documentation/Expense%20Management.pdf?csf=1&e=XlfhFQ&cid=14517d17-dee9-4249-8b47-761a4fcd0186">Expense Management document. </a>'
    Travel_link='<a href="https://irdeto.sharepoint.com/sites/c_pp/BMS%20Documentation/Travel%20Policy.pdf?csf=1&e=kjG03f&cid=889d64b4-5a63-4d93-a7fd-f301f9a75f85">Travel Policy document. </a>'
    html=html+'<br/><br/>For the peer team average amounts by category for Rydoo and Egencia, the value is calculated by taking the sum of all transactions for that category divided by the number of people making a transaction in that category. However, as each category may include a different number of people spending in that category, you cannot add up all the peer team average amounts to arrive at a total average spending per person.<br/><br/>'
    text=text+'\n\nFor the peer team average amounts by category for Rydoo and Egencia, the value is calculated by taking the sum of all transactions for that category divided by the number of people making a transaction in that category. However, as each category may include a different number of people spending in that category, you cannot add up all the peer team average amounts to arrive at a total average spending per person.\n\n'
    html=html+'If you would like more information regarding the expense claims procedure, please refer to Section 3 of our '+Expense_link+'For information on travel-related expenses, please refer to our '+Travel_link
    text=text+'If you would like more information regarding the expense claims procedure, please refer to Section 3 of our '+Expense_link+'For information on travel-related expenses, please refer to our '+Travel_link
    ##final text messages
    html=html+'<br/><br/>This is an automatically generated email. It is not possible to directly reply to this email.'
    text=text+'\n\n\This is an automatically generated email. It is not possible to directly reply to this email.'
    html=html+'<br/>If you have any questions related to your phone, Rydoo or Egencia expenses, please contact your manager. If you have any comments or suggestions for this report, please send an e-mail to costtransparency@irdeto.com<br/><br/>Best regards,<br/>Technology Team'
    text=text+'\n\If you have any questions related to your phone, Rydoo or Egencia expenses, please contact your manager. If you have any comments or suggestions for this report, please send an e-mail to costtransparency@irdeto.com<br/><br/>Best regards,<br/>Technology Team'
    
    # Don't send email if employee has no data
    if vf_concat[vf_concat['User Name']==user][['Total costs in EUR','Month']].groupby('Month').sum().tail(3)['Total costs in EUR'].empty == True\
    and vf_concat[vf_concat['User Name']==user][['Total costs in EUR','Month']].groupby('Month').sum().tail(2)['Total costs in EUR'].empty == True\
    and vf_concat[vf_concat['User Name']==user][['Total costs in EUR','Month']].groupby('Month').sum().tail(1)['Total costs in EUR'].empty == True\
    and uk_phone[uk_phone['User Name']==user].empty==True\
    and usa_phone[usa_phone['User Name']==user].empty==True\
    and canada_phone[canada_phone['User Name']==user].empty==True\
    and france_phone[france_phone['User Name']==user].empty==True\
    and india_phone[india_phone['User Name']==user].empty == True\
    and sa_phone[sa_phone['User Name']==user].empty==True\
    and rydoo_expenses_indv.empty == True\
    and egencia_raw[egencia_raw['Traveler email address']==user].groupby(['Line of business','Transaction Month','Traveler email address']).sum()['Transaction amount ($)'].groupby(['Line of business','Transaction Month']).sum().reset_index().pivot(index='Line of business',columns='Transaction Month').fillna(0).empty == True:\
        print('No costs for this person.')
    else:
        #sending email
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "Monthly Personal Expenditure Report - "+last_month_text
        msg['From'] = 'noreply-costtransparency@irdeto.com'
        msg['To'] = 'minjun.choi@irdeto.com'
                        #user
        part1 = MIMEText(text, 'plain')
        part2 = MIMEText(html, 'html')
        msg.attach(part1)
        msg.attach(part2)

        #banner addition
        with open(local_directory+'a.png', 'rb') as header:
            # set attachment mime and file name, the image type is png
            mime = MIMEBase('image', 'png', filename='banner2.png')
            # add required header data:
            mime.add_header('Content-Disposition', 'attachment', filename='banner2.png')
            mime.add_header('X-Attachment-Id', '0')
            mime.add_header('Content-ID', '<0>')
            # read attachment file content into the MIMEBase object
            mime.set_payload(header.read())
            # encode with base64
            encoders.encode_base64(mime)
            # add MIMEBase object to MIMEMultipart object
            msg.attach(mime)

        smtpserver = smtplib.SMTP(smtpsrv,587)
        smtpserver.ehlo()
        smtpserver.starttls()
        smtpserver.ehlo
        smtpserver.login(mailuser, password)
        smtpserver.sendmail(msg['From'], msg['To'], msg.as_string())
        print('Email sent!')
        smtpserver.close()

print('End of list.')
