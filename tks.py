import tabula
import os
from tabula.io import read_pdf
import numpy as np
import pandas as pd
import datetime as dt
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tabula.io import read_pdf
import PyPDF2
from log import *

months  =['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember']

def checkTKS(df):
    df.columns=['title']
    df=df.dropna()
    isTKS=df['title'].eq('Tingkat Solvabilitas').any()
    return isTKS

def readpdf_syariah(pdf_file):
    # read PDF file
    tables = read_pdf(pdf_file)
    for table in tables:
        dfcheck=table.iloc[:,:1]
        isTKS=checkTKS(dfcheck)
        if isTKS:
            df= table.iloc[:, [0,2]]
            df.columns = ['title','desc']
            df=df.dropna()
            #parsing data dengan spae
            parse_df = df['desc'].str.split(' ', n=2, expand=True)
            df=df.drop(['desc'], axis=1)
            df['Dana_Tabarru']=parse_df[0]
            df['Dana_Perusahaan']=parse_df[1]
            df = df[df['title'].str.contains('MMBR')] 
            df['jenis']='Syariah'
            print(df)
            break
    return df

def readpdf_konven(pdf_file):
    # read PDF file
    tables = read_pdf(pdf_file)
    print(len(tables))
    idx=0
    for table in tables:
        print(table)
        table.to_csv('file{}.csv'.format(idx))
        idx+=1
        
        
def get_report_date(year,month):
    my_month = months.index(month) + 1
    cons_tanggal ="{}-{:02}".format(year,my_month)
    final_tanggal = datetime.strptime(cons_tanggal, "%Y-%m")
    report_date = final_tanggal - relativedelta(day=31)
    return report_date.date()

def dump_to_db(db_name,table_name,df,report_date):
    from sqlalchemy import create_engine
    from sqlalchemy.exc import SQLAlchemyError
    db = create_engine('sqlite:///{}.db'.format(db_name))
    # drop 'records table'
    try:
        with db.connect() as conn:
            query="DELETE FROM {} WHERE report_date='{}'".format(table_name,report_date)
            r_set=conn.exec_driver_sql(query)
            conn.commit()
    except Exception as e:
        print('error')
    else:
        print("No of Records deleted : ",r_set.rowcount)
    finally:
        df.to_sql(table_name, db, if_exists='append')
        conn.close()

def read_file(pdf_file):
    reader = PyPDF2.PdfReader(pdf_file)
    print(reader.metadata)
    tables = []
    for page_num in range(len(reader.pages)):
        table = tabula.io.read_pdf(pdf_file, pages=page_num + 1, stream=True)[0]
        tables.append(table)
    df = pd.concat(tables, ignore_index=True)
    
    #df.to_csv('./lapkeu.csv')
    
    #sisi aset
    cols = ['account','2023','2022']
    dfAsset = df[['Unnamed: 2','Unnamed: 4','Unnamed: 5']].dropna()
    dfAsset.columns= cols
    dfAsset['{}'.format(cols[1])]=dfAsset['{}'.format(cols[1])].replace([","," "],"",inplace=True)
    #pendapatan
    dfPendapatan = df[['LAPORAN LABA (RUGI) KOMPREHENSIF','Unnamed: 10']].dropna()
    #pemenuhan tingkat solvability
    dfTks= df['TINGKAT KESEHATAN KEUANGAN'].dropna()
    #liabilitas & equitas
    dflia= df[['LAPORAN POSISI KEUANGAN','Unnamed: 7','Unnamed: 8']].fillna(0)

    #print(dfAsset)
    #print(dfPendapatan)
    #print(dfTks)
    #print(dflia)
    
    #dfAsset,company_name,yr,yrprev,mth
    asset_konven(dfAsset,'PT AIA Financial','Konvensional',2023,2022,'Desember')
    
    #print(df.columns)

def asset_konven(dfAsset,company_name,jenis,yr,yrprev,mth):
    cols = ['report_date','company_name','account','jenis','sector','created_date']
    cols += months
    #print(f'cols',cols)
    
    df = pd.DataFrame(columns=cols)
    report_date=get_report_date(yr,mth)
    #print(report_date)
    
    df['report_date']=report_date
    df['company_name']=company_name
    print(dfAsset)
    df['account']=dfAsset['account']
    df['{}'.format(mth)] = dfAsset['{}'.format(yr)].replace([',', ' '],'',inplace=True)
    df['jenis']=jenis
    created_date=pd.Timestamp.now
    df['created_date']= created_date
    
    dump_to_db('TKS','ASSET',df,report_date)
    
    df = pd.DataFrame(columns=cols)
    report_date=get_report_date(yrprev,mth)
    df['report_date']=report_date
    df['company_name']=company_name
    df['account']=dfAsset['account']
    df['{}'.format(mth)] = dfAsset['{}'.format(yrprev)].replace([',', ' '],'',inplace=True)
    df['jenis']=jenis
    created_date=pd.Timestamp.now
    df['created_date']= created_date
    dump_to_db('TKS','ASSET',df,report_date)
    
    print(df)

if __name__=="__main__":
    
    pdf_file=f"Published-Conventional-Dec23(unaudited).pdf.coredownload.inline.pdf"
    print(pdf_file)
    df=read_file(pdf_file) 
    print(df)