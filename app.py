import pandas as pd
import requests
from bs4 import BeautifulSoup
from scrap import *
from tks import *
from log import *

db_name_aj = './datas/Asuransi_Jiwa.db'
db_name_au ='./datas/Asuransi_Umum.db'

tbl_name_aj = 'Asuransi_Jiwa'
tbl_name_au = 'Asuransi_Umum'

db_tks='TKS'
tbl_tks='TKS'

def get_data(yr=2023):
    import sqlite3
    conn = sqlite3.connect(db_name_aj)
    df_aj = get_aj(yr,conn)
    conn.close()

    conn = sqlite3.connect(db_name_au)
    df_au = get_au(yr,conn)
    conn.close()

    df = pd.concat([df_aj,df_au],axis=0)

    # save to xls
    df.to_csv('./url_aj_au.csv')
    return df


def get_aj(yr,conn):
    query = "SELECT distinct [Nama Perusahaan] as Nama_Perusahaan,Website,'life_insurance' as sector From {} WHERE strftime('%Y',report_date)='{}'".format(tbl_name_aj,yr)
    #print(query)
    df_aj =pd.read_sql_query(query,conn)
    return df_aj

def get_au(yr,conn):
    query = "SELECT distinct [Nama Perusahaan] as Nama_Perusahaan,Website,'non_life_insurance' as sector From {} WHERE strftime('%Y',report_date)='{}'".format(tbl_name_au,yr)
    #print(query)
    df_au =pd.read_sql_query(query,conn)
    return df_au

def findarrayset(a,b):
    common = set(a)&set(b)
    diff=set(a)^set(b)
    #print(list(common))
    return list(diff) 

def get_me(nama,url,tag='h1'):
    #titleandmetaTags(url)
    #tags = getTags(url,'p')
    #headtags = headingTags(url,'h1')
    #for tag in tags:
    #    print(" Here are the tags from getTags function:", tag.contents)
    #alt_tag(url)
    #url='https://www.bumiputera.com/listdocument/document/our_company/financial_report/0/4/0/1'
    #url = 'http://avrist.com/avrist-life/about/Financial-Report'
    #url="{}/id/about-aia/laporan-penting/report-financial".format(url)
    #print(url)
    list_of_links=get_pdfs(url,nama)
    #print(list_of_links)
    
    if list_of_links and len(list_of_links)>0:
        list_of_links=list(dict.fromkeys(list_of_links)) # remove duplicate key links
        list_all_links=[]
        last_of_pages=False
        while not last_of_pages:
            print('he..he.{}'.format(url))
            for lnk in list_of_links:
                if not 'http' in lnk:
                    _url="{}{}".format(url,lnk)
                else:
                    _url=lnk
                msg="second level: {}".format(_url)
                info(msg)
                #print(msg)
                lst=get_pdfs(_url,nama,1)
                if lst and len(lst)>0:
                    lst=list(dict.fromkeys(lst)) #remove duplicate.. 
                    if len(list_all_links)>0:
                        list_all_links.extend(lst)
                    else:
                        list_all_links=lst

                    list_all_links=list(dict.fromkeys(list_all_links)) # remove duplicate key links

            if len(list_all_links)>0:
                #should be check is already call in 2 putaran?
                comparelist=findarrayset(list_of_links,list_all_links)
                if len(comparelist)==0:
                    last_of_pages=True
                else:
                    list_of_links = list_all_links
                    list_all_links=set()

                #msg=f"third level:",list_of_links
                #print(msg)
                #list_all_links.clear()
                #print(len(list_of_links))
            else:
                last_of_pages=True

def crawle_me(url):
    # Globals
    #url = url#'http://quotes.toscrape.com'

    url_list = [url,]
    pages = []
    soup_list = []
    not_last_page = True

    #1: Pull the requests
    def pullUrl(func):
        def inner(*args, **kwargs):
            page = requests.get(url_list[-1])
            if page.status_code == 200:
                pages.append(page)
                func(*args, **kwargs)
            else:
                print(f'The url {url} returned a status of {page.status_code}')
        return inner
        
    #2: Make some soup
    def makeSoup(func):
        def inner(*args, **kwargs):
            soup = BeautifulSoup(pages[-1].content, 'html.parser')
            soup_list.append(soup)
            func(*args, **kwargs)
        return inner
        
    #3: Parse the URLs
    @pullUrl
    @makeSoup
    def getURLs():
        global not_last_page
        try:
            next_page = url+soup_list[-1].find('li', {'class': 'next'}).find('a')['href']
            print(next_page)
            url_list.append(next_page)
        except:
            not_last_page = False


    while not_last_page:
        getURLs()

    # Start with an empty Data Frame:
    quotes_df = pd.DataFrame(columns=['Author', 'Quote'])

    # Add in the quotes dictionary:
    quotes_dict = {}

    try:
        for i in range(len(soup_list)):
            quotes = soup_list[i].find_all('div', {'class': 'quote'})
            for j in range(len(quotes)):
                v = quotes[j].find('small', {'class': 'author'}).text
                k = quotes[j].find('span', {'class': 'text'}).text
                quotes_dict[k] = v
    except: print('issue with', {i, j})

    quotes_df = pd.DataFrame(list(quotes_dict.items()), columns=['Quote', 'Author'])[['Author', 'Quote']].sort_values('Author')

    return quotes_

def getTKS(filename,isSyariah):
    #syariah
    pdf_file=filenam 
    if isSyariah:
        df=readpdf_syariah(pdf_file)
    else:
        df=readpdf_konven(pdf_file)
    
    df['Nama_Perusahaan']=row['Nama_Perusahaan']
    report_date=get_report_date(2023,'Desember')
    df['report_date']=report_date 
    dump_to_db(db_tks,tbl_tks,df,report_date)

def count_file(folder):
    import os
    count = 0
    for root_dir, cur_dir, files in os.walk(folder):
        count += len(files)
    return count

if __name__=="__main__":
    print('start...')
    
    loginit()

    df = get_data()
    #input
    yr = '23'
    month='Dec'
    # loop through the rows using iterrows()
    for index, row in df.iterrows():
        
        company_name=row['Nama_Perusahaan']
        
        if 'PT AIA Financial' in company_name : continue
        if 'PT Avrist Assurance' in company_name: continue
        if 'PT Asuransi Allianz Life Indonesia' in company_name :continue 
        if 'Asuransi Jiwa Bersama Bumiputera 1912' in company_name :continue
        #check latest
        if 'PT Axa Mandiri Financial Services' in company_name :continue
        if 'PT Axa Financial Indonesia' in company_name :continue

        print(row['Website'], row['Nama_Perusahaan'])
        
        url_web=row['Website']
        folder_pdfs = './pdfs/{}'.format(company_name)
        print(folder_pdfs)
        cnt_file =count_file(folder_pdfs)
        if cnt_file==0:
            if url_web and "https" in url_web:
                pages=get_me(company_name,"{}".format(url_web))
            else:
                pages=get_me(company_name,"https://{}".format(url_web))
        #print(pages)
        #check pages link
        #for page in pages:
        #    url = "{}{}".format(url_web,page)
        #    new_pages=get_me(company_name,"{}".format(url))
        '''
        for filename in os.listdir(pdf_folder):
            f = os.path.join(pdf_folder, filename)
            # checking if it is a file
            if os.path.isfile(f):
                split_tup = os.path.splitext(filename)
                if 'pdf' in split_tup[1]:
                    if yr  in filename and month in filename: 
                        if 'Syariah' in filename:
                            #getTKS(f,True)     
                            print('syariah')
                        else:
                            getTKS(f,False)
        '''
        #break
 