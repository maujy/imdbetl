
# coding: utf-8

# In[ ]:


import csv
import pandas as pd

import time
import random
import requests 
from bs4 import BeautifulSoup as bs
import concurrent.futures

WORKERNUM = 90
RETRYTIME = 0.3
# dprint = print
def dprint(s):
    True

DOMAIN = "http://www.imdb.com"
NAMEFILE = "./input/movie_principalCast_list.csv"
NMBIOFILE = "./output/movie_principalCast_nmbio_list.csv"
NMBIOFIELDS = ['nconst','overview','mini_bio','spouse',
               'trademark','trivia','quotes','salary']

tStart = time.time()
csvfile = open(NMBIOFILE, 'w')
writer = csv.DictWriter(csvfile, fieldnames=NMBIOFIELDS)
writer.writeheader()

nmbio_df = pd.read_csv(NAMEFILE, skipinitialspace=True, usecols=['nconst'])
nmbio_df = nmbio_df.reindex(columns = NMBIOFIELDS)
dprint(nmbio_df.head())

# get free proxy list
def get_proxy():
    resp = requests.get("https://free-proxy-list.net/")
    iplist = bs(resp.text, "lxml").select_one(".table-striped").select("tbody tr")
    plist = [iplist[i].select("td")[0].text + ":" + iplist[i].select("td")[1].text
            for i in range(len(iplist))]
    return plist

# random choice proxy for crawling
pflag=0
def get_url_data(url):
    global pflag
    global proxies
    if (pflag == 0):
        proxies = get_proxy()
        pflag = 1000
    while True:
        pflag-= 1
        proxy = {'http':'http://' + random.choice(proxies)}
        dprint(proxy)
        try:
            return requests.get(url, proxies=proxy, timeout=(1,3))
        except:
            time.sleep(RETRYTIME)     
            
# use imdb nconst to get director or cast biography
def worker(index):
    global nmbio_df
    print(nmbio_df.loc[index,'nconst']+",")
    url = DOMAIN + "/name/" + nmbio_df.loc[index,'nconst'] + "/bio"
    dprint(url)
    
    resp = get_url_data(url)
    dprint(resp)
    while (resp.status_code != 200):
        dprint("retry ...")
        resp = get_url_data(url)
        dprint(resp)
        
    soup = bs(resp.text, 'html5lib')
    try:
        # according to the tag of jumpto list to get personal information
        jumpto = soup.find('div', class_='jumpto')
        if (jumpto is not None):
            biography = jumpto.find_all('a')
            for bio in biography:
                category = bio["href"].replace("#","")
                tag = soup.find('a', attrs={'name':category}).find_next().find_next()
                if (tag.name == "table"):
                    text = ""
                    for tr in tag.find_all('tr'):
                        k = ' '.join(tr.find_next('td').text.replace("\n","").split())
                        v = ' '.join(tr.find_next('td').find_next('td').text.replace("\n", "").split())
                        text += k + ":" +v + ","
                    nmbio_df.loc[index,category] = text
                elif (tag.name == "div"):
                    text = ""
                    while (tag.name == "div"):
                        text += ' '.join(tag.text.replace("\n","").split())
                        tag = tag.find_next_sibling()
                    nmbio_df.loc[index,category] = text
        else:
            print("no jumpto: %s"%(nmbio_df.loc[index,'nconst']))
        global csvfile, writer             
        writer.writerow(nmbio_df.iloc[index].to_dict())
        csvfile.flush()
        mbio_df.iloc[index]=""
        return
    except:
        traceback.print_exc(limit=1, file=sys.stdout)
        time.sleep(RETRYTIME)

# multithread crawler
with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERNUM) as executor:
    executor.map(worker,range(0,len(nmbio_df)))
#     executor.map(worker,range(0,10000)) 
       
csvfile.close()

tEnd = time.time()
print("-----------------------------")
print("Getting data num: %s"%(len(nmbio_df)))
print("Concurrent worker num: %s"%(WORKERNUM))
print("Execute time: %s"%(tEnd-tStart))

