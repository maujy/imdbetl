
# coding: utf-8

# In[ ]:

import csv
import pandas as pd

import time
import random
import requests 
from bs4 import BeautifulSoup as bs
import concurrent.futures

#from pymongo import MongoClient
#client = MongoClient("mongodb://localhost:27017")
#db = client.imdb
##coll = db.nminfo
#coll = db.test

WORKERNUM = 30
RETRYTIME = 0.3
#dprint = print
def dprint(s):
    True

DOMAIN = "http://www.imdb.com"
NAMEFILE = "./input/movie_directors_list.csv"
NMINFOFILE = "./output/movie_directors_nminfo_list.csv"
NMINFOFIELDS = ['nconst', 'starmeter', 'award', 'akas', 'director', 'producer', 'writer',
                'actor', 'actress', 'miscellaneous', 
                'assistant_director', 'art_director', 'set_decorator', 'casting_director',
                'editor', 'soundtrack', 'composer', 'stunts', 'special_effects',
                'cinematographer', 'visual_effects', 'costume_designer',
                'camera_department', 'editorial_department', 'music_department', 'sound_department',
                'animation_department', 'art_department', 'make_up_department', 
                'costume_department', 'casting_department', 'transportation_department',
                'production_department', 'production_designer', 'production_manager', 'location_management', 
                'thanks', 'self' , 'archive_footage']

tStart = time.time()
csvfile = open(NMINFOFILE, 'w')
writer = csv.DictWriter(csvfile, fieldnames=NMINFOFIELDS)
writer.writeheader()

nminfo_df = pd.read_csv(NAMEFILE, skipinitialspace=True, usecols=['nconst'])
nminfo_df = nminfo_df.reindex(columns = NMINFOFIELDS)
dprint(nminfo_df.head())

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
    global nminfo_df
    print(nminfo_df.loc[index,'nconst']+",")
    url = DOMAIN + "/name/" + nminfo_df.loc[index,'nconst']
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
        starmeter = soup.find('a', id='meterRank')
        if (starmeter is not None):
            nminfo_df.loc[index,'starmeter'] = starmeter.text.strip()
        
        award = soup.find('span', itemprop='awards')
        if (award is not None):
            nminfo_df.loc[index,'award']=' '.join(award.text.split())

        jumpto = soup.find('div', id='jumpto')
        if (jumpto is not None):
            filmography = jumpto.find_all('a')
            for filmo in filmography:
                section = filmo["data-category"]
                if section in nminfo_df.columns.tolist():
                    data_id = filmo["data-category"] + "-tt"
                    text = ""
                    for film in soup.select('div[id^='+data_id+']'):
                        tconst = film['id'].replace(section+"-","")                         
                        text += tconst + ","
                    nminfo_df.loc[index, section] = text
                else:
                    print("new section: %s"%(section))
        else:
            print("no jumpto: %s"%(nminfo_df.loc[index,'nconst']))

        akas = soup.find('div', id='details-akas')
        if (akas is not None):
            nminfo_df.loc[index,'akas'] = akas.text.replace(" | ",",").replace("Alternate Names:","").strip()
            
#        result = coll.insert_one(nminfo_df.iloc[index].to_dict())
        global csvfile, writer             
        writer.writerow(nminfo_df.iloc[index].to_dict())
        csvfile.flush()        
        nminfo_df.iloc[index]=""
        return
    except:
        traceback.print_exc(limit=1, file=sys.stdout)
        time.sleep(RETRYTIME)  

# multithread crawler
with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERNUM) as executor:
    executor.map(worker,range(0,len(nminfo_df)))
#    executor.map(worker,range(0,1000)) 
       
#client.close()
csvfile.close()

tEnd = time.time()
print("-----------------------------")
print("Getting data num: %s"%(len(nminfo_df)))
print("Concurrent worker num: %s"%(WORKERNUM))
print("Execute time: %s"%(tEnd-tStart))

