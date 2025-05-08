"""
@author: Covariant Joe

Apache Airflow pipeline to web scrape house market data in Guadalajara.
Performs the ETL process, saving the data to a IBM db2 warehouse and an inflation data mart.
It prevents duplicates from being written to the db.

Sources are mercado libre, casas y terrenos, and inmuebles24 before they blocked scraper programs.

"""

import re
from bs4 import BeautifulSoup
from requests import request
from datetime import datetime
import os
import subprocess
import json

# ------------- Configure -------------
N_PAGES = 1  # How many pages to request per url, only applies to new data
#------------- ------------- -------------

# --------------------------Get new data----------------------------
Url1 = "https://www.casasyterrenos.com/jalisco/guadalajara/casas/venta?desde=0&hasta=1000000000"
"https://www.casasyterrenos.com/jalisco/guadalajara/casas/renta?desde=0&hasta=1000000000"
"https://www.casasyterrenos.com/jalisco/guadalajara/departamentos/renta?desde=0&hasta=1000000000"
"https://www.casasyterrenos.com/jalisco/guadalajara/departamentos/venta?desde=0&hasta=1000000000"
Url2 = "https://www.casasyterrenos.com/buscar/jalisco/guadalajara/casas-y-departamentos/renta?desde=0&hasta=1000000000&utm_source=results_page"

Url3 = "https://inmuebles.mercadolibre.com.mx/departamentos/renta/jalisco/guadalajara/"
Url4 = "https://inmuebles.mercadolibre.com.mx/departamentos/venta/jalisco/guadalajara/"
Url5 = "https://inmuebles.mercadolibre.com.mx/casas/venta/jalisco/guadalajara/"
Url6 = "https://inmuebles.mercadolibre.com.mx/casas/renta/jalisco/guadalajara/"

# --------------------------Data from 2022--------------------------
Url7 = "https://web.archive.org/web/20221208000341/https://inmuebles.mercadolibre.com.mx/departamentos/renta/jalisco/guadalajara/"

# --------------------------Data from 2021--------------------------
Url8 = "https://web.archive.org/web/20210514160946/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url9 = "https://web.archive.org/web/20210724231729/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url10 =  "https://web.archive.org/web/20210724230610/https://www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"


# --------------------------Data from 2020--------------------------
Url11 = "https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url12 = "https://web.archive.org/web/20201126121239/https://www.inmuebles24.com/casas-en-venta-en-guadalajara.html"
Url13 = "https://web.archive.org/web/20201129221404/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url14 = "https://web.archive.org/web/20201129095928/https://www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"

# --------------------------Data from 2017--------------------------
# Apartments
Url15 = "https://web.archive.org/web/20170707002615/https://inmuebles.mercadolibre.com.mx/departamentos/renta/jalisco/guadalajara/"
Url16 = "https://web.archive.org/web/20170707041708/https://inmuebles.mercadolibre.com.mx/departamentos/venta/jalisco/guadalajara/"


Url17 = "https://web.archive.org/web/20171117104650/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url18 = "https://web.archive.org/web/20171124091233/http://www.inmuebles24.com/casas-en-renta-en-guadalajara-pagina-2.html"
Url19 = "https://web.archive.org/web/20171115160106/http://www.inmuebles24.com/casas-en-renta-en-guadalajara-pagina-3.html"
Url20 = "https://web.archive.org/web/20171115051725/http://www.inmuebles24.com/casas-en-renta-en-guadalajara-pagina-4.html"
Url21 = "https://web.archive.org/web/20171115030305/http://www.inmuebles24.com/casas-en-renta-en-guadalajara-pagina-5.html"

Url22 = "https://web.archive.org/web/20171113003311/https://www.inmuebles24.com/casas-en-venta-en-guadalajara.html"
Url23 = "https://web.archive.org/web/20171111024429/http://www.inmuebles24.com/casas-en-venta-en-guadalajara-pagina-2.html"
Url24 = "https://web.archive.org/web/20171111053132/http://www.inmuebles24.com/casas-en-venta-en-guadalajara-pagina-3.html"
Url25 = "https://web.archive.org/web/20171111133150/http://www.inmuebles24.com/casas-en-venta-en-guadalajara-pagina-4.html"
Url26 = "https://web.archive.org/web/20171112015617/http://www.inmuebles24.com/casas-en-venta-en-guadalajara-pagina-5.html"

Url27 = "https://web.archive.org/web/20171111020224/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url28 = "https://web.archive.org/web/20171111072509/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara-pagina-2.html"
Url29 = "https://web.archive.org/web/20171111072509/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara-pagina-3.html"
Url30 = "https://web.archive.org/web/20171111072509/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara-pagina-4.html"
Url31 = "https://web.archive.org/web/20171111072509/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara-pagina-5.html"



Url32 = "https://web.archive.org/web/20171122235650/https://www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"
Url33 = "https://web.archive.org/web/20171119111019/http://www.inmuebles24.com/departamentos-en-venta-en-guadalajara-pagina-2.html"
Url34 = "https://web.archive.org/web/20171119111023/http://www.inmuebles24.com/departamentos-en-venta-en-guadalajara-pagina-3.html"
Url35 = "https://web.archive.org/web/20171119111028/http://www.inmuebles24.com/departamentos-en-venta-en-guadalajara-pagina-4.html"
Url36 = "https://web.archive.org/web/20171122062810/http://www.inmuebles24.com/departamentos-en-venta-en-guadalajara-pagina-5.html"

# Houses
Url37 = "https://web.archive.org/web/20170707022710/https://inmuebles.mercadolibre.com.mx/casas/venta/jalisco/guadalajara/"
Url38 = "https://web.archive.org/web/20170707014305/https://inmuebles.mercadolibre.com.mx/casas/renta/jalisco/guadalajara/"


# --------------------------Data from 2015--------------------------
Url39 = "https://web.archive.org/web/20150928075811/http://www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"
Url40 = "https://web.archive.org/web/20150928034046/https://www.inmuebles24.com/casas-en-venta-en-guadalajara.html"
Url41 = "https://web.archive.org/web/20150926110649/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url42 = "https://web.archive.org/web/20151021063305/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"

#_Desde_145_NoIndex_True

def log(message):
    now = datetime.now()
    path = os.getcwd()
    T0 = now.strftime("%Y-%m-%d %H:%M:%S")

    try:
        with open(path + "etl.log", "a") as f:
            f.write("[ " + T0 + " ] " + message + "\n")
    except FileNotFoundError:
            try:
                subprocess.run("touch " + path + "etl.log",shell = True)
            except:
                raise Exception("Fatal error, couldn't find nor create log")
            return -1
    return 0

def Extractor(urls):
    '''
    Pipeline stage to scrape the rent and sale house/department data from different sources

    Parameters
    ----------
    urls : list with urls to scrape from mercado libre or casas y terrenos.

    Returns
    -------
    A dictionary with all the data that could be extracted.

    '''
    data = []

    for u in urls:
        domain = u.split('/')[2]
        content = request("get", u)
    
    if content.status_code == 403:
        log(f"Error extracting data from {domain}, code 403, IP most likely blocked")
        return
    elif content.status_code > 400:
        log(f"Error extracting data from {domain}, the server returned code {content.status_code}")
        return

    soup = BeautifulSoup(content.text, 'html.parser')
    if "web.archive" in domain:
        data.extend( web_archive_parser(soup, u) )
    elif "mercadolibre" in domain:
        data.extend( mercado_libre_parser(soup) )
    elif "casasyterrenos" in domain:
        data.extend( casas_y_terrenos_parser(soup) )
    else:
        log(f"Error parsing, domain {domain} not implemented")
    
    return data
    
def mercado_libre_parser(soup):
    '''
    Function to parse the HTML returned by Mercado Libre with JSON

    Parameters
    ----------
    content : HTML returned by requests.request

    Returns
    -------
    A list of dictionaries with all the data that could be extracted.

    '''
    data = []
    source = json.loads(soup.find_all("script")[3].get_text())["pageState"]["initialState"]["results"]
    for house in source:
        house = house["polycard"]
        try:
            ID = house["unique_id"]
            url = house["metadata"]["url"]
            name = house["pictures"]["sanitized_title"]
            
            house = house["components"]
            for i in len(house):
                if house[i]["type"] == 'headline':             
                    info_sale = house[i]["headline"]["text"]
                elif house[i]["type"] == 'price':                    
                    price = house[i]["price"]["current_price"]["value"]
                elif house[i]["type"] == 'attributes_list':             
                    info_rooms = house["components"][i]["attributes_list"]["texts"]
                elif house[i]["type"] == 'location':             
                    info_location = house[i]["locations"]["text"]
        except:
            log("Error extracting data from house with url {url}")
            continue
        
        if "casa" in info_sale.lower():
            tipo = "casa"
        elif "departamento" in info_sale.lower():
            tipo = "departamento"
        else:
            tipo = "na"
            
        if "renta" in info_sale.lower():
            sale = "rent"
        elif "venta" in info_sale.lower():
            sale = "sale"
        else:
            sale = "na"
        
        rooms = int(info_rooms[0].split(" ")[0])
        bathrooms = int(info_rooms[1].split(" ")[0]) 
        size = int(info_rooms[2].split(" ")[0])
        location = info_location.split(",")[0]
        
        data.append({"ID":ID, "name":name, "price":price, "rooms":rooms, "bathrooms":bathrooms, "size":size, "type":tipo, "sale":sale, "location":location, "year":datetime.now(), "url":url, "permalink":url})
    return data    

def casas_y_terrenos_parser(soup):
    '''
    Function to parse the HTML returned by casas y terrenos with JSON

    Parameters
    ----------
    content : HTML returned by requests.request

    Returns
    -------
    A list of dictionaries with all the data that could be extracted.

    '''
    data = []
    source = json.loads(soup.find_all("script")[-1].get_text())["props"]["pageProps"]["initialState"]
    tipo = source["filters"]["propertyType"][0]
    source = source["propertyData"]["properties"]
    
    for house in source:
        url = "www.casasyterrenos.com" + house["canonical"]
        if bool(house["isSale"]):
            price = house["priceSale"]
            sale = "sale"
        elif bool(house["isRent"]):
            price = house["priceRent"]
            sale = "rent"
        else:
            continue
        data.append({"ID":str(house["id"]), "name": house["name"], "price":price, "rooms": house["rooms"], "bathrooms":house["bathrooms"], "size":house["construction"], "type":tipo, "sale":sale, "location":house["neighborhood"], "year": int(house["lastUpdate"][:4]), "url":url, "permalink":url })
    return  data  

def web_archive_parser(soup, url):
    '''
    Function to parse the HTML returned by webarchive with JSON and re

    Parameters
    ----------
    content : HTML returned by requests.request

    Returns
    -------
    A list of dictionaries with all the data that could be extracted.

    '''
    data = []
    year = int(url.split("/")[4][:4])
    
    if "mercadolibre" in url and year > 2021:
        script = soup.find("script", text=re.compile("__PRELOADED_STATE__")).string  
        match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*({.*?});', script, re.DOTALL)
    
        if match:
            json_str = match.group(1)
        else:
            log("Error extracting data from web archive mercado libre")
            raise Exception()
        
        source = json.loads(match.group(1))["initialState"]["results"]
        
        for house in source:
            if "departamento" in house["subtitles"]["operation"].lower() or "departamento" in house["title"].lower():
                tipo = "departamento"
            elif "casa" in house["subtitles"]["operation"].lower() or "casa" in house["title"].lower():
                tipo = "casa"
            else:
                tipo = "na"
            if "renta" in house["subtitles"]["operation"].lower() or "renta" in house["title"].lower():
                sale = "rent"
            elif "venta" in house["subtitles"]["operation"].lower() or "venta" in house["title"].lower():
                sale = "sale"
            else:
                sale = "na"
            
            location = "na"
            info_location = house["location"].split(",")
            for j in range(len(info_location)):
                if info_location[j].strip().lower() == "guadalajara":
                    location = info_location[j-1].strip()
                    break
                
            data.append({"ID":house["id"], "name": house["subtitles"],"price": house["price"]["amount"],"rooms":int(house["descriptions"][1]["label"].split(" ")[0]),"bathrooms":"na","size":int(house["descriptions"][0]["label"].split(" ")[0]),"type":tipo,"sale":sale,"location":location, "year": year, "url":url, "permalink": house["permalink"]})
    
    elif "mercadolibre" in url:
        'div class="rowItem item item--grid new'
    elif "inmuebles24" in url:
        pass    
    return data