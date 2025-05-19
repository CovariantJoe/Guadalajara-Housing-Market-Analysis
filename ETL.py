"""
@author: Covariant Joe

Functions to add to Apache Airflow pipeline to web scrape house market data in Guadalajara.
ETL.py may be used alone or from AirflowDAG.py.
Performs the ETL process, saving the data to a IBM db2 warehouse and an inflation data mart.
It prevents duplicates from being written to the db.

Sources are mercado libre, casas y terrenos, and inmuebles24 before they blocked scraper programs.

Credentials need to be provided in Credentials.txt to connect remotely to a IBM db2 database instance.

In order to run this program ibm_db needs to be installed, using pip for example.
"""

import time
import re
from bs4 import BeautifulSoup
from requests import request
from datetime import datetime
import os
import subprocess
import json

# ------------- Configure -------------
Credentials = "Credentials.txt" # Path to file with IBM db2 login/connection credentials
#------------- ------------- -------------

# --------------------------Get new data----------------------------
Url1 = "https://www.casasyterrenos.com/jalisco/guadalajara/casas/venta?desde=0&hasta=1000000000"
Url2 = "https://www.casasyterrenos.com/jalisco/guadalajara/casas/renta?desde=0&hasta=1000000000"
Url3 = "https://www.casasyterrenos.com/jalisco/guadalajara/departamentos/renta?desde=0&hasta=1000000000"
Url4 = "https://www.casasyterrenos.com/jalisco/guadalajara/departamentos/venta?desde=0&hasta=1000000000"
Url5 = "https://www.casasyterrenos.com/buscar/jalisco/guadalajara/casas-y-departamentos/renta?desde=0&hasta=1000000000&utm_source=results_page"

Url6 = "https://inmuebles.mercadolibre.com.mx/departamentos/renta/jalisco/guadalajara/"
Url7 = "https://inmuebles.mercadolibre.com.mx/departamentos/venta/jalisco/guadalajara/"
Url8 = "https://inmuebles.mercadolibre.com.mx/casas/venta/jalisco/guadalajara/"
Url9 = "https://inmuebles.mercadolibre.com.mx/casas/renta/jalisco/guadalajara/"

# --------------------------Data from 2022--------------------------
Url10 = "https://web.archive.org/web/20221208000341/https://inmuebles.mercadolibre.com.mx/departamentos/renta/jalisco/guadalajara/"

# --------------------------Data from 2021--------------------------
Url11 = "https://web.archive.org/web/20210514160946/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url12 = "https://web.archive.org/web/20210724231729/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url13 =  "https://web.archive.org/web/20210724230610/https://www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"


# --------------------------Data from 2020--------------------------
Url14 = "https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url15 = "https://web.archive.org/web/20201126121239/https://www.inmuebles24.com/casas-en-venta-en-guadalajara.html"
Url16 = "https://web.archive.org/web/20201129221404/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url17 = "https://web.archive.org/web/20201129095928/https://www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"

# --------------------------Data from 2017--------------------------
# Apartments
Url18 = "https://web.archive.org/web/20170707002615/https://inmuebles.mercadolibre.com.mx/departamentos/renta/jalisco/guadalajara/"
Url19 = "https://web.archive.org/web/20170707041708/https://inmuebles.mercadolibre.com.mx/departamentos/venta/jalisco/guadalajara/"


Url20 = "https://web.archive.org/web/20171117104650/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url21 = "https://web.archive.org/web/20171124091233/http://www.inmuebles24.com/casas-en-renta-en-guadalajara-pagina-2.html"
Url22 = "https://web.archive.org/web/20171115160106/http://www.inmuebles24.com/casas-en-renta-en-guadalajara-pagina-3.html"
Url23 = "https://web.archive.org/web/20171115051725/http://www.inmuebles24.com/casas-en-renta-en-guadalajara-pagina-4.html"
Url24 = "https://web.archive.org/web/20171115030305/http://www.inmuebles24.com/casas-en-renta-en-guadalajara-pagina-5.html"

Url25 = "https://web.archive.org/web/20171113003311/https://www.inmuebles24.com/casas-en-venta-en-guadalajara.html"
Url26 = "https://web.archive.org/web/20171111024429/http://www.inmuebles24.com/casas-en-venta-en-guadalajara-pagina-2.html"
Url27 = "https://web.archive.org/web/20171111053132/http://www.inmuebles24.com/casas-en-venta-en-guadalajara-pagina-3.html"
Url28 = "https://web.archive.org/web/20171111133150/http://www.inmuebles24.com/casas-en-venta-en-guadalajara-pagina-4.html"
Url29 = "https://web.archive.org/web/20171112015617/http://www.inmuebles24.com/casas-en-venta-en-guadalajara-pagina-5.html"

Url30 = "https://web.archive.org/web/20171111020224/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url31 = "https://web.archive.org/web/20171111072509/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara-pagina-2.html"
Url32 = "https://web.archive.org/web/20171111072509/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara-pagina-3.html"
Url33 = "https://web.archive.org/web/20171111072509/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara-pagina-4.html"
Url34 = "https://web.archive.org/web/20171111072509/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara-pagina-5.html"



Url35 = "https://web.archive.org/web/20171122235650/https://www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"
Url36 = "https://web.archive.org/web/20171119111019/http://www.inmuebles24.com/departamentos-en-venta-en-guadalajara-pagina-2.html"
Url37 = "https://web.archive.org/web/20171119111023/http://www.inmuebles24.com/departamentos-en-venta-en-guadalajara-pagina-3.html"
Url38 = "https://web.archive.org/web/20171119111028/http://www.inmuebles24.com/departamentos-en-venta-en-guadalajara-pagina-4.html"
Url39 = "https://web.archive.org/web/20171122062810/http://www.inmuebles24.com/departamentos-en-venta-en-guadalajara-pagina-5.html"

Url40 = "https://web.archive.org/web/20170707022710/https://inmuebles.mercadolibre.com.mx/casas/venta/jalisco/guadalajara/"
Url41 = "https://web.archive.org/web/20170707014305/https://inmuebles.mercadolibre.com.mx/casas/renta/jalisco/guadalajara/"

# --------------------------Data from 2016--------------------------
Url42 = "https://web.archive.org/web/20160310052802/www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"
Url43 = "https://web.archive.org/web/20160307002542/www.inmuebles24.com/casas-en-venta-en-guadalajara.html"
Url44 = "https://web.archive.org/web/20160513061027/www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"
Url45 = "https://web.archive.org/web/20160511220424/www.inmuebles24.com/casas-en-venta-en-guadalajara.html"
Url46 = "https://web.archive.org/web/20160716072356/www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"
Url47 = "https://web.archive.org/web/20160716084233/www.inmuebles24.com/casas-en-venta-en-guadalajara.html"
Url48 = "https://web.archive.org/web/20161011145610/www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"
Url49 = "https://web.archive.org/web/20161011202858/www.inmuebles24.com/casas-en-venta-en-guadalajara.html"

Url50 = "https://web.archive.org/web/20160305023115/www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url51 =  "https://web.archive.org/web/20160307002305/www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url52 =  "https://web.archive.org/web/20160405164559/www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url53 =  "https://web.archive.org/web/20160407090314/www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url54 =  "https://web.archive.org/web/20160512003735/www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url55 =  "https://web.archive.org/web/20160508064440/www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url56 =  "https://web.archive.org/web/20160713124118/www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url57 =  "https://web.archive.org/web/20160720204550/www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url58 =  "https://web.archive.org/web/20161013133724/www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url59 =  "https://web.archive.org/web/20161013063056/www.inmuebles24.com/casas-en-renta-en-guadalajara.html"

# --------------------------Data from 2015--------------------------
Url60 = "https://web.archive.org/web/20150928075811/http://www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"
Url61 = "https://web.archive.org/web/20150928034046/https://www.inmuebles24.com/casas-en-venta-en-guadalajara.html"
Url62 = "https://web.archive.org/web/20150926110649/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url63 = "https://web.archive.org/web/20151021063305/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url64 = "https://web.archive.org/web/20151230150006/www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url65 = "https://web.archive.org/web/20151230170106/www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url66 = "https://web.archive.org/web/20150819080049/www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"
Url67 = "https://web.archive.org/web/20151208005800/www.inmuebles24.com/casas-en-venta-en-guadalajara.html"
Url68 = "https://web.archive.org/web/20151202073008/www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"

Urls = [ globals()[f"Url{i}"] for i in range(1,69) ]

def log(message):
    now = datetime.now()
    path = os.getcwd()
    T0 = now.strftime("%Y-%m-%d %H:%M:%S")

    try:
        with open(path + "/etl.log", "a") as f:
            f.write(T0 + " " + f"{message}" + "\n")
    except FileNotFoundError:
            try:
                subprocess.run("touch " + path + "/etl.log",shell = True)
                with open(path + "/etl.log", "a") as f:
                    f.write("[ " + T0 + " ] " + f"{message}" + "\n")
            except:
                raise Exception("Fatal error, couldn't find nor create log")

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
        try:
            content = request("get", u)
        except Exception as e:
            if "Failed to establish a new connection: [Errno 111] Connection refused" in str(e):
                log(f"[Error] - extracting data from {u}: Connection refused, you're most likely rate-limited")                
            else:
                log(f"[Error] - extracting data from {u}: {e}")
            continue
        else:
            if content.status_code == 403:
                log(f"[Error] - extracting data from {domain}, code 403, IP most likely blocked or programs like this one blocked")
                continue
            elif content.status_code > 400:
                log(f"[Error] - extracting data from {domain} the server returned code {content.status_code}")
                continue
            elif content.status_code == 202:
                log(f"[Warn] - extracting data from {domain}, code 202, you are probably rate-limited, skipping {u}")
                continue
    
        soup = BeautifulSoup(content.text, 'html.parser')
        if "web.archive" in domain:
            data.extend( web_archive_parser(soup, u) )
        elif "mercadolibre" in domain:
            data.extend( mercado_libre_parser(soup, u) )
        elif "casasyterrenos" in domain:
            data.extend( casas_y_terrenos_parser(soup, u) )
        else:
            log(f"[Error] - scanning domain, {domain} not implemented")
        
    log(f"[Info] - Success extracting {len(data)} data from valid sources")
    return data
    
def mercado_libre_parser(soup, url):
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
            ID = house["metadata"]["id"]#house["unique_id"]
            permalink = house["metadata"]["url"]
            name = house["pictures"]["sanitized_title"]
            
            house = house["components"]
            for i in range(len(house)):
                if house[i]["type"] == 'headline':             
                    info_sale = house[i]["headline"]["text"]
                elif house[i]["type"] == 'price':                    
                    price = house[i]["price"]["current_price"]["value"]
                elif house[i]["type"] == 'attributes_list':             
                    info_rooms = house[i]["attributes_list"]["texts"]
                elif house[i]["type"] == 'location':             
                    info_location = house[i]["location"]["text"].split(",")
        except:
            log("[Error] - extracting data from house with url {url}")
            continue
        
        if "casa" in info_sale.lower():
            tipo = "casa"
        elif "departamento" in info_sale.lower():
            tipo = "departamento"
        else:
            tipo = "na"
            
        if "renta" in info_sale.lower() or "renta" in url:
            sale = "rent"
        elif "venta" in info_sale.lower() or "venta" in url:
            sale = "sale"
        else:
            sale = "na"
        
        bathrooms = None; size = None; rooms = None
        for item in info_rooms:
            if "recámaras" in item:
                rooms = item.split(" ")[0]
            elif "construidos" in item:
                size = item.split(" ")[0]
            elif "baño" in item:             
                bathrooms = int(item.split(" ")[0])

        location = None
        for k in range(len(info_location)):
            if info_location[k].strip().lower() == "guadalajara":
                location = info_location[k-1].strip()
                break
        
        data.append({"ID":ID, "name":name, "price":price, "rooms":rooms, "bathrooms":bathrooms, "size":size, "type":tipo, "sale":sale, "location":location, "year":int(datetime.now().strftime("%Y")), "url":url, "permalink":permalink})
    return data    

def casas_y_terrenos_parser(soup, url):
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
        permalink = "www.casasyterrenos.com" + house["canonical"]
        if bool(house["isSale"]):
            price = house["priceSale"]
            sale = "sale"
        elif bool(house["isRent"]):
            price = house["priceRent"]
            sale = "rent"
        else:
            continue
        data.append({"ID":str(house["id"]), "name": house["name"], "price":price, "rooms": house["rooms"], "bathrooms":house["bathrooms"], "size":house["construction"], "type":tipo, "sale":sale, "location":house["neighborhood"], "year": int(house["lastUpdate"][:4]), "url":url, "permalink":permalink })
    return  data  

def web_archive_parser(soup, url):
    '''
    Function to parse the HTML returned by wayback machine with JSON and re.
    It works with all the different sources tested

    Parameters
    ----------
    content : HTML returned by requests.request

    Returns
    -------
    A list of dictionaries with all the data that could be extracted.

    '''
    data = []
    year = int(url.split("/")[4][:4])
    
    # Section to extract data from Mercado libre's website format after 2021
    if "mercadolibre" in url and year > 2021:
        script = soup.find("script", text=re.compile("__PRELOADED_STATE__")).string  
        match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*({.*?});', script, re.DOTALL)
    
        if match:
            json_str = match.group(1)
        else:
            log("[Error] - extracting data from web archive mercado libre")
            raise Exception()
        
        source = json.loads(match.group(1))["initialState"]["results"]
        
        for house in source:
            name = house["subtitles"]["item_title"]

            if "departamento" in house["subtitles"]["operation"].lower() or "departamento" in house["title"].lower():
                tipo = "departamento"
            elif "casa" in house["subtitles"]["operation"].lower() or "casa" in house["title"].lower():
                tipo = "casa"
            else:
                tipo = None
            if "renta" in house["subtitles"]["operation"].lower() or "renta" in house["title"].lower():
                sale = "rent"
            elif "venta" in house["subtitles"]["operation"].lower() or "venta" in house["title"].lower():
                sale = "sale"
            else:
                sale = None
            
            location = None
            info_location = house["location"].split(",")
            for j in range(len(info_location)):
                if info_location[j].strip().lower() == "guadalajara":
                    location = info_location[j-1].strip()
                    break
                
            data.append({"ID":house["id"], "name": name,"price": house["price"]["amount"],"rooms":house["descriptions"][1]["label"].split(" ")[0],"bathrooms":None,"size":house["descriptions"][0]["label"].split(" ")[0],"type":tipo,"sale":sale,"location":location, "year": year, "url":url, "permalink": house["permalink"]})
    
    # Section to extract data from Mercado libre's website format before 2021
    elif "mercadolibre" in url:
        try:
            source = soup.find_all('div',class_='rowItem item item--grid new') 
            
            for house in source:
                ID = house.find('div',class_='images-viewer')["item-id"]
                name = house.find('img')["alt"]
                permalink = house.find('a')["href"]
                price = house.find("span", class_="price-fraction").contents[0] # String, need to fix
                info = house.find("div",class_="item__attrs").contents[0]
                size = info.split("|")[0].strip().split(" ")[0]
                try:
                    rooms = info.split("|")[1].strip().split(" ")[0]
                except IndexError:
                    rooms = None
                
                if "departamento" in house.find("p",class_="item__info-title").contents[0].lower() or "departamento" in name.lower():
                    tipo = "departamento"
                elif "casa" in house.find("p",class_="item__info-title").contents[0].lower() or "casa" in name.lower():
                    tipo = "casa"
                else:
                    tipo = None
                if "renta" in house.find("p",class_="item__info-title").contents[0].lower() or "renta" in name.lower():
                    sale = "rent"
                elif "venta" in house.find("p",class_="item__info-title").contents[0].lower() or "venta" in name.lower():
                    sale = "sale"
                else:
                    sale = None
                    
                location = house.find("div", class_="item__title").contents[0].strip().split("-")[0].strip()
                data.append({"ID":ID, "name": name,"price": price,"rooms":rooms,"bathrooms":None,"size":size,"type":tipo,"sale":sale,"location":location, "year": year, "url":url, "permalink": permalink})
        except:
            log(f"[Error] - extracting from mercado libre year {year}, the assumptions related to the website's structure at that year are probably wrong, this was tested with late 2017, 2022 and 2025 data")
    
    # Section to extract data from Inmuebles24 website format after 2020
    elif "inmuebles24" in url and year >= 2020:
        baseUrl = "https://web.archive.org"
        names = soup.find_all("h2", class_="postingCardTitle")
        prices = soup.find_all("div" ,class_="firstPriceContainer") if len(soup.find_all("div" ,class_="firstPriceContainer")) > 0 else soup.find_all("span", class_ = "firstPrice")
        sizes = soup.find_all("div", class_="postingCardRow postingCardMainFeaturesBlock go-to-posting")
        locations = soup.find_all("span", class_ ="postingCardLocation")
        
        for j in range(len(names)):
            name = names[j].find("a", class_="go-to-posting").contents[0].strip("\t \n")
            permalink = baseUrl + names[j].find("a", class_="go-to-posting")["href"]
            ID = str(permalink.split(".")[-2].split("-")[-1])
            # needs converting to int:
            try:
                price = prices[j].find("span", class_="firstPrice").contents[0].strip("\t \n").split(" ")[-1]
            except AttributeError:
                price = prices[j]["data-price"].split(" ")[-1]

            if "casas" in url.lower() or "casa" in name.lower():
                tipo = "casa"
            elif "departamentos" in url.lower() or "departamento" in name.lower():
                tipo = "departamento"
            else:
                tipo = None

            if "renta" in name.lower() or "renta" in url:
                sale = "rent"
            elif "venta" in name.lower() or "venta" in url:
                sale = "sale"
            else:
                sale = None
                
            rooms = None; size = None; bathrooms = None
            for item in sizes[j].find_all("li"):
                if "Bedrooms" in str(item.contents[1]):
                    rooms = item.contents[2].strip("\t \n").split(" ")[0]
                elif "Area" in str(item.contents[1]):
                    size = item.contents[2].strip("\t \n").split(" ")[0]
                elif "Bathrooms" in str(item.contents[1]):
                    bathrooms = int(item.contents[2].strip("\t \n").strip("baños").strip("\n \t"))

            
            location = soup.find_all("span", class_ ="postingCardLocation")[j].find("span").contents[0].split(",")[0].strip()
            data.append({"ID":ID, "name":name, "price":price, "rooms":rooms, "bathrooms":bathrooms, "size":size, "type":tipo, "sale":sale, "location":location, "year":year, "url":url, "permalink":permalink})
            
    # Section to extract data from Inmuebles24 website format before 2020
    elif "inmuebles24" in url:
        classes = ["post-titulo","price price-clasificado","bottom-info","post-location dl-aviso-link"] if year >= 2017 else ["post-title","prize","post-text-pay", "noexiste"]        
        baseUrl = "https://web.archive.org"
        names = soup.find_all("h4",class_= classes[0])
        price_classes = classes[1].split()
        prices = soup.find_all("p", class_=lambda x: x and all(c in x for c in price_classes))
        if len(prices) == 0:
            price_classes = "price".split()
            prices = soup.find_all("p", class_=lambda x: x and all(c in x for c in price_classes))
            
        sizes = soup.find_all("div", class_= classes[2])
        locations = soup.find_all("div" ,class_= classes[3])
        
        if len(names) != len(prices):
            log(f"[Error] - extracting data from {url}, there are {len(names)} properties and {len(prices)} prices, skipping all the data")
            return []
        
        for j in range(len(names)):
            name = names[j].find("a")["title"]
            permalink =  baseUrl + names[j].find("a")["href"]
            ID = str(permalink.split(".")[-2].split("-")[-1])
            try:
                price = prices[j].find("span").contents[0].split(" ")[-1]
            except AttributeError:
                continue
            
            if price == "Desde" or price == "\t\t" or price == "Venta":
                price = prices[j].find("span", class_="precio-valor").contents[0].split(" ")[-1]
            
            
            if "casas" in url.lower() or "casa" in name.lower():
                tipo = "casa"
            elif "departamentos" in url.lower() or "departamento" in name.lower():
                tipo = "departamento"
            else:
                tipo = None

            if "renta" in name.lower() or "renta" in url:
                sale = "rent"
            elif "venta" in name.lower() or "venta" in url:
                sale = "sale"
            else:
                sale = None
                
            rooms = None; size = None; bathrooms = None
            if year >= 2017:
                location = locations[j].find("span").contents[0].split(",")[0]
                for item in sizes[j].find_all("li"):
                    if "recámaras" in str(item.contents).lower():
                        rooms = item.contents[0].strip("\t \n").split(" ")[0]
                    elif "construidos" in str(item.contents[1]).lower() or "totales" in str(item.contents[1]).lower():
                        size = item.contents[0].strip("\t \n").split(" ")[0]
                    elif "baños" in str(item.contents[1]).lower():
                        bathrooms = int(item.contents[0].strip("\t \n").strip("\n \t"))
            else:
                location = None
                for item in sizes[j].find_all("li"):
                    if item["class"][0] == "misc-unidades":
                        continue
                    elif item["class"][0] == "misc-habitaciones":
                        try:
                            rooms = item.contents[0].contents[0]
                        except AttributeError:
                            rooms = item.contents[0].strip("\n \t")
                    elif item["class"][0] == "misc-m2totales":
                        try:
                            size = item.contents[0].contents[0]
                        except AttributeError:
                            size = item.contents[0].strip("\n \t").split(" ")[0]
                    elif item["class"][0] == "misc-m2cubiertos":
                        size = item.contents[0].strip("\n \t").split(" ")[0]
                            
                    elif item["class"][0] == "misc-metros":
                        size = item.contents[2].contents[0]                    
                    elif item["class"][0] == "misc-banos":
                        bathrooms = int(item.contents[0])
                        
                    try:
                        int(str(size).replace(",",""))
                    except ValueError:
                        for subitem in item.contents:
                            try:
                                size = int(str(subitem.contents[0]).replace(",",""))
                            except:
                                continue
                            else:
                                break

            data.append({"ID":ID, "name":name, "price":price, "rooms":rooms, "bathrooms":bathrooms, "size":size, "type":tipo, "sale":sale, "location":location, "year":year, "url":url, "permalink":permalink})
    
    time.sleep(3.5) # Try to avoid rate-limiting
    return data

def Transformer(Extracted):
    '''
    Pipeline stage to transform data to a data analytics ready form.
    removes irrelevant entries, like non-residential rent, ensures correct data types, etc 

    Parameters
    ----------
    data : A list of dictionaries returned by Extractor()

    Returns
    -------
    A dictionary with all the data ready to be saved to the Db, pending verification of uniqueness.

    '''

    data = []
    repeated = []
    ACCENTS = {"Á":"A","É":"E","Í":"I","Ó":"O","Ú":"U"}
    
#    import pdb; pdb.set_trace()
    for index in range(len(Extracted)):
        house = Extracted[index]

        
        # Remove entry if it is repeated in the same batch, keep only one repetition
        if repeated.count(house["ID"]) < 1:
            repeated.append(house["ID"])
        else:
            continue
        
        try:
            # Capitalize location and make it consistent with other entries
            if house["year"] > 2016:
                house["location"] = house["location"].upper()
                loc = house["location"].split(" ")
                s = ""
                for j in range(len(loc)):
                    word = loc[j]
                    try:
                        int(word)
                    except ValueError:
                        pass
                    else:
                        if j > 0:
                            continue
                        
                    if word == "SECC":
                        word = "SECCION"
                    elif word.endswith("."):
                        word = word[:-1]
                        
                    if word == "FRACCIONAMIENTO" or word == "BARRIO" or word == "COLONIA" or word == "NO" or word == "NUMERO" or word == "NÚMERO" or word == "CALLE" or word == "AVENIDA" or "-" in word or "/" in word or "," in word:
                        continue
                    else:
                        s = s + " " + word
                        
                s = re.sub(r'[ÁÉÍÓÚ]', lambda m: ACCENTS[m.group()], s)
                house["location"] = s.strip(" ")
                
            if house["location"] == "" or house["location"] == None:
                house["location"] = "LOCATION NOT SPECIFIED"
            elif house["location"] == "LAFAYETTE":
                house["location"] = "CHAPULTEPEC"
                
            # Capitalize sale
            house["sale"] = house["sale"].upper()
            
            # Translate
            if "casa" in house["type"].lower():
                house["type"] = "HOUSE"
            elif "departamento" in house["type"].lower():
                house["type"] = "APARTMENT"
            else:
                continue
            
            # Convert price to int                
            house["price"] = int(str(house["price"]).replace(",",""))
            
            # Convert rooms to int, "None" rooms will be removed           
            house["rooms"] = int(str(house["rooms"]).replace(",",""))
            
            # Convert size to int, "None" size will be removed
            if isinstance(house["size"],float):
                house["size"] = int(house["size"])
            else:
                house["size"] = int(str(house["size"]).replace(",",""))
        except:
            continue
        
        # Remove entry if it is for bussiness only, or only one room, or no price
        if ("local" in house["name"].strip().lower() and house["type"].lower() not in house["name"].strip().lower()) or house["size"] < 40 or house["rooms"] == 0 or house["price"] <= 500:
            continue
        if "renta" in house["name"].strip().lower() and "venta" in house["name"].strip().lower():
            continue
        
        data.append(house)
    return data

def Loader(data, Credentials):
    '''
    Pipeline stage to load data to an IBM db2 database.
    Prevents duplicates from being written.
    Requires db2 credentials saved in txt file whose path is the Credentials variable
    Logs information to the etl.log.

    Parameters
    ----------
    data : A list of dictionaries returned by Transformer()

    '''
    import ibm_db
    import ibm_db_dbi
    counter = 0
    PATH = os.getcwd()
    log(f"[Info] - Inside Loader")
    try:
        with open(Credentials, 'r') as f:
           credentials = json.loads(f.read())
        HOST = credentials["connection"]["db2"]["hosts"][0]["hostname"]
        PORT = credentials["connection"]["db2"]["hosts"][0]["port"]    
        USER = credentials["connection"]["db2"]["authentication"]["username"]
        PASS = credentials["connection"]["db2"]["authentication"]["password"]
        DB = credentials["connection"]["db2"]["database"]
        
    except FileNotFoundError:
        log(f"[Error] - the credentials txt file to connect to IBM db2 were not found in {Credentials}")
        return

    except:
        log(f"[Error] - Could not extract credentials from {Credentials}. You need to create service credentials on the db2 website and copy the long text as is.")
        return
        
    STRING = (
        f"DATABASE={DB};"
        f"HOSTNAME={HOST};"
        f"PORT={PORT};"
        f"PROTOCOL=TCPIP;"
        f"UID={USER};"
        f"PWD={PASS};"
        "SECURITY=SSL"
        )
   
    try:
        ibm_conn = ibm_db.connect(STRING,'','')
        conn = ibm_db_dbi.Connection(ibm_conn)
        cursor = conn.cursor()
    except Exception as e:
        log(f"[Error] - Couldn't connect to database: {e}")
        return
    
    SELECT = "SELECT HouseID FROM Housing"
    INSERT = "INSERT INTO Housing (HouseID, Name, Price, Rooms, Bathrooms, Size, Type, Sale, Location, Year, Url, Permalink) VALUES (?,?,?,?,?,?,?,?,?,?,?,?);"
    cursor.execute(SELECT)
    IDs = [row[0] for row in cursor.fetchall()]
    
    for house in data:
        if house["ID"] in IDs:
            continue
        try:
            prepInsert = ibm_db.prepare(ibm_conn, INSERT)
            ibm_db.execute(prepInsert, tuple(house.values() ))
        except Exception as e:
            ibm_db.rollback(ibm_conn)
            print(house.values())
            if "a row does not satisfy the check constraint" not in str(e):
                log(f"[Error] - Couldn't write entry to connected database table (Housing): {e}")
            continue
        else:
            ibm_db.commit(ibm_conn)
            counter = counter + 1
            subprocess.run(f"touch {PATH}/History.sql", shell = True)
            with open(PATH + "/History.sql",'a') as f:
                f.write(f"INSERT INTO Housing (HouseID, Name, Price, Rooms, Bathrooms, Size, Type, Sale, Location, Year, Url, Permalink) VALUES {tuple(house.values())}; \n")
        
    log(f"[Info] - Success Saving {counter} unique new entries to IBM db2 out of {len(data)} valid, transformed data.")
    ibm_db.close(ibm_conn)
    return

if __name__ == "__main__":
    Extracted = Extractor(Urls)
    Transformed = Transformer(Extracted)
    Loader(Transformed, Credentials)
