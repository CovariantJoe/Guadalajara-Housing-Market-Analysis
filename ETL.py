"""
@author: Covariant Joe

Apache Airflow pipeline to web scrape house market data in Guadalajara.
Performs the ETL process, saving the data to a IBM db2 warehouse and an inflation data mart.
It prevents duplicates from being written to the db.

Sources are mercado libre, casas y terrenos, and inmuebles24 before they blocked scraper programs.

Credentials need to be provided in Credentials.txt to connect remotely to a IBM db2 database instance.

In order to run this program ibm_db needs to be installed, using pip for example.
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

# Houses
Url40 = "https://web.archive.org/web/20170707022710/https://inmuebles.mercadolibre.com.mx/casas/venta/jalisco/guadalajara/"
Url41 = "https://web.archive.org/web/20170707014305/https://inmuebles.mercadolibre.com.mx/casas/renta/jalisco/guadalajara/"


# --------------------------Data from 2015--------------------------
Url42 = "https://web.archive.org/web/20150928075811/http://www.inmuebles24.com/departamentos-en-venta-en-guadalajara.html"
Url43 = "https://web.archive.org/web/20150928034046/https://www.inmuebles24.com/casas-en-venta-en-guadalajara.html"
Url44 = "https://web.archive.org/web/20150926110649/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html"
Url45 = "https://web.archive.org/web/20151021063305/http://www.inmuebles24.com/departamentos-en-renta-en-guadalajara.html"

#_Desde_145_NoIndex_True
Urls = [ globals()[f"Url{i}"] for i in range(1,46) ]
#Urls = [Url14]

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
        content = request("get", u)
    
        if content.status_code == 403:
            log(f"[Error] - extracting data from {domain}, code 403, IP most likely blocked")
            return
        elif content.status_code > 400:
            log(f"[Error] - extracting data from {domain} the server returned code {content.status_code}")
            return
        elif content.status_code == 202:
            log(f"[Warn] - extracting data from {domain}, code 202, you are probably rate-limited, skipping {u}")
            continue
    
        soup = BeautifulSoup(content.text, 'html.parser')
        if "web.archive" in domain:
            data.extend( web_archive_parser(soup, u) )
        elif "mercadolibre" in domain:
            data.extend( mercado_libre_parser(soup) )
        elif "casasyterrenos" in domain:
            data.extend( casas_y_terrenos_parser(soup) )
        else:
            log(f"[Error] - parsing, domain {domain} not implemented")
        
    log(f"[Info] - Success extracting data from all the sources")
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
            ID = house["metadata"]["id"]#house["unique_id"]
            url = house["metadata"]["url"]
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
            
        if "renta" in info_sale.lower():
            sale = "rent"
        elif "venta" in info_sale.lower():
            sale = "sale"
        else:
            sale = "na"
        
        bathrooms = None; size = None; rooms = None
        for item in info_rooms:
            if "recámaras" in item:
                rooms = int(item.split(" ")[0])
            elif "construidos" in item:
                size = item.split(" ")[0]
            elif "baño" in item:             
                bathrooms = int(item.split(" ")[0])

        location = None
        for k in range(len(info_location)):
            if info_location[k].strip().lower() == "guadalajara":
                location = info_location[k-1].strip()
                break
        
        data.append({"ID":ID, "name":name, "price":price, "rooms":rooms, "bathrooms":bathrooms, "size":size, "type":tipo, "sale":sale, "location":location, "year":int(datetime.now().strftime("%Y")), "url":url, "permalink":url})
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
                
            data.append({"ID":house["id"], "name": name,"price": house["price"]["amount"],"rooms":int(house["descriptions"][1]["label"].split(" ")[0]),"bathrooms":None,"size":int(house["descriptions"][0]["label"].split(" ")[0]),"type":tipo,"sale":sale,"location":location, "year": year, "url":url, "permalink": house["permalink"]})
    
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
                rooms = int(info.split("|")[1].strip().split(" ")[0])
                
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
                    rooms = int(item.contents[2].strip("\t \n").split(" ")[0])
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
        prices = soup.find_all("p", class_= classes[1])
        sizes = soup.find_all("div", class_= classes[2])
        locations = soup.find_all("div" ,class_= classes[3])
        
        for j in range(len(names)):
            name = names[j].find("a")["title"]
            permalink =  baseUrl + names[j].find("a")["href"]
            ID = str(permalink.split(".")[-2].split("-")[-1])
            try:
                price = prices[j].find("span").contents[0].split(" ")[-1]
            except:
                a = 1
            
            if price == "Desde":
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
                        rooms = int(item.contents[0].strip("\t \n").split(" ")[0])
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
                            rooms = int(item.contents[0].contents[0])
                        except AttributeError:
                            rooms = int(item.contents[0].strip("\n \t"))
                    elif item["class"][0] == "misc-m2cubiertos":
                        try:
                            size = item.contents[0].contents[0]
                        except AttributeError:
                            size = item.contents[0].strip("\n \t").split(" ")[0]
                    elif item["class"][0] == "misc-metros":
                        size = item.contents[2].contents[0]
                    elif item["class"][0] == "misc-banos":
                        bathrooms = int(item.contents[0])

            data.append({"ID":ID, "name":name, "price":price, "rooms":rooms, "bathrooms":bathrooms, "size":size, "type":tipo, "sale":sale, "location":location, "year":year, "url":url, "permalink":permalink})
                
    return data

def Transformer(extracted):
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
    
    for index in range(len(extracted)):
        house = extracted[index]

        # Remove entry if it is for bussiness only, or only one room, or number of rooms/size is unknown
        if "local" in house["name"].strip().lower() or house["size"] <= 45 or house["rooms"] == None or house["size"] == None:
            continue
        
        # Remove entry if it is repeated in the same batch, keep only one repetition
        if repeated.count(house["ID"]) < 1:
            repeated.append(house["ID"])
        else:
            continue
        
        # Capitalize location
        house["location"] = house["location"].upper()
        
        # Convert price to int                
        house["price"] = int(str(house["price"]).replace(",",""))
        
        # Convert rooms to int           
        house["rooms"] = int(str(house["rooms"]).replace(",",""))
        
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

    except:
        log(f"[Error] - Could not extract credentials from {Credentials}. You need to create service credentials on the db2 website and copy the long text as is.")
        
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
            log(f"[Error] - Couldn't write entry to connected database table (Housing): {e}")
            ibm_db.rollback(ibm_conn)
            ibm_db.close(ibm_conn)
            return
        else:
            ibm_db.commit(ibm_conn)
            counter = counter + 1
            subprocess.run(f"touch {PATH}/History.sql", shell = True)
            with open(PATH + "/History.sql",'a') as f:
                f.write(f"INSERT INTO Housing (HouseID, Name, Price, Rooms, Bathrooms, Size, Type, Sale, Location, Year, Url, Permalink) VALUES {tuple(house.values())}; \n")
        
    log(f"[Info] - Success Saved {counter} unique new entries to IBM db2 out of {len(Transformed)} valid extracted data.")
    ibm_db.close(ibm_conn)
    return

Extracted = Extractor(Urls)
Transformed = Transformer(Extracted)
#Transformed = [{'ID': '53068734', 'name': 'Hermosa Casa de Una Planta', 'price': 28000, 'rooms': 5, 'bathrooms': 4, 'size': 351, 'type': 'casa', 'sale': 'rent', 'location': 'FRACCIONAMIENTO VALLARTA NORTE', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/hermosa-casa-de-una-planta-53068734.html'}, {'ID': '58359930', 'name': 'Residencia Enorme en Venta a 3 Cuadras de Minerva', 'price': 80000, 'rooms': 10, 'bathrooms': 5, 'size': 600, 'type': 'casa', 'sale': 'rent', 'location': 'VALLARTA PONIENTE', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/residencia-enorme-en-venta-a-3-cuadras-de-minerva-58359930.html'}, {'ID': '59724852', 'name': 'Casa 3hab + Estudio, 2 Autos Remodelada en Privada', 'price': 25000, 'rooms': 3, 'bathrooms': 2, 'size': 120, 'type': 'casa', 'sale': 'rent', 'location': 'LAFAYETTE', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-3hab-estudio-2-autos-remodelada-en-privada-59724852.html'}, {'ID': '58021334', 'name': 'Casa en Renta Regidores, Ayuntamiento', 'price': 27000, 'rooms': 5, 'bathrooms': 2, 'size': 300, 'type': 'casa', 'sale': 'rent', 'location': 'AYUNTAMIENTO', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-en-renta-regidores-ayuntamiento-58021334.html'}, {'ID': '59478923', 'name': 'Casa Amueblada en Calle Marsella, Colonia Obrera Chapultepec', 'price': 20000, 'rooms': 3, 'bathrooms': 2, 'size': 140, 'type': 'casa', 'sale': 'rent', 'location': 'AMERICANA', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-amueblada-en-calle-marsella-colonia-obrera-59478923.html'}, {'ID': '59903838', 'name': 'Casa en Renta Zona de Galería del Calzado', 'price': 22000, 'rooms': 3, 'bathrooms': 2, 'size': 119, 'type': 'casa', 'sale': 'rent', 'location': 'VALLARTA PONIENTE', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-en-renta-zona-de-galeria-del-calzado-59903838.html'}, {'ID': '56886785', 'name': 'Casa en Renta Carlos Mérida, Colinas de La Normal', 'price': 16500, 'rooms': 3, 'bathrooms': 1, 'size': 170, 'type': 'casa', 'sale': 'rent', 'location': 'COLINAS DE LA NORMAL', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-en-renta-carlos-merida-colinas-de-la-normal-56886785.html'}, {'ID': '58447219', 'name': 'Hermosa Casa en Renta', 'price': 20000, 'rooms': 4, 'bathrooms': 2, 'size': 220, 'type': 'casa', 'sale': 'rent', 'location': '8 DE JULIO', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/hermosa-casa-en-renta-58447219.html'}, {'ID': '54174688', 'name': 'Se Renta Casa en Jardines del Bosque, Av Niños Heroes', 'price': 18500, 'rooms': 4, 'bathrooms': 3, 'size': 250, 'type': 'casa', 'sale': 'rent', 'location': 'JARDINES DEL BOSQUE NORTE', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/se-renta-casa-en-jardines-del-bosque-av-ninos-heroes-54174688.html'}, {'ID': '57533414', 'name': 'Casa en Renta Calle Ramón Blancarte, Mirador', 'price': 9000, 'rooms': 3, 'bathrooms': 1, 'size': 95, 'type': 'casa', 'sale': 'rent', 'location': 'EL MIRADOR', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-en-renta-calle-ramon-blancarte-mirador-57533414.html'}, {'ID': '59472670', 'name': 'A 20 m de Av Lopez Mateos', 'price': 29500, 'rooms': 6, 'bathrooms': None, 'size': 300, 'type': 'casa', 'sale': 'rent', 'location': 'CHAPALITA', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/a-20-m-de-av-lopez-mateos-59472670.html'}, {'ID': '58430042', 'name': 'Renta de Casa Esquina en Av México', 'price': 59000, 'rooms': 8, 'bathrooms': 6, 'size': 518, 'type': 'casa', 'sale': 'rent', 'location': 'ARCOS VALLARTA', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/renta-de-casa-esquina-en-av-mexico-58430042.html'}, {'ID': '59696973', 'name': 'Casa en Renta en Colomos Providencia', 'price': 20000, 'rooms': 5, 'bathrooms': 4, 'size': 252, 'type': 'casa', 'sale': 'rent', 'location': 'FRACCIONAMIENTO COLOMOS PROVIDENCIA', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-en-renta-en-colomos-providencia-59696973.html'}, {'ID': '59882819', 'name': 'Casa Renta en El Country Club un Nivel', 'price': 22900, 'rooms': 3, 'bathrooms': 2, 'size': 225, 'type': 'casa', 'sale': 'rent', 'location': 'COUNTRY CLUB', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-renta-en-el-country-club-un-nivel-59882819.html'}, {'ID': '59580864', 'name': 'Casa Para Oficina Ubicadisima en Renta - Jardines del Bosque - Calle El Rosario', 'price': 25000, 'rooms': 4, 'bathrooms': 3, 'size': 189, 'type': 'casa', 'sale': 'rent', 'location': 'JARDINES DEL BOSQUE NORTE', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-para-oficina-ubicadisima-en-renta-jardines-del-59580864.html'}, {'ID': '59905734', 'name': 'Casa en Renta Col. Americana', 'price': 35000, 'rooms': 4, 'bathrooms': 3, 'size': 697, 'type': 'casa', 'sale': 'rent', 'location': 'AMERICANA', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-en-renta-col.-americana-59905734.html'}, {'ID': '59429614', 'name': 'Casa en Renta en Medrano', 'price': 10000, 'rooms': 4, 'bathrooms': 2, 'size': 185, 'type': 'casa', 'sale': 'rent', 'location': 'MEDRANO', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-en-renta-en-medrano-59429614.html'}, {'ID': '59385958', 'name': 'Casa en Venta / Renta Santa Tere por Enrique Diaz de León', 'price': 26700, 'rooms': 8, 'bathrooms': 4, 'size': 253, 'type': 'casa', 'sale': 'rent', 'location': 'BARRIO SANTA TERESITA', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-en-venta-renta-santa-tere-por-enrique-diaz-de-59385958.html'}, {'ID': '57585790', 'name': 'Colonia Americana Fines Comerciales', 'price': 28000, 'rooms': 2, 'bathrooms': 3, 'size': 390, 'type': 'casa', 'sale': 'rent', 'location': 'AMERICANA', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/colonia-americana-fines-comerciales-57585790.html'}, {'ID': '59181159', 'name': 'Amplia Casa en El Corazón de Santa Tere', 'price': 12000, 'rooms': 4, 'bathrooms': 2, 'size': 200, 'type': 'casa', 'sale': 'rent', 'location': 'BARRIO SANTA TERESITA', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/amplia-casa-en-el-corazon-de-santa-tere-59181159.html'}, {'ID': '58642917', 'name': 'Casa en Renta Providencia Calle Florencia', 'price': 35000, 'rooms': 4, 'bathrooms': 2, 'size': 350, 'type': 'casa', 'sale': 'rent', 'location': 'PROVIDENCIA', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-en-renta-providencia-calle-florencia-58642917.html'}, {'ID': '58260644', 'name': 'Casa Zona Chapalita, Expo Gdl. Hotel Riu', 'price': 35000, 'rooms': 5, 'bathrooms': 2, 'size': 200, 'type': 'casa', 'sale': 'rent', 'location': 'CHAPALITA', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-zona-chapalita-expo-gdl.-hotel-riu-58260644.html'}, {'ID': '59720890', 'name': 'Casa en Renta en San Juan Bosco', 'price': 6700, 'rooms': 2, 'bathrooms': 1, 'size': 150, 'type': 'casa', 'sale': 'rent', 'location': 'BARRIO SAN JUAN BOSCO', 'year': 2020, 'url': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/casas-en-renta-en-guadalajara.html', 'permalink': 'https://web.archive.org/web/20201202221017/https://www.inmuebles24.com/propiedades/casa-en-renta-en-san-juan-bosco-59720890.html'}]
Loader(Transformed, Credentials)


'''
CREATE TABLE Housing (
HouseID VARCHAR(30) NOT NULL PRIMARY KEY,
Name VARCHAR(250) NOT NULL UNIQUE,
Price INTEGER NOT NULL CHECK(Price > 0),
Rooms SMALLINT NOT NULL ,
Bathrooms SMALLINT,
Size INTEGER NOT NULL,
Type VARCHAR(12) NOT NULL,
Sale CHAR(4) CHECK(Sale = 'rent' OR Sale = 'sale'),
Location VARCHAR(75),
Year INTEGER NOT NULL CHECK(Year > 2000 AND Year < 2030),
Url VARCHAR(200) NOT NULL,
Permalink VARCHAR(200) NOT NULL
);
'''
