"""
Created on Wed May  8 11:49:10 2025

@author: Covariant Joe


Apache Airflow pipeline to web scrape house market data in Guadalajara.
Performs the ETL process, saving the data to a warehouse and an inflation data mart.

"""

from bs4 import BeautifulSoup
from requests import request
from datetime import datetime
import os
import subprocess
import json

# ------------- Configure -------------
N_PAGES = 1  # How many pages to request per url
#------------- ------------- -------------

Url1 = "https://www.casasyterrenos.com/buscar/jalisco/guadalajara/casas-y-departamentos/venta?desde=0&hasta=1000000000&utm_source=results_page"
Url2 = "https://www.casasyterrenos.com/buscar/jalisco/guadalajara/casas-y-departamentos/renta?desde=0&hasta=1000000000&utm_source=results_page"
Url3 = "https://inmuebles.mercadolibre.com.mx/departamentos/renta/jalisco/guadalajara/"
Url4 = "https://inmuebles.mercadolibre.com.mx/departamentos/venta/jalisco/guadalajara/"
Url5 = "https://inmuebles.mercadolibre.com.mx/casas/venta/jalisco/guadalajara/"
Url6 = "https://inmuebles.mercadolibre.com.mx/casas/renta/jalisco/guadalajara/"

#_Desde_145_NoIndex_True

website1 = [Url1, Url2]
website2 = [Url3, Url4, Url5, Url6]

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
    Pipeline stage to scrape the rent and sale house/department data

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
    
    if content.ok:
        log(f"Success extracting data from {domain}")
    elif content.status_code == 403:
        log(f"Error extracting data from {domain}, code 403, IP most likely blocked")
        return
    else:
        log(f"Error extracting data from {domain}, the server returned code {content.status_code}")
        return
    
    if "mercadolibre" in domain:
        data.append( mercado_libre_parser(urls, content.text) )
    elif "casasyterrenos" in domain:
        data.append( casas_y_terrenos_parser(urls, content.text) )
    else:
        log(f"Error parsing, domain {domain} not implemented")
    
    return data
    
def mercado_libre_parser(content):
    data = []
    soup = BeautifulSoup(content.text, 'html.parser')
    #info = soup.find_all("script")[-2]
    #source = json.loads(info.get_text)["@graph"]
    source = json.loads(soup.find_all("script")[3].get_text())["pageState"]["initialState"]["results"]
    
    for house in source:
        try:
            house = house["polycard"]
            ID = house["unique_id"]
            url = house["url"]
            name = house["pictures"]["sanitized_title"]
            price = house["components"][3]["price"]["current_price"]["value"]
            info_sale = house["components"][0]["headline"]["text"]
            info_rooms = house["components"][4]["attributes_list"]["texts"]
            info_location = house["components"][5]["locations"]["text"]
        except:
            continue
        
        if "casa" in info_sale.lower():
            tipo = "casa"
        elif "departamento" in info_sale.lower():
            tipo = "departamento"
        else:
            tipo = "NaN"
            
        if "renta" in info_sale.lower():
            sale = "rent"
        elif "venta" in info_sale.lower():
            sale = "sale"
        else:
            sale = "NaN"
        
        rooms = int(info_rooms[0].split(" ")[0])
        bathrooms = int(info_rooms[1].split(" ")[0]) 
        size = int(info_rooms[2].split(" ")[0])
        location = info_location.split(",")[0]
        
        data.append({"ID":ID, "name":name, "price":price, "rooms":rooms, "bathrooms":bathrooms, "size":size, "type":tipo, "sale":sale, "location":location, "url":url})
    return data    

def casas_y_terrenos_parser(content):
    return    
#