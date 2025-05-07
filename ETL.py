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
    Pipeline stage to scrape the rent and sell house/department data

    Parameters
    ----------
    urls : list with urls to scrape from the same domain.

    Returns
    -------
    A dictionary with all the data that could be extracted.
    '''
    
    domain = "u.split('/')[2]"
    for u in urls:
        content = request("get", u)
    
    if content.ok:
        log(f"Success extracting data from {domain}")
    elif content.status_code == 403:
        log(f"Error extracting data from {domain}, code 403, IP most likely blocked")
        return
    else:
        log(f"Error extracting data from {domain}, the server returned code {content.status_code}")
        return
        
    soup = BeautifulSoup(content.text, 'html.parser')
    

#