import pandas as pd
import json
import requests
import math
import urllib.request
from bs4 import BeautifulSoup as BS
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt


import pandas as pd


def extract_doctor_card(npi):
    req = requests.get(configure_npi_request(npi))
    result = req.json()["results"][0]
    doctor = {"name": [], "npi": npi, "credential": [], "enumeration_date": [], "taxonomy": [], "zipcode": [], "address": [], "address_purpose": [], "phone": []}

    if result["enumeration_type"] == "NPI-1":
        doctor["name"] = result["basic"]["first_name"] + " " + result["basic"]["last_name"]
        doctor["credential"] = result["basic"].get("credential", None)

    else:
        doctor["name"] = result["basic"]["organization_name"]
        doctor["credential"] = "organization"
        
    doctor["enumeration_date"] = result["basic"]["enumeration_date"]
    doctor["taxonomy"] = result["taxonomies"][0]["desc"]

    addresses = result["addresses"]
    for idx, addr in enumerate(addresses):
        if addr["address_purpose"] == "LOCATION":
            break
    doctor["zipcode"] = result["addresses"][idx]["postal_code"][:5]
    doctor["address"] = result["addresses"][idx]["address_1"]
    doctor["address_purpose"] = result["addresses"][idx]["address_purpose"]
    doctor["phone"] = result["addresses"][idx].get("telephone_number", None)
    return doctor

def configure_npi_request(npi_id):
    request_str = f"https://npiregistry.cms.hhs.gov/api/?version=2.1&number={npi_id}"
    return request_str


def getHaversineDistance(p1, p2):
    R = 6378137; # Earth’s mean radius in meter
    dLat = rad(p2[0] - p1[0]);
    dLong = rad(p2[1] - p1[1]);
    a = (math.sin(dLat / 2) * math.sin(dLat / 2) + math.cos(rad(p1[0])) * math.cos(rad(p2[0])) * 
         math.sin(dLong / 2) * math.sin(dLong / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = R * c #
    return d # // returns the distance in meter

def meters2miles(meters):
    return meters * 0.000621371

def rad(x):
    return x * math.pi / 180.0;

def get_lat_long(zipcode):
    uri = f'https://public.opendatasoft.com/api/records/1.0/search/?q={zipcode}&dataset=georef-united-states-of-america-zcta5'
    records = requests.get(uri).json()["records"]
    if len(records) == 0:
        return None
    return records[0]["fields"]["geo_point_2d"]

def get_distance(doctor_zipcode, patient_zipcode):
    p_latlong = get_lat_long(patient_zipcode)
    d_latlong = get_lat_long(doctor_zipcode)
    
    if d_latlong is None or p_latlong is None:
        return None
    d = getHaversineDistance(p_latlong, d_latlong)
    return meters2miles(d)
    
def find_matching_relations(procedure_name, price_df):
    find_matches = []
    for i, n in enumerate(price_df.name):
        if n == procedure_name:
            find_matches.append(True)
        else:
            find_matches.append(False)

    matches = price_df.loc[find_matches]
    print(f"found {len(matches)} price points")
    return matches

def get_cpt_name(cpt_code):
    url = f"https://www.aapc.com/codes/cpt-codes/{cpt_code}"
    html = urllib.request.urlopen(url).read()
    soup = BS(html)
    cpt_procuedure = soup.title.split("-")[1][1:-1]
    return cpt_procuedure
    
def get_icd10x_code(icd_code):
    url = f"https://www.icd10data.com/search?s={icd_code}"
    html = urllib.request.urlopen(url).read()
    soup = BS(html)
    diagnosis = str(soup.find_all("h2"))
    if diagnosis:
        diagnosis = diagnosis.split(">")[1].split("<")[0]
    else:
        print("code failure")
    return diagnosis

def get_icd9_code(code):
    url = f"https://www.icd10data.com/Convert/{code}"
    html = urllib.request.urlopen(url).read()
    soup = BS(html)
    component = str(soup.find_all("ul", {"class":"ulConversion"}))
    diagnosis = component.split("</a>")[1].split("<")[0][1:]
    return diagnosis

def iterate_for_df(parser, df_row, iter_step_size):

    procedure_keys = ['negotiation_arrangement', 'name', 'billing_code_type', 'billing_code_type_version', 'billing_code', 'description']
    negotiated_rate_keys = ['negotiated_rate', 'expiration_date', 'provider_references', 'negotiated_type', 'billing_class']


    main_df = pd.DataFrame()
    procedure_df = pd.DataFrame()
    procedure_dict = dict(df_row)
    for i in range(iter_step_size):
        prefix, event, value = next(parser)
        if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item.service_code.item":
            continue
        # print(prefix, event, value)

        for k in procedure_keys:

            if prefix == f"in_network.item.{k}":
                df_row[k] = [value]
                continue

        if prefix == "in_network.item.negotiated_rates":
            if event == "end_array" or event == "start_array":
                procedure_dict = dict(df_row)

        if prefix == "in_network.item.negotiated_rates.item":
            if event == "start_map":
                provider_references_list = []
            if event == "end_map":
                provider_references_list = []

        if procedure_dict["name"] != [None]:
            for k in negotiated_rate_keys:

                if k == "provider_references":
                    if prefix == f"in_network.item.negotiated_rates.item.{k}.item":
                        provider_references_list.append(value)
                        s = json.dumps(provider_references_list)
                        procedure_dict[k] = [s]

                else:
                    if prefix == f"in_network.item.negotiated_rates.item.negotiated_prices.item.{k}":
                        procedure_dict[k] = [value]

            if prefix == f"in_network.item.negotiated_rates.item.negotiated_prices.item.billing_class":
                procedure_df = pd.DataFrame(procedure_dict)
                # print(procedure_dict, procedure_df, prefix)

                main_df = pd.concat((main_df, procedure_df))

    return main_df
