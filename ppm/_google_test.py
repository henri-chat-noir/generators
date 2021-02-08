import json
import googlemaps as gm
from datetime import datetime
from urllib import parse, request
import requests

import globals as glob
from globals import SUB_GOOGLE

api_key = 'AIzaSyAwxSgeATleocZNTkyE3zNH1HYqtf9eKmI'
gmaps_client = gm.Client(key=api_key)

# Look up an address with reverse geocoding
# reverse_geocode_result = gmaps.reverse_geocode((40.714224, -73.961452))

def places(search_text):

    type = 'establishment'

    lat = 55.6848762
    lon = 12.6231738
    lat_lon = (lat, lon)

    radius = 500

    return_dict = gmaps_client.places(search_text, location=None, radius=None, type=None)
    search_status = return_dict['status'] # OK, ZERO_RESULTS
    print(f"status_code: {search_status}")
    candidates = return_dict['results']

    if search_status == "OK":

        for num, candidate in enumerate(candidates):

            places_dict_fn = f"places_dict-{num}.json"
            places_dict_spec = glob.set_path(places_dict_fn, SUB_GOOGLE)
            with open(places_dict_spec, mode='w', encoding='UTF-8') as file_handle:
                json.dump(candidate, file_handle, indent=4)

            name = candidate['name']
            print(f"{num}. {name}")

    return


def proximity_search(lat_lon=None, query=None, radius=None):

    # https://maps.googleapis.com/maps/api/place/textsearch/json?key=AIzaSyAwxSgeATleocZNTkyE3zNH1HYqtf9eKmI&query=power%20station&location=55.6848762,12.6231738&radius=500

    # key=AIzaSyAwxSgeATleocZNTkyE3zNH1HYqtf9eKmI&query=power%20station&location=55.6848762,12.6231738&radius=500

    lat = 55.6848762
    lon = 12.6231738
    lat_lon_str = f"{lat},{lon}"

    api_call_prefix = r'https://maps.googleapis.com/maps/api/place/textsearch/json?'
    params = {  'key': api_key,
                'query': "power station",
                'location': lat_lon_str,
                'radius': 500
            }

    param_string = parse.urlencode(params, quote_via=parse.quote)

    get_fields = ['place_id', 'name', 'formatted_address', 'geometry', 'types'] # Applicable to find_place only

    # find_place_dict = gmaps_client.find_place([search_text], input_type='textquery', fields=get_fields)
    
    # return from find_place client._request("/maps/api/place/findplacefromtext/json", params)

    
    url = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
    url = r"https://maps.googleapis.com/maps/api/place/textsearch/json?key=AIzaSyAwxSgeATleocZNTkyE3zNH1HYqtf9eKmI&query=power%20station&location=55.6848762,12.6231738&radius=5000"

    with request.urlopen(url) as url:
        return_dict = json.loads(url.read().decode())
        print(return_dict)

    # return_dict = requests.get(url)
    request_status = return_dict['status'] # OK, ZERO_RESULTS
    print(f"status_code: {request_status}")
    candidates = return_dict['results']

    if request_status == "OK":

        for num, candidate in enumerate(candidates):

            find_dict_fn = f"proximity_dict-{num}.json"
            find_dict_spec = glob.set_path(find_dict_fn, SUB_GOOGLE)
            with open(find_dict_spec, mode='w', encoding='UTF-8') as file_handle:
                json.dump(candidate, file_handle, indent=4)

    return


def find_place(search_text):

    get_fields = ['place_id', 'name', 'formatted_address', 'geometry', 'types'] # Applicable to find_place only

    find_place_dict = gmaps_client.find_place([search_text], input_type='textquery', fields=get_fields)
    find_status = find_place_dict['status'] # ZERO_RESULTS
    print(f"status_code: {find_status}")
    candidates = find_place_dict['candidates']

    if find_status != "ZERO_RESULTS":

        for num, candidate in enumerate(candidates):

            find_dict_fn = f"find_dict-{num}.json"
            find_dict_spec = glob.set_path(find_dict_fn, SUB_GOOGLE)
            with open(find_dict_spec, mode='w', encoding='UTF-8') as file_handle:
                json.dump(candidate, file_handle, indent=4)

    return

def get_details(place_id):

    get_fields = ['address_component', 'formatted_address', 'geometry', 'name', 'photo', 'place_id', 'type', 'url', 'vicinity']

    place_details = gmaps_client.place(place_id, fields=get_fields, language='da')
    find_dict_fn = f"find_dict-{place_id}.json"
    find_dict_spec = glob.set_path(find_dict_fn, SUB_GOOGLE)
    with open(find_dict_spec, mode='w', encoding='UTF-8') as file_handle:
        json.dump(place_details, file_handle, indent=4)

    print(place_details)

    return

def geocode(search_text):

    geo_result = gmaps_client.geocode(search_text)
    if len(geo_result) > 0:
        geo_dict = geo_result[0]

        geo_dict_fn = "geo_dict.json"
        geo_dict_spec = glob.set_path(geo_dict_fn, SUB_GOOGLE)

        with open(geo_dict_spec, mode='w', encoding='UTF-8', language='da') as file_handle:
            json.dump(geo_dict, file_handle, indent=4)

    return


# search_text = '1600 Amphitheatre Parkway, Mountain View, CA'
search_text = 'Klingenberg CHP Coal Power Plant Germany'
search_text = 'Freudenau hydroelectric dam, Austria'
search_text = 'Ottensheim-Wilhering hydroelectric dam, Austria'
search_text = 'AKW - AG KW Wägital hydroelectric dam, Switzerland'
search_text = 'Amercoeur 1 R TGV gas-fired power station, Belgium'
search_text = 'Hkw Merkenich gas-fired power station, Germany'
search_text = 'Nürnberg Sandreuth, Germany'
search_text = 'Aceca Uf, Spain'
search_text = 'Cabot Ravenna Cte, italy'
search_text = "New York, NY"
search_text = 'Järnvägsforsen, Sweden'
search_text = "Amagerværket power station"
search_text = "power station"

search_text = 'Amagervaerket power station'
search_text = 'Lamarmora, Italy'
# find_place(search_text)
# proximity_search()
# places(search_text)

place_id = "ChIJxWD4QKp1TUYRNHyWZxviKFI"
get_details(place_id)