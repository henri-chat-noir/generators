import re
import json
from datetime import datetime
import math
import pandas as pd
import Levenshtein as lev

import googlemaps as gm
from geopy.geocoders import Nominatim 

import globals as glob
from globals import COUNTRY_MAP, SUB_GOOGLE
import file_handling as fh

place_types_spec = glob.package_data('place_types.csv')
place_types_df = pd.read_csv(place_types_spec, encoding='UTF-8')
place_type_black_list = set( place_types_df[place_types_df.black_list == 1].label )

esource_id_spec = glob.package_data('esource_ids.csv')
esource_id_df = pd.read_csv(esource_id_spec)
plant_suffix_dict = dict( zip(esource_id_df['esource_id'], esource_id_df['plant_suffix']) )
    
country_name_lookup_df = COUNTRY_MAP.set_index('name')

def _fetch_pygeo_data(lat, lon):

    geolocator = Nominatim(user_agent="geoapiExercises") 
    location = geolocator.reverse( f"{lat},{lon}") 
    pygeo_dict = location.raw['address'] 

    return pygeo_dict

def _gmap_search(search_text, search_method, region=None):

    api_key = 'AIzaSyAwxSgeATleocZNTkyE3zNH1HYqtf9eKmI'
    gmaps_client = gm.Client(key=api_key)

    if search_method == 'find_place':
        get_fields = ['place_id', 'name', 'formatted_address', 'geometry', 'types'] # Applicable to find_place API only
        found_places_dict = gmaps_client.find_place([search_text], input_type='textquery', fields=get_fields)
        find_status = found_places_dict['status']

        if find_status == 'OK':
            search_status = "OK"
            found_candidates = found_places_dict['candidates']   
            
        elif find_status == 'ZERO_RESULTS':
            search_status = "ZERO"

        else: # "OK" and "ZERO_RESULTS" only codes discovered so far
            stop = True
          
    elif search_method == 'geocode':
        found_candidates = gmaps_client.geocode(search_text, region=region)
        if len(found_candidates) == 0:
            search_status = 'ZERO'
        else:
            search_status = 'OK'

    if search_status != "ZERO":
        num_results = len(found_candidates)
        if num_results == 1: # If only one result, nothing further to do, as best going to get
            call_info = found_candidates[0].copy()

        # When mulitiple results, first listed isn't necessarily the best,
        # e.g. "Farfa, Italy" hits on the church there, before the small village itself
        else: 
            for candidate in found_candidates:
                good_hit = False
                candidate_types = set(candidate['types'])
                bad_types = candidate_types.intersection(place_type_black_list)
                if len(bad_types) == 0: # Store value and stop iterating through candidates, so first possible entry stored
                    call_info = candidate.copy()
                    good_hit = True
                    break

            if not good_hit:
                search_status = "BUST" # Indicated busted search as no results have valid place_type

        if search_method == 'find_place':
            # 'name" field also bit of an issue as .name is in-built Pandas attribute -- not good as column name
            call_info['place_name'] = call_info.pop('name') # Essentially rename less-specific 'name' field early on

        elif search_method == 'geocode':
            call_info['place_name'] = "" # geocode does not return a 'name' field
            call_info.pop('address_components') # Normalize data structures by removing this field

        lat = call_info['geometry']['location']['lat']
        call_info['gm_lat'] = lat

        lon = call_info['geometry']['location']['lng']
        call_info['gm_lon'] = lon

        place_id = call_info['place_id']
        # https://www.google.com/maps/place/?q=place_id:ChIJp4JiUCNP0xQR1JaSjpW_Hms
        gm_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
        call_info['gm_url'] = gm_url
        call_info.pop('geometry')

    else: # search_status = "ZERO"
        num_results = 0
        call_info = {}
        call_info['place_id'] = None
        call_info['place_name'] = ""
        call_info['formatted_address'] = ""
        call_info['gm_lat'] = float('nan')
        call_info['gm_lon'] = float('nan')
        call_info['types'] = []
        call_info['gm_url'] = ""

    call_info['search_text'] = search_text
    call_info['search_method'] = search_method
    call_info['search_status'] = search_status
    call_info['num_results'] = num_results

    if call_info.get('place_name') is None:
        stop = True

    return call_info

def _test_search(call_info, test_string, country=None, min_score=0.85):

    def is_result_in_country(country, country_code):
        
        address = call_info['formatted_address'].lower()
        if country in address:
            return True

        # If country text does not appear within formatted address, the hope is that pygeo data provided from lat/lon
        # If lat/lon also missing, then this will pass through as a False hit, and search can be investigated
        lat = call_info['gm_lat']
        lon = call_info['gm_lon']
        pygeo_data = _fetch_pygeo_data(lat, lon)
        pygeo_cc = pygeo_data['country_code']
        if pygeo_cc == country_code:
            return True
        
        return False

    if call_info['search_status'] == 'ZERO':
        return False, 0.0

    indicator_word_list = ['hydroelectric', 'power', 'station']
    address_stop_words = ['municipality']

    bottom_limit_sim_score = 0.4
    test_string = test_string.lower()

    country_code = country_name_lookup_df.loc[country].alpha_2.lower() # Look up before lower-casing country
    country = country.lower()
        
    # Trapping for nullstring in saved csv being loaded as float nan
    # Try as I might, have not been able to get to load to str dtype (yet)
    place_name = call_info['place_name']
    place_name = place_name.lower()
    place_name = re.sub(r'[^\w\s]', ' ', place_name)
    place_words = place_name.split()

    non_stop_words = []
    for place_word in place_words:
        if place_word not in indicator_word_list:
            non_stop_words.append(place_word)
    place_name = " ".join(non_stop_words)

    address = call_info['formatted_address'].lower()
    address = re.sub(r'[^\w\s]', ' ', address)
    address = address.replace(country, "")
    address_words = address.split()
    for address_word in address_words:
        if address_word in address_stop_words:
            address_words.remove(address_word)
    address = " ".join(address_words)

    returns_text = place_name + " " + address
    search_returns = (place_name, address)
    sim_score = 0
    for return_text in search_returns:
        if return_text is not None: # Geocode searches do not return 'name' field
            score = lev.ratio(return_text, test_string.lower() )
            if score > sim_score: sim_score = score
                
    if sim_score > min_score:
        sim_test = True
    else:
        sim_test = False
                
    return_words = set(place_words + address_words)
    test_words = set( test_string.split() )
    same_words = return_words.intersection(test_words)
    if len(same_words) != 0:
        same_test = True
    else:
        same_test = False

    if not(sim_test or same_test):
        return False, sim_score

    # Not withstanding possible pass based on 'same word', test that at least > than bottom-level sim_score
    if sim_score < bottom_limit_sim_score:
        return False, sim_score

    if "Famagosta" in call_info['place_name']:
        stop = True

    place_types = call_info['types']
    if place_types:
        for label in place_types:
            if label in place_type_black_list:
                return False, sim_score

    country_ok = is_result_in_country(country, country_code)
    if not country_ok:
        return False, sim_score

    return True, sim_score

def _get_search_info(projectID, row_vals):

    def search_keywords_individually(search_api_calls):

        call_info = {}
        search_word_list = keywords.split()
        results = []
        for search_word in search_word_list:
            search_result = {}
            search_text = f"{search_word}, {country}"
            call_info = _gmap_search(search_text, search_method='find_place')
            search_api_calls += 1
            place_ok, sim_score = _test_search(call_info, test_string=keywords, country=country, min_score=0.5)
            if not place_ok: # If failes, then test single word with geocode search variant
                search_method = 'geocode'
                search_text = search_word
                call_info = _gmap_search(search_text, search_method='geocode', region=region_code)
                search_api_calls += 1
                place_ok, sim_score = _test_search(call_info, test_string=keywords, country=country, min_score=0.5)

            search_result['search_word'] = search_word
            search_result['call_info'] = call_info
            search_result['place_ok'] = place_ok
            search_result['sim_score'] = sim_score
            results.append(search_result)

        max_sim_score = 0
        valid_results = []
        for search_result in results:
            if search_result['place_ok']:
                max_sim_score = max([max_sim_score, search_result['sim_score']])
                valid_results.append(search_result)

        for search_result in valid_results:
            if search_result['sim_score'] == max_sim_score:
                call_info = search_result['call_info']
                sim_score = max_sim_score
                place_ok = search_result['place_ok']

        search_info = call_info.copy()
        search_info['place_ok'] = place_ok
        search_info['sim_score'] = sim_score
        search_info['search_api_calls'] = search_api_calls

        return search_info

    # 1.  Start with a Google find_place search on (extended) PlantName text
    search_text = row_vals['PlantName']
    esource_id = row_vals['esource_id']
    plant_suffix = plant_suffix_dict[esource_id]
    search_text += " " + plant_suffix
        
    country = row_vals['Country']
    search_text += ", " + country
    search_method = 'find_place'
    call_info = _gmap_search(search_text, search_method)
    search_api_calls = 1
    keywords = row_vals['KeywordName']      
    place_ok, sim_score = _test_search(call_info, test_string=keywords, country=country, min_score=0.70)

    region_code = country_name_lookup_df.loc[country].ccTLD # Can be used to restrict geocode search
    
    # If insufficiently strong return, move to <keywords>, <country> search with find_place api    
    if keywords:

        if not place_ok:
            search_info = {} # Need to reset full search dictionary, so legacy values not retained from prior failed searches
            search_text = keywords
            search_text = f"{search_text}, {country}"
            call_info = _gmap_search(search_text, search_method)
            search_api_calls += 1
            place_ok, sim_score = _test_search(call_info, test_string=keywords, country=country, min_score=.5)

            search_info = call_info.copy()
            search_info['place_ok'] = place_ok
            search_info['sim_score'] = sim_score
            search_info['search_api_calls'] = search_api_calls

        # Then try cycling through keywords, searching each individually, appending country
        if not place_ok:
            search_info = search_keywords_individually(search_api_calls)
  
    place_ok = search_info['place_ok']
    if not place_ok:
        search_info['search_status'] = 'BUST'
    
    now = datetime.now()
    search_time = now.strftime("%Y-%m-%d %H:%M:%S")
    search_info['search_time'] = search_time

    return search_info

def get_place_ids(in_df, max_api_limit=None, force_refresh=False):

    """
    Routine pulls together three information sets:

    """
    out_df = in_df.copy()
    place_id_df = pd.DataFrame()

    def load_search_info_df(file_spec):

        try:
            nas_ex_nullstring =  ['#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null']
            na_val_dict = {'place_name':nas_ex_nullstring, 'formatted_address':nas_ex_nullstring}

            converters = {'types': eval}

            df = pd.read_csv(file_spec, index_col='projectID', converters=converters, na_values=nas_ex_nullstring, keep_default_na=False)
            df['place_id'].where(df['place_id']=="", None)

            search_info_df = df

        except:
            empty_index = pd.Series(name='projectID')
            search_info_df = pd.DataFrame(index=empty_index)
            # Don't want to run from here by mistake, as will re-query API potentially for existing info
            stop = True

        return search_info_df

    ds_place_info_df = pd.DataFrame()
    total_api_calls = 0

    search_ref_spec = glob.ref_data("project_search_info.csv")
    search_ref_df = load_search_info_df(search_ref_spec)    

    run_search_df = pd.DataFrame()
    for projectID, row_vals in in_df.iterrows():

        country = row_vals['Country']
        if not force_refresh and projectID in search_ref_df.index: # Load stored search_info
            saved_search_info = search_ref_df.loc[projectID]
            search_info = saved_search_info.copy()
            # Update search_info to reflect latest logic in _test_search function
            keywords = row_vals['KeywordName']      
            place_ok, sim_score = _test_search(saved_search_info, test_string=keywords, country=country, min_score=.5)

            search_info['place_ok'], search_info['sim_score'] = place_ok, sim_score
            if not place_ok:
                search_info['search_status'] = 'BUST'

            search_api_calls = 0

        else:
            search_info = _get_search_info(projectID, row_vals)
            place_ok = search_info['place_ok']
            search_api_calls = search_info['search_api_calls']
            
        # search_info keys:
        # search_text, place_id, place_name, formatted_address, gm_lat, gm_lon, types
        # place_ok, sim_score, num_results, search_method, search_status, search_api_calls

        search_info_series = pd.Series(search_info, name=projectID)
        clean_fields_to_add = ['PlantName', 'lat', 'lon']
        search_info_series = search_info_series.append( row_vals[clean_fields_to_add] )
        # for field in clean_fields_to_add:
        #   search_info_series[field] = row_vals[field]

        search_ref_df.loc[projectID] = search_info_series

        search_info_series['projectID'] = projectID
        run_search_df = run_search_df.append(search_info_series)

        place_id_fields = ['place_id', 'place_name', 'formatted_address']
        place_id_df = place_id_df.append(search_info_series[place_id_fields])
        
        plant_name = row_vals['PlantName']
        print(f"API calls for {projectID}, {plant_name}: {search_api_calls}")
        total_api_calls += search_api_calls
        if max_api_limit is not None and total_api_calls > max_api_limit:
            print("HIT MAX API LIMIT")
            break

    # place_id_df.set_index('projectID')
    out_df = pd.concat([out_df, place_id_df], axis=1)

    # Besides passing back place_id_info to calling routine, 2 files to save:
    #   - Updated (complete) stored_search_info into 'ref_data' package directory
    #   - Incremental run_search_info into Google-stage diagnostics directory

    # Search-related information (for this run only) saved into stage diagnostic directory
    ds_name = in_df.columns.name
    run_search_fn = f'search_info - {ds_name}.csv'
    run_search_spec = glob.set_diag_path(run_search_fn, SUB_GOOGLE)
    run_search_df.set_index('projectID', inplace=True)
    run_search_df.to_csv(run_search_spec, mode='w')

    search_ref_df.to_csv(search_ref_spec, mode='w')

    print(f"\n\nTotal API calls on this run: {total_api_calls}")

    return out_df


# Summarize detailed place info based on having found place_ids
# =============================================================

def _get_place_details(place_id):

    api_key = 'AIzaSyAwxSgeATleocZNTkyE3zNH1HYqtf9eKmI'
    gmaps_client = gm.Client(key=api_key)

    """
    address_component
    adr_address
    business_status
    formatted_address
    geometry
    icon
    name
    permanently_closed
    photo
    place_id
    plus_code
    type
    url
    utc_offset
    vicinity
    """
    get_fields = ['address_component', 'formatted_address',
                    'geometry', 'name', 'photo', 'place_id', 'type', 'url', 'vicinity']

    place_result = gmaps_client.place(place_id, fields=get_fields)
    info_status = place_result['status']
    gmap_place_details = place_result['result']
    
    place_details = {}
    place_details['place_name'] = gmap_place_details.get('name', "")
    place_details['formatted_address'] = gmap_place_details['formatted_address']
    place_details['address_components'] = gmap_place_details['address_components']

    lat = gmap_place_details['geometry']['location']['lat']
    lon = gmap_place_details['geometry']['location']['lng']
    if lat == "" or lon == "":
        stop = True

    place_details['gm_lat'] = lat
    place_details['gm_lon'] = lon

    place_details['place_types'] = gmap_place_details['types']
    place_details['gm_url'] = gmap_place_details['url']
    place_details['vicinity'] = gmap_place_details.get('vicinity', "NONE")

    place_details['photos'] = gmap_place_details.get('photos')
            
    return place_details

def _summarize_place_info(info_dict):

    projectID = info_dict['projectID']
    place_summary = pd.Series(name=projectID)
    summary_fields = ['projectID', 'search_text', 'search_method', 'search_status', 'num_results', 'search_api_calls']
    summary_fields += ['place_id', 'name', 'formatted_address', 'types', 'sim_score', ]

    if info_dict['search_status'] != 'BUST':
        place_details_fields = ['gm_lat', 'gm_lon', 'gm_url', 'vicinity']
        summary_fields += place_details_fields
        
    for field in summary_fields:
        if field == 'name':
            place_summary['place_name'] = info_dict['name']
        else:
            place_summary[field] = info_dict[field]
        
    return place_summary # Pandas series return

def get_place_details(in_df):

    # Load existing stored place_details
    # If place_id in refence dictionary, no need to call place_details API
    places_dict_spec = glob.ref_data('places_dict.json')
    places_dict = fh.load_json_to_dict(places_dict_spec)


    for projectID, row_vals in in_df.iterrows():
        

        if place_id not in places_dict.keys():
            if place_id == "":
                stop = True
            place_details = _get_place_details(place_id)
            id_api_calls += 1
            places_dict[place_id] = place_details
        else:
            place_details = places_dict[place_id]


        place_id = search_info['place_id']
        id_api_calls = search_api_calls
        if place_id is not None:

            lat = place_details['gm_lat']
            lon = place_details['gm_lon']
            pygeo_data = _add_pygeo_data(lat, lon)
            
            if place_details.get('pygeo_data') is None:
                place_details['pygeo_data'] = pygeo_data
                places_dict[place_id] = place_details

            if not country_ok:
                search_info['search_status'] = 'BUST'
        else:
            place_details = {}


        place_info = dict(search_info)
        place_info['projectID'] = projectID
        place_info.update(place_details)
        
        place_summary = _summarize_place_info(place_info) # Returns a series
        ds_place_info_df = ds_place_info_df.append(place_summary, ignore_index=True)
        

    # Save amended "places.json' into ref_data directory
    with open(places_dict_spec, mode='w', encoding ='UTF-8') as save_file: 
        json.dump(places_dict, save_file, indent=4, ensure_ascii = False) 

    # Full amount of detailed place information saved into stage diagnostic directory
    ds_name = in_df.columns.name
    ds_place_info_fn = f'ds_place_info - {ds_name}.csv'
    ds_place_info_spec = glob.set_diag_path(ds_place_info_fn, SUB_GOOGLE)

    ds_place_info_df.set_index('projectID', inplace=True)
    ds_place_info_df.to_csv(ds_place_info_spec, mode='w')
    

    return place_details_df


