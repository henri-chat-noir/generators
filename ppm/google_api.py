import numbers
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

MIN_SCORE = 0.50

place_types_spec = glob.package_data('place_types.csv')
place_types_df = pd.read_csv(place_types_spec, encoding='UTF-8')
place_type_black_list = set( place_types_df[place_types_df.black_list == 1].label )

esource_id_spec = glob.package_data('esource_ids.csv')
esource_id_df = pd.read_csv(esource_id_spec)
plant_suffix_dict = dict( zip(esource_id_df['esource_id'], esource_id_df['plant_suffix']) )
    
country_name_lookup_df = COUNTRY_MAP.set_index('name')

def _fetch_pygeo_data(lat, lon):

    geolocator = Nominatim(user_agent="geoapiExercises")
    try:
        location = geolocator.reverse( f"{lat},{lon}") 
        pygeo_dict = location.raw['address'] 
    except:
        pygeo_dict = {}

    return pygeo_dict

def _save_search_results(candidates, projectID, row_vals, run_search_df):

    for search_info in candidates:
        log_info = search_info.copy()
        log_info['projectID'] = projectID
        
        log_info['PlantName'] = row_vals.PlantName
        log_info['KeywordName'] = row_vals.KeywordName
        log_info['lat'] = row_vals.lat
        log_info['lon'] = row_vals.lon

        run_search_df = run_search_df.append(log_info, ignore_index=True)

        if run_search_df.shape[0] == 0:
            stop = True

    return run_search_df

def _gmap_search(search_text, search_method, region=None):

    def parse_return(return_dict, result_key='results'):
        return_status = return_dict['status']
        if return_status == 'OK':
            search_status = "OK"
            all_results = return_dict[result_key]   
            
        elif return_status == 'ZERO_RESULTS':
            search_status = "ZERO"
            all_results = []

        else: # "OK" and "ZERO_RESULTS" only codes discovered so far, but various failure codes documented
            stop = True

        return search_status, all_results

    def load_place_info(result_candidates):

        candidates = []
        for num, result in enumerate(result_candidates):
            place_info = result.copy()
            place_info['return_pos'] = num + 1

            if search_method in {'find_place', 'places'}:
                # 'name" field also bit of an issue as .name is in-built Pandas attribute -- not good as column name
                place_info['place_name'] = place_info.pop('name') # Essentially rename less-specific 'name' field early on

            elif search_method == 'geocode':
                place_info['place_name'] = "" # geocode does not return a 'name' field
                place_info.pop('address_components') # Normalize data structures by removing this field

            if result.get('formatted_address') is None:
                place_info['formatted_address'] = ""

            lat = place_info['geometry']['location']['lat']
            place_info['gm_lat'] = lat

            lon = place_info['geometry']['location']['lng']
            place_info['gm_lon'] = lon
            place_info.pop('geometry')

            place_id = place_info['place_id']
            # https://www.google.com/maps/place/?q=place_id:ChIJp4JiUCNP0xQR1JaSjpW_Hms
            gm_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
            place_info['gm_url'] = gm_url

            plus_code = place_info.get('plus_code')
            if plus_code is not None:
                place_info['compound_code'] = plus_code['compound_code']
                place_info['global_code'] = plus_code['global_code']
                place_info.pop('plus_code', None)

            # Generally from places search, where field selection not possible

            remove_keys = ['business_status', 'icon', 'obfuscated_type', 'opening_hours',
                            'permanently_closed', 'photos', 'rating', 'reference', 'user_ratings_total']
            for del_key in remove_keys:
                place_info.pop(del_key, None)
            
            candidates.append(place_info)

        return candidates

    api_key = 'AIzaSyAwxSgeATleocZNTkyE3zNH1HYqtf9eKmI'
    gmaps_client = gm.Client(key=api_key)

    # CALL APIS AND RETURN STATUS AND RESULTS LIST
    if search_method == 'places':
        return_dict = gmaps_client.places(search_text, region=region, location=None, radius=None, type=None)
        search_status, all_results = parse_return(return_dict, result_key='results')

    elif search_method == 'find_place':
        get_fields = ['place_id', 'name', 'formatted_address', 'geometry', 'types'] # Applicable to find_place API only
        return_dict = gmaps_client.find_place([search_text], input_type='textquery', fields=get_fields)
        search_status, all_results = parse_return(return_dict, result_key='candidates')

    elif search_method == 'geocode':
        all_results = gmaps_client.geocode(search_text, region=region)
        if len(all_results) == 0:
            search_status = 'ZERO'
        else:
            search_status = 'OK'

    if search_status == "OK":
        result_candidates = []
        for result in all_results:
            good_hit = False
            result_types = set(result['types'])
            bad_types = result_types.intersection(place_type_black_list)
            if len(bad_types) == 0: # Store value
                result_candidates.append(result)
                good_hit = True
                    
        if not good_hit: # If cycle through all results and none are good, store first entry as 'better than nothing'
            result_candidates = [all_results[0]]
            search_status = "BUST" # Indicated busted search as no results have valid place_type

        candidates = load_place_info(result_candidates)
        num_candidates = len(candidates)

    else: # search_status = "ZERO"
        num_candidates = 0
        place_info = {}
        place_info['place_id'] = None
        place_info['place_name'] = ""
        place_info['formatted_address'] = ""
        place_info['gm_lat'] = float('nan')
        place_info['gm_lon'] = float('nan')
        place_info['types'] = []
        place_info['gm_url'] = ""
        place_info['return_pos'] = 0
        candidates = [place_info]
        
    # Values generic to call
    now = datetime.now()
    search_time = now.strftime("%Y-%m-%d %H:%M:%S")
    
    for search_info in candidates:
        search_info['search_text'] = search_text
        search_info['search_method'] = search_method
        search_info['search_status'] = search_status
        search_info['num_results'] = num_candidates
        search_info['search_time'] = search_time

        if search_info.get('place_name') is None:
            stop = True
        
    # place_id_list = [place_info['place_id'] for place_info in candidates]
    # call_info['place_ids'] = place_id_list

    return candidates

def _test_search(search_info, test_string, country=None):

    def is_result_in_country(search_info, lat_lon, country, country_code):
        
        address = search_info['formatted_address'].lower()
        if country in address:
            return True

        if lat_lon == (58.1159027, -4.5744921):
            return False

        # If country text does not appear within formatted address, the hope is that pygeo data provided from lat/lon
        # If no hit to lat/lon search, then this will pass through as a False hit, and search can be investigated
        pygeo_data = _fetch_pygeo_data(lat_lon[0], lat_lon[1])
        pygeo_cc = pygeo_data['country_code']
        if pygeo_cc == country_code:
            return True
        
        return False

    if search_info['search_status'] == 'ZERO':
        return False, 0.0

    indicator_word_list = ['nuclear', 'hydroelectric', 'power', 'station']
    address_stop_words = ['municipality']

    bottom_limit_sim_score = 0.4
    test_string = test_string.lower()

    country_code = country_name_lookup_df.loc[country].alpha_2.lower() # Look up before lower-casing country
    country = country.lower()
        
    # Trapping for nullstring in saved csv being loaded as float nan
    # Try as I might, have not been able to get to load to str dtype (yet)
    place_name = search_info['place_name']
    if "Gravelines" in place_name:
        stop = True

    place_name = place_name.lower()
    place_name = re.sub(r'[^\w\s]', ' ', place_name)
    place_words = place_name.split()

    non_stop_words = []
    for place_word in place_words:
        if place_word not in indicator_word_list:
            non_stop_words.append(place_word)
    place_name = " ".join(non_stop_words)

    address = search_info['formatted_address'].lower()
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
                
    if sim_score > MIN_SCORE:
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

    place_types = search_info['types'] # Only circumstance shold be if search_status = "BUST"
    if place_types:
        for label in place_types:
            if label in place_type_black_list:
                return False, sim_score

    lat_str = search_info['gm_lat']
    lon_str = search_info['gm_lon']
    if lat_str != "" and lon_str != "":
        lat = float(lat_str)
        lon = float(lon_str)
        country_ok = is_result_in_country( search_info, (lat, lon), country, country_code)
    else:
        country_ok = False

    if not country_ok:
        return False, sim_score

    return True, sim_score

def _eval_call_results(candidates, test_string, country):

    max_sim_score = 0
    best_idx = None
    revised_candidates = []
    for num, search_info in enumerate(candidates):
        # For now simple routine of returning highest sim_score amongst those that return place_ok=True
        place_ok, sim_score = _test_search(search_info, test_string=test_string, country=country)
        search_info['sim_score'] = sim_score
        search_info['place_ok'] = place_ok
        search_info['best_match'] = False
        if place_ok:
            search_info['search_status'] = "OK"
            if sim_score > max_sim_score:
                max_sim_score = max(sim_score, max_sim_score)
                best_idx = num
        else:
            search_info['search_status'] = "BUST"

        revised_candidates.append(search_info)

    if best_idx is not None and max_sim_score >= MIN_SCORE:
        revised_candidates[best_idx]['best_match'] = True
     
    return revised_candidates, best_idx

def _get_best_result(projectID, row_vals, run_search_df):

    def search_keywords_individually(search_api_calls, keywords, run_search_df):

        call_info = {}
        search_word_list = keywords.split()
        best_results = []
        all_candidates = []
        for search_word in search_word_list:
            search_text = f"{search_word}, {country}"
            candidates = _gmap_search(search_text, search_method='find_place')
            search_api_calls += 1

            keywords = row_vals['KeywordName']
            candidates, best_idx = _eval_call_results(candidates, test_string=keywords, country=country)
            run_search_df = _save_search_results(candidates, projectID, row_vals, run_search_df)

            if best_idx is None: # If fails, then test single word with geocode search variant
                search_method = 'geocode'
                search_text = search_word
                candidates = _gmap_search(search_text, search_method='geocode', region=region_code)
                search_api_calls += 1
                
                candidates, best_idx = _eval_call_results(candidates, test_string=keywords, country=country)
                run_search_df = _save_search_results(candidates, projectID, row_vals, run_search_df)

            
            if best_idx is not None:
                best_results.append(candidates[best_idx])

        best_score = 0
        if len(best_results) == 0:
            best_result = None
        else:
            for result in best_results:
                score = result['sim_score']
                if score > best_score:
                    best_score = score
                    best_result = result

        return best_result, search_api_calls, run_search_df

    # 1.  Start with a Google places search on (extended) PlantName text
    search_text = row_vals['PlantName']
    esource_id = row_vals['esource_id']
    plant_suffix = plant_suffix_dict[esource_id]
    search_text += " " + plant_suffix
        
    country = row_vals['Country']
    search_text += ", " + country
    search_method = 'places'
    region_code = country_name_lookup_df.loc[country].ccTLD # Can be used to restrict places and geocode API calls

    candidates = _gmap_search(search_text, search_method='places', region=region_code)
    search_api_calls = 1

    keywords = row_vals['KeywordName']
    if keywords == math.nan:
        stop = True
    candidates, best_idx = _eval_call_results(candidates, test_string=keywords, country=country)
    run_search_df = _save_search_results(candidates, projectID, row_vals, run_search_df)
    
    best_result = None
    # If none of current candidates met threshold, re-try places search with <keywords>, <country>
    if keywords and best_idx is None:
        search_info = {} # Need to reset full search dictionary, so legacy values not retained from prior failed searches
        search_text = keywords
        search_text = f"{search_text}, {country}"
        candidates = _gmap_search(search_text, search_method='places')
        search_api_calls += 1

        candidates, best_idx = _eval_call_results(candidates, test_string=keywords, country=country)
        run_search_df = _save_search_results(candidates, projectID, row_vals, run_search_df)

    if best_idx is not None:
        best_result = candidates[best_idx]
            
    # Failing THAT, then try cycling through keywords, searching each individually, appending country
    if keywords and best_idx is None:
        best_result, search_api_calls, run_search_df = search_keywords_individually(search_api_calls, keywords, run_search_df=run_search_df)
  
    if best_result is None:
        # Might as well just set to first, which could be empty record entry
        # Essentially stores 'junk return' in reference file to prevent
        # consistently re-trying APIs on failed search, unless force_refresh=True
        best_result = candidates[0]
    
    best_result['search_api_calls'] = search_api_calls

    return best_result, best_idx, run_search_df

def get_place_ids(in_df, max_api_limit=None, force_refresh=False):

    """
    Routine pulls together three information sets:

    """

    def load_search_results_df(file_spec):

        try:
            nas_ex_nullstring =  ['#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null']
            na_val_dict = { 'place_name': nas_ex_nullstring,
                            'formatted_address': nas_ex_nullstring,
                            'KeywordName': nas_ex_nullstring}

            converters = {'types': eval}
            df = pd.read_csv(file_spec, index_col='projectID', converters=converters, na_values=nas_ex_nullstring, keep_default_na=False)
            df['place_id'].where(df['place_id']=="", None)
            # df['KeywordName'].where(df['KeywordName']==math.nan, "")
            search_info_df = df

        except:
            empty_index = pd.Series(name='projectID')
            search_info_df = pd.DataFrame(index=empty_index)
            # Don't want to run from here by mistake, as will re-query API potentially for existing info
            stop = True

        return search_info_df

    run_search_df = pd.DataFrame()
    project_search_info_spec = glob.ref_data("project_search_info.csv")
    best_results_df = load_search_results_df(project_search_info_spec)    

    out_df = in_df.copy()
    place_id_df = pd.DataFrame()

    ds_place_info_df = pd.DataFrame()
    total_api_calls = 0
    clean_fields_to_add = ['PlantName', 'KeywordName', 'lat', 'lon']

    for projectID, row_vals in in_df.iterrows():

        country = row_vals['Country']
        if not force_refresh and projectID in best_results_df.index: # Load stored search_info

            best_result = best_results_df.loc[projectID].copy()
            best_result = best_result.drop(clean_fields_to_add)
            search_status = best_result['search_status']
            # Update search_info to reflect latest logic in _test_search function
            candidates = [best_result]
            keywords = row_vals['KeywordName']
            if search_status != "ZERO":
                candidates, best_idx = _eval_call_results(candidates, test_string=keywords, country=country)
            else:
                best_idx = None

            # Should only be single entry, one per projectID in stored best results
            if len(candidates) != 1:
                stop = True

            if best_idx is None:
                best_result['best_match'] = False
            else:
                best_result['best_match'] = True

            search_api_calls = 0

        else:
            best_result, best_idx, run_search_df = _get_best_result(projectID, row_vals, run_search_df)
            search_api_calls = best_result['search_api_calls']

        best_result_series = pd.Series(best_result, name=projectID)
        best_result_series = pd.concat( [best_result_series, row_vals[clean_fields_to_add]], axis=0)
        best_results_df.loc[projectID] = best_result_series # Apply new or modified entry back into stored dataframe

        place_id_fields = ['place_id', 'place_name', 'formatted_address']
        place_id_df = place_id_df.append(best_result_series[place_id_fields])
        
        plant_name = row_vals['PlantName']
        print(f"API calls for {projectID}, {plant_name}: {search_api_calls}")
        total_api_calls += search_api_calls
        if max_api_limit is not None and total_api_calls > max_api_limit:
            print("\nHIT MAX API LIMIT")
            print(40*"=")
            break

    out_df = pd.concat([out_df, place_id_df], axis=1)

    # Besides passing back place_id_info to calling routine, 2 files to save:
    #   - Updated (complete) stored_search_info for 'best_result' into 'ref_data' package directory
    #   - Incremental run_search_info, for all (potentially valid results), i.e. candidates, into Google-stage diagnostics directory

    # Search-related information (for this run only) saved into stage diagnostic directory
    ds_name = in_df.columns.name
    now = datetime.now()
    search_time = now.strftime("%Y-%m-%d %H_%M_%S")
  
    if run_search_df.shape[0] > 0:
        run_search_fn = f'run_info {ds_name} ({search_time}).csv'
        run_search_spec = glob.set_diag_path(run_search_fn, SUB_GOOGLE)
        # run_search_df.set_index('projectID', inplace=True)
        run_search_df.to_csv(run_search_spec, mode='w')

    best_results_df.to_csv(project_search_info_spec, mode='w')

    print(f"\nTotal API calls on this run: {total_api_calls}")

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