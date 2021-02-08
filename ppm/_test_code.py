import pandas as pd
# from ..entsoe_repo.entsoe import EntsoePandasClient
# from power_system_data.entsoe_repo.entsoe import EntsoeRawClient

import json
import globals as glob
from globals import SUB_GOOGLE, SUB_LAND

import file_handling as fh

entsoe_api_token = "b12b7b27-33eb-4aa1-8845-ca5bf29d1c2c"
client = EntsoeRawClient(api_key=entsoe_api_token)
# client = EntsoePandasClient(api_key=entsoe_api_token)

start = pd.Timestamp('20161201', tz='Europe/Brussels')
end = pd.Timestamp('20171202', tz='Europe/Brussels')
country_code = 'GB'  # United Kingdom

# xml_string = client.query_installed_generation_capacity(country_code, start, end, psr_type=None)
# xml_string = client.query_generation_per_plant(country_code, start, end, psr_type=None)

xml_string = client.query_installed_generation_capacity_per_unit(country_code, start, end, psr_type=None)

file_spec = glob.set_path('uk_capacity_by_unit_test09.xml', SUB_LAND)

with open(file_spec, 'w') as f:
    f.write(xml_string)

# methods that return XML
# client.query_day_ahead_prices(country_code, start, end)
# client.query_load(country_code, start, end)
# client.query_load_forecast(country_code, start, end)
# client.query_wind_and_solar_forecast(country_code, start, end, psr_type=None)
# # client.query_generation_forecast(country_code, start, end)
# client.query_generation(country_code, start, end, psr_type=None)
# client.query_installed_generation_capacity(country_code, start, end, psr_type=None)
# client.query_crossborder_flows(country_code_from, country_code_to, start, end)
# client.query_imbalance_prices(country_code, start, end, psr_type=None)



# methods that return ZIP
# client.query_unavailability_of_generation_units(country_code, start, end, docstatus=None)
# client.query_withdrawn_unavailability_of_generation_units(country_code, start, end)


# xml_string = client.query_day_ahead_prices(country_code, start, end)


# zip_bytes = client.query_unavailability_of_generation_units(country_code, start, end)
# with open('outfile.zip', 'wb') as f:
    # f.write(zip_bytes)



# methods that return Pandas Series
# client.query_day_ahead_prices(country_code, start=start,end=end)
# client.query_load(country_code, start=start,end=end)
# client.query_load_forecast(country_code, start=start,end=end)
# client.query_generation_forecast(country_code, start=start,end=end)

# methods that return Pandas DataFrames
# client.query_wind_and_solar_forecast(country_code, start=start,end=end, psr_type=None)
# client.query_generation(country_code, start=start,end=end, psr_type=None)
# client.query_installed_generation_capacity(country_code, start=start,end=end, psr_type=None)
# client.query_crossborder_flows('DE', 'DK', start=start,end=end)
# client.query_imbalance_prices(country_code, start=start,end=end, psr_type=None)
# client.query_unavailability_of_generation_units(country_code, start=start,end=end, docstatus=None)
# client.query_withdrawn_unavailability_of_generation_units('DE', start=start,end=end)

# client.query_installed_generation_capacity(country_code="GB", start=start,end=end, psr_type=None)
# ts = client.query_generation_per_plant(country_code="GB", start=start, end=end, psr_type=None, nett=False)
# ts = client.query_day_ahead_prices(country_code="GB", start=start, end=end)

# file_spec = glob.set_path('output_file03.csv', SUB_LAND)
# ts.to_csv(file_spec)


"""

======================================
documentType / processType () = A01 default

A44         query_day_ahead_prices                  (self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp) -> str:
A65 / A16   query_load                              (self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp) -> str:
A65 / ()    query_load_forecast                     (self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, process_type: str = 'A01') -> str:
A68 / A33   query_installed_generation_capacity     (self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, psr_type: Optional[str] = None) -> str:
A69 / ()    query_wind_and_solar_forecast           (self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, psr_type: Optional[str] = None,            process_type: str = 'A01', **kwargs) -> str:
A71 / A33   query_installed_generation_capacity_per_unit    (self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, psr_type: Optional[str] = None) -> str:
A73 / A16   query_generation_per_plant              (self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, psr_type: Optional[str] = None, **kwargs) -> str:
A75 / ()    query_generation_forecast               (self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, process_type: str = 'A01') -> str:
A75 / A16   query_generation                        (self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, psr_type: Optional[str] = None, **kwargs) -> str:


CROSS_BORDER
========================================
doctype / contract_marketagreement*

A11 / None  query_crossborder_flows                 (self, country_code_from: Union[Area, str], country_code_to: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, **kwargs) -> str:
A09 / A05   query_scheduled_exchanges               (self, country_code_from: Union[Area, str], country_code_to: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, **kwargs) -> str:
A61 / A01   query_net_transfer_capacity_dayahead    (self, country_code_from: Union[Area, str], country_code_to: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp) -> str:
A61 / A02   query_net_transfer_capacity_weekahead   (self, country_code_from: Union[Area, str], country_code_to: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp) -> str:
A61 / A03   query_net_transfer_capacity_monthahead  (self, country_code_from: Union[Area, str], country_code_to: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp) -> str:
A61 / A04   query_net_transfer_capacity_yearahead   (self, country_code_from: Union[Area, str], country_code_to: Union[Area, str], start: pd.Timestamp,end: pd.Timestamp) -> str:

* On-call to _query_cross_border in return
_query_crossborder(self, country_code_from: Union[Area, str], country_code_to: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, doctype: str, contract_marketagreement_type: Optional[str] = None) -> str:


================================================================
documentType / controlArea_Domain = area.code (from country code)

A85     query_imbalance_prices(self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, psr_type: Optional[str] = None) -> bytes:
A89     query_contracted_reserve_prices(self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, type_marketagreement_type: str, psr_type: Optional[str] = None) -> str:
A81     query_contracted_reserve_amount(self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, type_marketagreement_type: str, psr_type: Optional[str] = None) -> str:
        

UNAVAILABILITY ZIP FILES (AT SPECIFIC END POINT IN SYSTEM)
======================================================================
This endpoint serves ZIP files. The query is limited to 200 items per request.

A80         query_unavailability_of_generation_units(self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, docstatus: Optional[str] = None, periodstartupdate: Optional[pd.Timestamp] = None,            periodendupdate: Optional[pd.Timestamp] = None) -> bytes:

A77         query_unavailability_of_production_units(self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, docstatus: Optional[str] = None, periodstartupdate: Optional[pd.Timestamp] = None,            periodendupdate: Optional[pd.Timestamp] = None) -> bytes:
A78         query_unavailability_transmission(self, country_code_from: Union[Area, str], country_code_to: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, docstatus: Optional[str] = None,            periodstartupdate: Optional[pd.Timestamp] = None,            periodendupdate: Optional[pd.Timestamp] = None, **kwargs) -> bytes:

A13 / A80   query_withdrawn_unavailability_of_generation_units(self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp) -> bytes:
                doctype="A80", docstatus='A13')

            _query_unavailability(self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, doctype: str, docstatus: Optional[str] = None, periodstartupdate: Optional[pd.Timestamp] = None,
                periodendupdate: Optional[pd.Timestamp] = None) -> bytes:
            
            # ,'businessType': 'A53 (unplanned) | A54 (planned)'
        
"""


