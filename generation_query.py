import pandas as pd
from entsoe import EntsoePandasClient
import os
DOMAIN_MAPPINGS = {
    'AL': '10YAL-KESH-----5',
    'AT': '10YAT-APG------L',
    'BA': '10YBA-JPCC-----D',
    'BE': '10YBE----------2',
    'BG': '10YCA-BULGARIA-R',
    'BY': '10Y1001A1001A51S',
    'CH': '10YCH-SWISSGRIDZ',
    'CZ': '10YCZ-CEPS-----N',
    'DE': '10Y1001A1001A83F',
    'DK': '10Y1001A1001A65H',
    'EE': '10Y1001A1001A39I',
    'ES': '10YES-REE------0',
    'FI': '10YFI-1--------U',
    'FR': '10YFR-RTE------C',
    'GB': '10YGB----------A',
    'GB_NIR': '10Y1001A1001A016',
    'GR': '10YGR-HTSO-----Y',
    'HR': '10YHR-HEP------M',
    'HU': '10YHU-MAVIR----U',
    'IE': '10YIE-1001A00010',
    'IT': '10YIT-GRTN-----B',
    'LT': '10YLT-1001A0008Q',
    'LU': '10YLU-CEGEDEL-NQ',
    'LV': '10YLV-1001A00074',
    'MD': 'MD',
    'ME': '10YCS-CG-TSO---S',
    'MK': '10YMK-MEPSO----8',
    'MT': '10Y1001A1001A93C',
    'NL': '10YNL----------L',
    'NO': '10YNO-0--------C',
    'PL': '10YPL-AREA-----S',
    'PT': '10YPT-REN------W',
    'RO': '10YRO-TEL------P',
    'RS': '10YCS-SERBIATSOV',
    'RU': '10Y1001A1001A49F',
    'RU_KGD': '10Y1001A1001A50U',
    'SE': '10YSE-1--------K',
    'SI': '10YSI-ELES-----O',
    'SK': '10YSK-SEPS-----K',
    'TR': '10YTR-TEIAS----W',
    'UA': '10YUA-WEPS-----0',
    'DE_AT_LU': '10Y1001A1001A63L',
    'DE_LU':'10Y1001A1001A82H',
}

BIDDING_ZONES = {
    'AL': '10YAL-KESH-----5',
    'AT': '10YAT-APG------L',
    'BA': '10YBA-JPCC-----D',
    'BE': '10YBE----------2',
    'BG': '10YCA-BULGARIA-R',
    'BY': '10Y1001A1001A51S',
    'CH': '10YCH-SWISSGRIDZ',
    'CZ': '10YCZ-CEPS-----N',
    'DE': '10Y1001A1001A83F',
    'DK': '10Y1001A1001A65H',
    'EE': '10Y1001A1001A39I',
    'ES': '10YES-REE------0',
    'FI': '10YFI-1--------U',
    'FR': '10YFR-RTE------C',
    'GB': '10YGB----------A',
    'GB_NIR': '10Y1001A1001A016',
    'GR': '10YGR-HTSO-----Y',
    'HR': '10YHR-HEP------M',
    'HU': '10YHU-MAVIR----U',
    'IE': '10YIE-1001A00010',
    'IT': '10YIT-GRTN-----B',
    'LT': '10YLT-1001A0008Q',
    'LU': '10YLU-CEGEDEL-NQ',
    'LV': '10YLV-1001A00074',
    'MD': 'MD',
    'ME': '10YCS-CG-TSO---S',
    'MK': '10YMK-MEPSO----8',
    'MT': '10Y1001A1001A93C',
    'NL': '10YNL----------L',
    'NO': '10YNO-0--------C',
    'PL': '10YPL-AREA-----S',
    'PT': '10YPT-REN------W',
    'RO': '10YRO-TEL------P',
    'RS': '10YCS-SERBIATSOV',
    'RU': '10Y1001A1001A49F',
    'RU_KGD': '10Y1001A1001A50U',
    'SE': '10YSE-1--------K',
    'SI': '10YSI-ELES-----O',
    'SK': '10YSK-SEPS-----K',
    'TR': '10YTR-TEIAS----W',
    'UA': '10YUA-WEPS-----0',
    'DE_AT_LU': '10Y1001A1001A63L',
    'DE_LU':'10Y1001A1001A82H',
    'DE': '10Y1001A1001A63L',  # DE-AT-LU
    'LU': '10Y1001A1001A63L',  # DE-AT-LU
    'IT_NORD': '10Y1001A1001A73I',
    'IT_CNOR': '10Y1001A1001A70O',
    'IT_CSUD': '10Y1001A1001A71M',
    'IT_SUD': '10Y1001A1001A788',
    'IT_FOGN': '10Y1001A1001A72K',
    'IT_ROSN': '10Y1001A1001A77A',
    'IT_BRNN': '10Y1001A1001A699',
    'IT_PRGP': '10Y1001A1001A76C',
    'IT_SARD': '10Y1001A1001A74G',
    'IT_SICI': '10Y1001A1001A75E',
    'IT_CALA': '10Y1001C--00096J',
    'NO_1': '10YNO-1--------2',
    'NO_2': '10YNO-2--------T',
    'NO_3': '10YNO-3--------J',
    'NO_4': '10YNO-4--------9',
    'NO_5': '10Y1001A1001A48H',
    'SE_1': '10Y1001A1001A44P',
    'SE_2': '10Y1001A1001A45N',
    'SE_3': '10Y1001A1001A46L',
    'SE_4': '10Y1001A1001A47J',
    'DK_1': '10YDK-1--------W',
    'DK_2': '10YDK-2--------M'
}

NEIGHBOURS = {
    'BE': ['NL', 'DE_AT_LU', 'FR', 'GB', 'DE_LU'],
    'NL': ['BE', 'DE_AT_LU', 'DE_LU', 'GB', 'NO_2', 'DK_1'],
    'DE_AT_LU': ['BE', 'CH', 'CZ', 'DK_1', 'DK_2', 'FR', 'IT_NORD', 'IT_NORD_AT', 'NL', 'PL', 'SE_4', 'SI'],
    'FR': ['BE', 'CH', 'DE_AT_LU', 'DE_LU', 'ES', 'GB', 'IT_NORD', 'IT_NORD_FR'],
    'CH': ['AT', 'DE_AT_LU', 'DE_LU', 'FR', 'IT_NORD', 'IT_NORD_CH'],
    'AT': ['CH', 'CZ', 'DE_LU', 'HU', 'IT_NORD', 'SI'],
    'CZ': ['AT', 'DE_AT_LU', 'DE_LU', 'PL', 'SK'],
    'GB': ['BE', 'FR', 'IE_SEM', 'NL'],
    'NO_2': ['DK_1', 'NL', 'NO_5'],
    'HU': ['AT', 'HR', 'RO', 'RS', 'SK', 'UA'],
    'IT_NORD': ['CH', 'DE_AT_LU', 'FR', 'SI', 'AT', 'IT_CNOR'],
    'ES': ['FR', 'PT'],
    'SI': ['AT', 'DE_AT_LU', 'HR', 'IT_NORD'],
    'RS': ['AL', 'BA', 'BG', 'HR', 'HU', 'ME', 'MK', 'RO'],
    'PL': ['CZ', 'DE_AT_LU', 'DE_LU', 'LT', 'SE_4', 'SK', 'UA'],
    'ME': ['AL', 'BA', 'RS'],
    'DK_1': ['DE_AT_LU', 'DE_LU', 'DK_2', 'NO_2', 'SE_3', 'NL'],
    'RO': ['BG', 'HU', 'RS', 'UA'],
    'LT': ['BY', 'LV', 'PL', 'RU_KGD', 'SE_4'],
    'BG': ['GR', 'MK', 'RO', 'RS', 'TR'],
    'SE_3': ['DK_1', 'FI', 'NO_1', 'SE_4'],
    'LV': ['EE', 'LT', 'RU'],
    'IE_SEM': ['GB'],
    'BA': ['HR', 'ME', 'RS'],
    'NO_1': ['NO_2', 'NO_3', 'NO_5', 'SE_3'],
    'SE_4': ['DE_AT_LU', 'DE_LU', 'DK_2', 'LT', 'PL'],
    'NO_5': ['NO_1', 'NO_2', 'NO_3'],
    'SK': ['CZ', 'HU', 'PL', 'UA'],
    'EE': ['FI', 'LV', 'RU'],
    'DK_2': ['DE_AT_LU', 'DE_LU', 'SE_4'],
    'FI': ['EE', 'NO_4', 'RU', 'SE_1', 'SE_3'],
    'NO_4': ['SE_2', 'FI', 'SE_1'],
    'SE_1': ['FI', 'NO_4', 'SE_2'],
    'SE_2': ['NO_3', 'NO_4', 'SE_3'],
    'DE_LU': ['AT', 'BE', 'CH', 'CZ', 'DK_1', 'DK_2', 'FR', 'NL', 'PL', 'SE_4'],
    'MK': ['BG', 'GR', 'RS'],
    'PT': ['ES'],
    'GR': ['AL', 'BG', 'IT_BRNN', 'IT_GR', 'MK', 'TR'],
    'NO_3': ['NO_4', 'NO_5', 'SE_2'],
    'IT': ['AT', 'FR', 'GR', 'MT', 'ME', 'SI', 'CH'],
    'IT_BRNN': ['GR', 'IT_SUD'],
    'IT_SUD': ['IT_BRNN', 'IT_CSUD', 'IT_FOGN', 'IT_ROSN', 'IT_CALA'],
    'IT_FOGN': ['IT_SUD'],
    'IT_ROSN': ['IT_SICI', 'IT_SUD'],
    'IT_CSUD': ['IT_CNOR', 'IT_SARD', 'IT_SUD'],
    'IT_CNOR': ['IT_NORD', 'IT_CSUD', 'IT_SARD'],
    'IT_SARD': ['IT_CNOR', 'IT_CSUD'],
    'IT_SICI': ['IT_CALA', 'IT_ROSN', 'MT'],
    'IT_CALA': ['IT_SICI', 'IT_SUD'],
    'MT': ['IT_SICI']
}


# Generation query

def generation_data(start, end, token= "a40164f1-ea44-4ebd-9ea7-81e6a0b08d3a", folder_name="generation_per_type") :
    """
    start: string date format '20201229'
    end: string date format '20201231'
    """
    start = pd.Timestamp('20201229', tz ='Europe/Brussels')
    end = pd.Timestamp('20201231', tz ='Europe/Brussels')
    client = EntsoePandasClient(api_key=token)

    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


    for i in DOMAIN_MAPPINGS :
        try:
            generation = client.query_generation(i, start=start,end=end)
            generation = generation.fillna(method='ffill')
            name = folder_name+'/'+i+'.csv'
            generation.to_csv(name, index=True )
        except  :
            continue
    return None

# Capacity 
def capacity_data(start, end, token= "a40164f1-ea44-4ebd-9ea7-81e6a0b08d3a", folder_name="capacity_per_type") :
    """
    start: string date format '20201229'
    end: string date format '20201231'
    """
    start = pd.Timestamp('20201229', tz ='Europe/Brussels')
    end = pd.Timestamp('20201231', tz ='Europe/Brussels')
    client = EntsoePandasClient(api_key=token)

    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    for i in DOMAIN_MAPPINGS :
        try:
            capacity = client.query_installed_generation_capacity(i, start=start,end=end, psr_type=None)
            capacity = capacity.fillna(method='ffill')
            name = folder_name+'/'+i+'.csv'
            capacity.to_csv(name, index=True)
        except  :
            continue
    return None

# Day-ahead prices 

def prices_data(start, end, token= "a40164f1-ea44-4ebd-9ea7-81e6a0b08d3a", folder_name='day_ahead_prices') :
    start = pd.Timestamp('20201229', tz ='Europe/Brussels')
    end = pd.Timestamp('20201231', tz ='Europe/Brussels')
    client = EntsoePandasClient(api_key=token)

    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


    for i in BIDDING_ZONES :
        try:
            prices = client.query_day_ahead_prices(i, start=start,end=end)
            prices = prices.fillna(method='ffill')
            name = folder_name+'/'+i+'.csv'
            prices.to_csv(name, index=True)
        except  :
            continue
    return None

# Imports and Exports 
def physical_flow_data(start, end, token= "a40164f1-ea44-4ebd-9ea7-81e6a0b08d3a", folder_name='day_ahead_prices') :

    start = pd.Timestamp('20201229', tz ='Europe/Brussels')
    end = pd.Timestamp('20201231', tz ='Europe/Brussels')
    client = EntsoePandasClient(api_key=token)

    for country_from in NEIGHBOURS :
        if not os.path.exists('day_ahead/'+country_from):
            os.makedirs('day_ahead/'+country_from)
        for country_to in NEIGHBOURS[country_from]:
            try :
                A = client.query_crossborder_flows(country_to, country_from, start=start, end=end) 
                A.fillna(method='ffill')
                name = 'day_ahead/'+country_from +'/'+country_from+'_to_'+country_to+'.csv'
                A.to_csv(name, index=True)
            except:
                continue
    return None

def load_data(start, end, token= "a40164f1-ea44-4ebd-9ea7-81e6a0b08d3a", folder_name='load') :
    start = pd.Timestamp('20201229', tz ='Europe/Brussels')
    end = pd.Timestamp('20201231', tz ='Europe/Brussels')
    client = EntsoePandasClient(api_key=token)

    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


    for i in BIDDING_ZONES :
        try:
            prices = client.query_load(i, start=start,end=end)
            prices = prices.fillna(method='ffill')
            name = folder_name+'/'+i+'.csv'
            prices.to_csv(name, index=True )
        except  :
            continue
    return None