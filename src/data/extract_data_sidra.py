# This script is responsible for collecting data provided by IBGE via the SIDRA platform
# To learn more about the SIDRA API, visit: https://apisidra.ibge.gov.br/
# Import necessary libraries
import warnings
import pandas as pd
import sidrapy
warnings.filterwarnings("ignore")

# path_data = '../../data'

def quarter_to_timestamp(value):
    """
    Quarterly data comes with the reference date in the format yyyytt,
    so this function converts SIDRA dates to timestamp format (yyyy-mm-01).
    """
    year = int(str(value)[:4])
    quarter = int(str(value)[-2:])
    
    # Map the quarter to its last corresponding month
    last_month_of_quarter = {1: 3, 2: 6, 3: 9, 4: 12}
    
    month = last_month_of_quarter.get(quarter)
    return pd.Timestamp(year=year, month=month, day=1)

#-----------------------

def get_ibge_gdp(path_data):
    """
    Retrieves quarterly total GDP index data from IBGE using the SIDRA API,
    converts it to timestamp format (yyyy-mm-01), and exports it to a Pickle file.
    """
    gdp = sidrapy.get_table(
        table_code='1621',
        territorial_level="1",
        ibge_territorial_code="all",
        variable="584",
        period="all",
        classifications={'11255': '90707'},
        header='n'
    )
    gdp = gdp[['V', 'D2C']].rename(columns={'V': 'gdp', 'D2C': 'Quarter'})
    gdp['gdp'] = pd.to_numeric(gdp['gdp'], errors='coerce')
    gdp['Quarter'] = gdp['Quarter'].map(quarter_to_timestamp)
    gdp.set_index('Quarter', inplace=True)

    gdp.to_pickle(f'{path_data}/raw/ibge-gdp-quarterly.pkl')
    return gdp

#-----------------------

def get_ibge_household_consumption(path_data):
    """
    Retrieves quarterly household consumption index data from IBGE using the SIDRA API,
    converts it to timestamp format (yyyy-mm-01), and exports it to a Pickle file.
    """
    data = sidrapy.get_table(
        table_code='1621',
        territorial_level='1',
        ibge_territorial_code='all',
        variable='584',
        period='all',
        classifications={'11255': '93404'},
        header='n'
    )
    data = data[['V', 'D2C']].rename(columns={'V': 'household_consumption', 'D2C': 'Quarter'})
    data['household_consumption'] = pd.to_numeric(data['household_consumption'], errors='coerce')
    data['Quarter'] = data['Quarter'].map(quarter_to_timestamp)
    data.set_index('Quarter', inplace=True)

    data.to_pickle(f'{path_data}/raw/ibge-household-consumption-quarterly.pkl')
    return data

#-----------------------

def get_ibge_industrial_gdp(path_data):
    """
    Retrieves quarterly industrial GDP index data from IBGE using the SIDRA API,
    converts it to timestamp format (yyyy-mm-01), and exports it to a Pickle file.
    """
    data = sidrapy.get_table(
        table_code='1621',
        territorial_level="1",
        ibge_territorial_code="all",
        variable="584",
        period="all",
        classifications={'11255': '90691'},
        header='n'
    )
    data = data[['V', 'D2C']].rename(columns={'V': 'industrial_gdp', 'D2C': 'Quarter'})
    data['industrial_gdp'] = pd.to_numeric(data['industrial_gdp'], errors='coerce')
    data['Quarter'] = data['Quarter'].map(quarter_to_timestamp)
    data.set_index('Quarter', inplace=True)

    data.to_pickle(f'{path_data}/raw/ibge-industrial-gdp-quarterly.pkl')
    return data

#-----------------------

def get_ibge_trade_gdp(path_data):
    """
    Retrieves quarterly trade GDP index data from IBGE using the SIDRA API,
    converts it to timestamp format (yyyy-mm-01), and exports it to a Pickle file.
    """
    data = sidrapy.get_table(
        table_code='1621',
        territorial_level="1",
        ibge_territorial_code="all",
        variable="584",
        period="all",
        classifications={'11255': '90697'},
        header='n'
    )
    data = data[['V', 'D2C']].rename(columns={'V': 'trade_gdp', 'D2C': 'Quarter'})
    data['trade_gdp'] = pd.to_numeric(data['trade_gdp'], errors='coerce')
    data['Quarter'] = data['Quarter'].map(quarter_to_timestamp)
    data.set_index('Quarter', inplace=True)

    data.to_pickle(f'{path_data}/raw/ibge-trade-gdp-quarterly.pkl')
    return data

#-----------------------

def get_ibge_unemployment_rate(path_data):
    """
    Retrieves quarterly unemployment rate data from IBGE using the SIDRA API,
    converts it to timestamp format (yyyy-mm-01), and exports it to a Pickle file.
    """
    data = sidrapy.get_table(
        table_code='4099',
        territorial_level='1',
        ibge_territorial_code='all',
        variable='4099',
        period='all',
        header='n'
    )
    data = data[['V', 'D2C']].rename(columns={'V': 'unemployment_rate', 'D2C': 'Quarter'})
    data['unemployment_rate'] = pd.to_numeric(data['unemployment_rate'], errors='coerce')
    data['Quarter'] = data['Quarter'].map(quarter_to_timestamp)
    data.set_index('Quarter', inplace=True)

    data.to_pickle(f'{path_data}/raw/ibge-unemployment-rate-quarterly.pkl')
    return data

#-----------------------

def get_ibge_ipca(path_data):
    """
    Retrieves monthly IPCA inflation index data from IBGE using the SIDRA API,
    removes the first row (Dec/1979), creates a datetime index in timestamp format (yyyy-mm-01),
    and exports it to a Pickle file.
    """
    data = sidrapy.get_table(
        table_code='1737',
        territorial_level="1",
        ibge_territorial_code="all",
        variable="2266",
        period="all",
        header='n'
    )
    data = data.drop([0])  # Remove Dec/1979 observation
    data = pd.DataFrame(data.loc[:, 'V'].astype(float)).rename(columns={'V': 'ipca'})
    data = data.assign(Month=pd.date_range('1980-01-01', periods=len(data), freq='MS')).set_index('Month')

    data.to_pickle(f'{path_data}/raw/ibge-ipca-monthly.pkl')
    return data

#-----------------------

def get_ibge_pmc(path_data):
    """
    Retrieves monthly retail sales index (PMC) data from IBGE using the SIDRA API,
    creates a datetime index in timestamp format (yyyy-mm-01),
    and exports it to a Pickle file.
    """
    data = sidrapy.get_table(
        table_code='8881',
        territorial_level="1",
        ibge_territorial_code="all",
        variable="7170",
        period="all",
        classifications={'11046': '56736'},
        header='n'
    )
    data = pd.DataFrame(data.loc[:, 'V'].astype(float)).rename(columns={'V': 'pmc'})
    data = data.assign(Month=pd.date_range('2003-01-01', periods=len(data), freq='MS')).set_index('Month')

    data.to_pickle(f'{path_data}/raw/ibge-pmc-monthly.pkl')
    return data
