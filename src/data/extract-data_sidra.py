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
    Quarterly data comes with the reference date in the format yyyytt, so the function below
    converts SIDRA dates to timestamp format (yyyy-mm-01)
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
    Retrieves quarterly total GDP index data from IBGE using the SIDRA API, converts it to timestamp format (yyyy-mm-01),
    and exports it to a Pickle file.
    """
    # Data collection
    gdp = sidrapy.get_table(
        table_code='1621',
        territorial_level="1",
        ibge_territorial_code="all",
        variable="584",
        period="all",
        classifications={'11255': '90707'},
        header='n'
    )

    # Data processing and conversion
    gdp = gdp[['V', 'D2C']].rename(columns={'V': 'gdp', 'D2C': 'Quarter'})
    gdp['gdp'] = pd.to_numeric(gdp['gdp'], errors='coerce')  # Convert 'gdp' to float
    gdp['Quarter'] = gdp['Quarter'].map(quarter_to_timestamp)
    gdp.set_index('Quarter', inplace=True)

    # Export to Pickle
    gdp.to_pickle(f'{path_data}/raw/ibge-gdp-quarterly.pkl')
    return gdp
