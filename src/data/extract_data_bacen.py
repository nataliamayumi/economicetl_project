# Document responsible for collecting expectation data made available by the Central Bank of Brazil
# We use the Olinda platform to collect the BCB expectations data
# Data collection link: https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/aplicacao#!/recursos
# Documentation link: https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/documentacao
# Path to example specifications: docs/EspecificacoesOlinda

# Import necessary libraries
import warnings
import pandas as pd
import numpy as np
warnings.filterwarnings("ignore")

#-----------------------

def quarter_to_date(x):
    """
    The reference date provided by Olinda is in the format t/yyyy,
    so we need to manipulate the date to convert it into a timestamp format (yyyy-mm-01)
    """
    end_month = {1: 3, 2: 6, 3: 9, 4: 12}
    quarter, year = map(int, x.split('/'))
    month = end_month[quarter]
    return pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthBegin(0)

#-----------------------

def get_focus_gdp(path_data):
    """
    Retrieves quarterly GDP expectations via the Olinda API from the Central Bank of Brazil,
    processes the 'DataReferencia' into timestamp format (yyyy-mm-01), and saves it as a Pickle file.
    """
    # Data collection
    url = (f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/" +
           f"ExpectativasMercadoTrimestrais?$top=8&$filter=Indicador%20eq%20'PIB%20" +
           f"Total'%20and%20baseCalculo%20eq%200&$orderby=Data%20desc&$format=text/" +
           f"csv&$select=Data,DataReferencia,Mediana")
    
    gdp_expectations = pd.read_csv(url, decimal=',')
    
    # Initial processing
    gdp_expectations['Quarter'] = gdp_expectations['DataReferencia'].apply(quarter_to_date)
    gdp_expectations = (
        gdp_expectations
        .set_index('Quarter')
        .drop(columns=['Data', 'DataReferencia'])
        .rename(columns={'Mediana': 'gdp_median_expectation'})
    )
    
    # Export to pickle
    gdp_expectations.to_pickle(f'{path_data}/raw/focus-gdp-quarterly.pkl')
    return gdp_expectations
