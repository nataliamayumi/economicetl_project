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
def get_focus_household_consumption(path_data):
    """
    Retrieves annual expectations for Household Consumption from the Olinda API by the Central Bank of Brazil,
    converts 'DataReferencia' into timestamp format (yyyy-mm-01), and saves the result as a Pickle file.
    """
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
        "ExpectativasMercadoAnuais?$top=5&$filter=Indicador%20eq%20'PIB%20"
        "Despesa%20de%20consumo%20das%20fam%C3%ADlias'%20and%20baseCalculo%20eq%200&$orderby=Data%20desc&"
        "$format=text/csv&$select=Indicador,Data,DataReferencia,Mediana"
    )

    df = pd.read_csv(url, decimal=',')
    df['Year'] = pd.to_datetime(df['DataReferencia'], format='%Y')
    df['Date'] = df['Year'].apply(lambda x: x.replace(month=12, day=1))
    df = (
        df.set_index('Date')
          .drop(columns=['Year', 'DataReferencia', 'Indicador', 'Data'])
          .rename(columns={'Mediana': 'household_consumption_expectation'})
    )

    df.to_pickle(f'{path_data}/raw/focus-household-consumption-annual.pkl')
    return df
def get_focus_industrial_gdp(path_data):
    """
    Retrieves annual expectations for Industrial GDP from the Olinda API by the Central Bank of Brazil,
    converts 'DataReferencia' into timestamp format (yyyy-mm-01), and saves the result as a Pickle file.
    """
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
        "ExpectativasMercadoAnuais?$top=5&$filter=Indicador%20eq%20'PIB%20Ind%C3%BAstria'"
        "&$orderby=Data%20desc&$format=text/csv&$select=Indicador,Data,DataReferencia,Mediana"
    )

    df = pd.read_csv(url, decimal=',')
    df['Year'] = pd.to_datetime(df['DataReferencia'], format='%Y')
    df['Date'] = df['Year'].apply(lambda x: x.replace(month=12, day=1))
    df = (
        df.set_index('Date')
          .drop(columns=['Year', 'DataReferencia', 'Indicador'])
          .rename(columns={'Mediana': 'industrial_gdp_expectation'})
    )

    df.to_pickle(f'{path_data}/raw/focus-industrial-gdp-annual.pkl')
    return df
def get_focus_commerce_gdp(path_data):
    """
    Retrieves annual expectations for Commerce GDP from the Olinda API by the Central Bank of Brazil,
    converts 'DataReferencia' into timestamp format (yyyy-mm-01), and saves the result as a Pickle file.
    """
    # Reusing the same endpoint as Industrial GDP
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
        "ExpectativasMercadoAnuais?$top=5&$filter=Indicador%20eq%20'PIB%20Ind%C3%BAstria'"
        "&$orderby=Data%20desc&$format=text/csv&$select=Indicador,Data,DataReferencia,Mediana"
    )

    df = pd.read_csv(url, decimal=',')
    df['Year'] = pd.to_datetime(df['DataReferencia'], format='%Y')
    df['Date'] = df['Year'].apply(lambda x: x.replace(month=12, day=1))
    df = (
        df.set_index('Date')
          .drop(columns=['Year', 'DataReferencia', 'Indicador'])
          .rename(columns={'Mediana': 'commerce_gdp_expectation'})
    )

    df.to_pickle(f'{path_data}/raw/focus-commerce-gdp-annual.pkl')
    return df
def get_focus_unemployment(path_data):
    """
    Retrieves quarterly unemployment expectations from the Olinda API by the Central Bank of Brazil,
    converts 'DataReferencia' into timestamp format (yyyy-mm-01), and saves the result as a Pickle file.
    """
    def quarter_to_date(x):  # You can reuse the original helper if needed
        end_month = {1: 3, 2: 6, 3: 9, 4: 12}
        quarter, year = map(int, x.split('/'))
        month = end_month[quarter]
        return pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthBegin(0)

    url = (
        "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
        "ExpectativasMercadoTrimestrais?$top=6&$filter=Indicador%20eq%20'Taxa%20de%20desocupa%C3%A7%C3%A3o'"
        "%20and%20baseCalculo%20eq%200&$orderby=Data%20desc&$format=text/csv&$select=Data,DataReferencia,Mediana"
    )

    df = pd.read_csv(url, decimal=',')
    df['Quarter'] = df['DataReferencia'].apply(quarter_to_date)
    df = (
        df.set_index('Quarter')
          .drop(columns=['Data', 'DataReferencia'])
          .rename(columns={'Mediana': 'unemployment_expectation'})
    )

    df.to_pickle(f'{path_data}/raw/focus-unemployment-quarterly.pkl')
    return df

def get_focus_ipca(path_data):
    """
    Retrieves monthly IPCA expectations from the Olinda API by the Central Bank of Brazil,
    converts 'DataReferencia' into timestamp format (yyyy-mm-01), and saves the result as a Pickle file.
    """
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
        "ExpectativaMercadoMensais?$top=25&$filter=Indicador%20eq%20'IPCA'%20and%20baseCalculo%20eq%200"
        "&$orderby=Data%20desc&$format=text/csv&$select=Data,DataReferencia,Mediana"
    )

    df = pd.read_csv(url, decimal=',')
    df['Month'] = pd.to_datetime(df['DataReferencia'], format='%m/%Y')
    df = (
        df.set_index('Month')
          .drop(columns=['Data', 'DataReferencia'])
          .rename(columns={'Mediana': 'ipca_expectation'})
          .sort_index()
    )

    df.to_pickle(f'{path_data}/raw/focus-ipca-monthly.pkl')
    return df

def get_focus_selic(path_data):
    """
    Retrieves Selic expectations by meeting from the Olinda API by the Central Bank of Brazil,
    filters only even-numbered meetings to use them as quarterly observations,
    converts them into timestamp format (yyyy-mm-01), and saves the result as a Pickle file.
    """
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
        "ExpectativasMercadoSelic?$top=16&$filter=baseCalculo%20eq%200&$orderby=Data%20desc&"
        "$format=text/csv&$select=Reuniao,Mediana"
    )

    df = pd.read_csv(url, decimal=',')
    df[['Meeting', 'Year']] = df['Reuniao'].str.split('/', expand=True)
    df = df[df['Meeting'].isin(['R2', 'R4', 'R6', 'R8'])]
    
    month_map = {'R2': 3, 'R4': 6, 'R6': 9, 'R8': 12}
    df['Month'] = df['Meeting'].map(month_map)
    df['Quarter'] = pd.to_datetime({
        'year': df['Year'].astype(int),
        'month': df['Month'],
        'day': 1
    })

    df = df.set_index('Quarter').drop(columns=['Reuniao', 'Meeting', 'Year', 'Month'])
    df.to_pickle(f'{path_data}/raw/focus-selic-quarterly.pkl')
    return df
