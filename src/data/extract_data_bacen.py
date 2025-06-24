# Import required libraries
import warnings
import pandas as pd
import requests
from io import StringIO
import time
from pandas.errors import ParserError

warnings.filterwarnings("ignore")

def fetch_bcb_data(code, start: str, end: str, attempts=3, wait=2) -> pd.DataFrame:
    """
    Downloads data from the Central Bank of Brazil API (SGS system).
    
    Parameters:
    - code (int): The SGS code for the data series.
    - start (str): Start date in format 'dd/mm/yyyy'. Use '' for full series.
    - end (str): End date in format 'dd/mm/yyyy'. Use '' for full series.
    - attempts (int): Number of retry attempts if request fails.
    - wait (int): Seconds to wait between attempts.

    Returns:
    - pd.DataFrame with datetime index and values.
    """
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados?formato=csv"
        f"&dataInicial={start}&dataFinal={end}"
    )
    headers = {"Accept": "text/csv"}

    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text), sep=';', decimal=',')
            df['data'] = pd.to_datetime(df['data'], dayfirst=True)
            return df.set_index('data')
        except (requests.RequestException, ParserError, ValueError) as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt < attempts:
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"Failed to fetch BCB data after {attempts} attempts."
                ) from e
def get_credit_concession_individuals(path_data):
    """
    Retrieves the volume of credit concessions to individuals from the BCB API
    and saves the data as a Pickle file.
    """
    df = fetch_bcb_data(20633, "01/01/2010", "")
    df = df.rename(columns={'valor': 'credit_concession_individuals_million'})
    df.to_pickle(f'{path_data}/raw/credit-concession-individuals.pkl')
    return df
def get_credit_concession_companies(path_data):
    """
    Retrieves the volume of credit concessions to companies from the BCB API
    and saves the data as a Pickle file.
    """
    df = fetch_bcb_data(20632, "01/01/2010", "")
    df = df.rename(columns={'valor': 'credit_concession_companies_million'})
    df.to_pickle(f'{path_data}/raw/credit-concession-companies.pkl')
    return df
def get_avg_interest_rate_individuals(path_data):
    """
    Retrieves the average interest rate for credit operations with free resources – 
    individuals (installment credit cards) from the BCB API and saves it as a Pickle file.
    """
    df = fetch_bcb_data(22023, "01/01/2010", "")
    df = df.rename(columns={'valor': 'avg_interest_rate_individuals'})
    df.to_pickle(f'{path_data}/raw/avg-interest-rate-individuals.pkl')
    return df
def get_selic_quarterly(path_data):
    """
    Retrieves the daily Selic target rate from the BCB API,
    selects the last available observation of each quarter,
    and saves the result as a Pickle file.
    """
    # Download data in two blocks due to API limits
    selic_2014_2021 = fetch_bcb_data(432, "01/01/2014", "31/12/2021")
    selic_2022_onward = fetch_bcb_data(432, "01/01/2022", "")
    df = pd.concat([selic_2014_2021, selic_2022_onward]).reset_index()

    # Add helper columns
    df['year'] = df['data'].dt.year
    df['month'] = df['data'].dt.month

    # Select last day of each quarter
    df_quarters = df[df['month'].isin([3, 6, 9, 12])]
    df_quarters = df_quarters[df_quarters['data'].dt.day.isin([30, 31])]
    df_quarters = (
        df_quarters
        .sort_values('data')
        .groupby(['year', 'month'])
        .tail(1)
    )
    df_quarters['data'] = df_quarters['data'] + pd.offsets.MonthBegin(-1)
    df_quarters = (
        df_quarters
        .drop(columns=['year', 'month'])
        .rename(columns={'valor': 'selic_rate', 'data': 'Quarter'})
        .set_index('Quarter')
    )

    df_quarters.to_pickle(f"{path_data}/raw/selic-rate-quarterly.pkl")
    return df_quarters
