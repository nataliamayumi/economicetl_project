# Responsible for projecting key macroeconomic indicators
# Import libraries
import warnings
import pandas as pd
import numpy as np
import extract_data_bacen
import extract_data_olinda
import extract_data_sidra


warnings.filterwarnings("ignore")

def project_gdp(path_data):
    """
    Projects quarterly GDP index based on year-over-year expectations from the Central Bank.

    Parameters:
    - path_data: Path to store and retrieve data files.

    Returns:
    - DataFrame with projected GDP index.
    """
    df_observed = extract_data_sidra.get_ibge_gdp(path_data)
    df_expected = extract_data_olinda.get_focus_gdp(path_data)

    df_gdp = pd.merge(df_observed, df_expected, on='Quarter', how='outer').sort_values('Quarter')
    df_gdp = df_gdp.sort_index()

    for i in range(len(df_gdp)):
        if pd.isna(df_gdp.iloc[i]['gdp']) and i >= 4:
            base = df_gdp.iloc[i - 4]['gdp']
            expectation = df_gdp.iloc[i]['gdp_median_expectation']
            if pd.notna(base) and pd.notna(expectation):
                df_gdp.at[df_gdp.index[i], 'gdp'] = base * (1 + expectation / 100)

    df_gdp = df_gdp[['gdp']]
    df_gdp.to_pickle(f'{path_data}/interim/gdp-quarterly.pkl')
    return df_gdp


def project_household_consumption(path_data):
    """
    Projects quarterly household consumption index using yearly expectations from the Central Bank
    and interpolates intermediate quarters.

    Parameters:
    - path_data: Path to store and retrieve data files.

    Returns:
    - DataFrame with projected household consumption index.
    """
    df_observed = extract_data_sidra.get_household_consumption(path_data)
    df_expected = extract_data_olinda.get_focus_household_consumption(path_data)


    idx_range = pd.date_range(start=df_observed.index[0], end=df_expected.index[-1], freq='QS') + pd.DateOffset(months=2)
    df_combined = pd.DataFrame(index=idx_range)

    df_combined = df_combined.join(df_observed).join(df_expected).sort_index()

    for i in range(len(df_combined)):
        if pd.isna(df_combined.iloc[i]['household_consumption']) and i >= 4:
            base = df_combined.iloc[i - 4]['household_consumption']
            expectation = df_combined.iloc[i]['household_consumption_exp_median']
            if pd.notna(base) and pd.notna(expectation):
                df_combined.at[df_combined.index[i], 'household_consumption'] = base * (1 + expectation / 100)

    df_combined['household_consumption'] = df_combined['household_consumption'].interpolate(method='cubic')
    df_combined = df_combined[['household_consumption']]
    df_combined.to_pickle(f'{path_data}/interim/household_consumption-quarterly.pkl')

    return df_combined


def project_industrial_gdp(path_data):
    """
    Projects quarterly industrial GDP index using yearly expectations from the Central Bank
    and interpolates intermediate quarters.

    Parameters:
    - path_data: Path to store and retrieve data files.

    Returns:
    - DataFrame with projected industrial GDP index.
    """
    df_observed = extract_data_sidra.get_ibge_industrial_gdp(path_data)
    df_expected = extract_data_olinda.get_focus_industrial_gdp(path_data)


    idx_range = pd.date_range(start=df_observed.index[0], end=df_expected.index[-1], freq='QS') + pd.DateOffset(months=2)
    df_combined = pd.DataFrame(index=idx_range)

    df_combined = df_combined.join(df_observed).join(df_expected).sort_index()

    for i in range(len(df_combined)):
        if pd.isna(df_combined.iloc[i]['industrial_gdp']) and i >= 4:
            base = df_combined.iloc[i - 4]['industrial_gdp']
            expectation = df_combined.iloc[i]['industrial_gdp_exp_median']
            if pd.notna(base) and pd.notna(expectation):
                df_combined.at[df_combined.index[i], 'industrial_gdp'] = base * (1 + expectation / 100)

    df_combined['industrial_gdp'] = df_combined['industrial_gdp'].interpolate(method='cubic')
    df_combined = df_combined[['industrial_gdp']]
    df_combined.to_pickle(f'{path_data}/interim/industrial_gdp-quarterly.pkl')

    return df_combined


def project_unemployment(path_data):
    """
    Combines observed and expected quarterly unemployment rates.

    Parameters:
    - path_data: Path to store and retrieve data files.

    Returns:
    - DataFrame with observed and expected unemployment rates.
    """
    df_observed = extract_data_sidra.get_ibge_unemployment_rate(path_data)
    df_expected = extract_data_olinda.get_focus_unemployment(path_data).rename(columns={'unemployment_exp_median': 'unemployment'})


    df_combined = pd.concat([df_observed, df_expected]).sort_values('Quarter')
    df_combined.to_pickle(f'{path_data}/interim/unemployment-quarterly.pkl')

    return df_combined


def project_ipca(path_data):
    """
    Projects monthly IPCA index based on year-over-year expectations,
    then selects quarter-end values (March, June, September, December).

    Parameters:
    - path_data: Path to store and retrieve data files.

    Returns:
    - DataFrame with quarterly projected IPCA index.
    """
    df_observed = extract_data_sidra.get_ibge_ipca(path_data)
    df_expected = extract_data_olinda.get_focus_ipca(path_data)


    df_combined = pd.merge(df_observed, df_expected, on='Month', how='outer').sort_index()

    for i in range(len(df_combined)):
        if pd.isna(df_combined.iloc[i]['ipca']) and i >= 1:
            base = df_combined.iloc[i - 1]['ipca']
            expectation = df_combined.iloc[i]['ipca_exp_median']
            if pd.notna(base) and pd.notna(expectation):
                df_combined.at[df_combined.index[i], 'ipca'] = base * (1 + expectation / 100)

    df_combined.reset_index(inplace=True)
    df_quarterly = df_combined[df_combined['Month'].dt.month.isin([3, 6, 9, 12])].copy()
    df_quarterly['Quarter'] = df_quarterly['Month'].dt.to_period('Q').dt.to_timestamp() + pd.DateOffset(months=2)
    df_quarterly.set_index('Quarter', inplace=True)
    df_quarterly = df_quarterly[['ipca']]
    df_quarterly.to_pickle(f'{path_data}/interim/ipca-quarterly.pkl')

    return df_quarterly


def project_selic(path_data):
    """
    Combines observed and expected quarterly Selic rate data.

    Parameters:
    - path_data: Path to store and retrieve data files.

    Returns:
    - DataFrame with projected Selic rates.
    """
    df_observed = extract_data_bacen.get_selic_quarterly(path_data)
    df_expected = extract_data_olinda.get_focus_selic(path_data).rename(columns={'median': 'selic_rate'})

    df_combined = pd.concat([df_observed, df_expected]).sort_index()
    df_combined.to_pickle(f'{path_data}/interim/selic-quarterly.pkl')

    return df_combined
