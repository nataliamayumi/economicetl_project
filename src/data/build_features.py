# Responsible for projecting key macroeconomic indicators
# Import libraries
import warnings
import pandas as pd
import numpy as np
import coleta_dados_olinda
import coleta_dados_sgs
import coleta_dados_sidra

warnings.filterwarnings("ignore")

def features_gdp(path_data):
    """
    Projects quarterly GDP index based on expected year-over-year variations published by the Central Bank.

    Parameters:
    - path_data: Path to store and retrieve raw and processed data files.

    Returns:
    - Combined DataFrame with projected GDP index.
    """
    df_obs = coleta_dados_sidra.make_pib(path_data)
    df_exp = coleta_dados_olinda.make_focus_pib(path_data)
    df_obs = df_obs[df_obs.index > '2013-12-01']  # Filter data starting from 2014

    df_gdp = pd.merge(df_obs, df_exp, on='Trimestre', how='outer').sort_values('Trimestre')
    df_gdp = df_gdp.sort_index()

    for i in range(len(df_gdp)):
        if pd.isna(df_gdp.iloc[i]['pib']) and i >= 4:
            base = df_gdp.iloc[i - 4]['pib']
            expectation = df_gdp.iloc[i]['pib_exp_mediana']
            if pd.notna(base) and pd.notna(expectation):
                df_gdp.iloc[i, df_gdp.columns.get_loc('pib')] = base * (1 + expectation / 100)

    df_gdp = df_gdp[['pib']]
    df_gdp.to_pickle(f'{path_data}/interim/pib-trimestral.pkl')
    return df_gdp


def features_household_consumption(path_data):
    """
    Projects year-end quarterly index of Household Consumption based on annual expectations from the Central Bank
    and interpolates values for intermediate quarters.

    Parameters:
    - path_data: Path to store and retrieve raw and processed data files.

    Returns:
    - Combined DataFrame with projected Household Consumption index.
    """
    df_obs = coleta_dados_sidra.make_consumoFamilias(path_data)
    df_exp = coleta_dados_olinda.make_focus_consumo_familias(path_data)
    df_obs = df_obs[df_obs.index >= '2013-12-01']

    idx_range = pd.date_range(start=df_obs.index[0], end=df_exp.index[-1], freq='QS') + pd.DateOffset(months=2)
    df_combined = pd.DataFrame(index=idx_range)

    df_combined = df_combined.join(df_obs).join(df_exp).sort_index()

    for i in range(len(df_combined)):
        if pd.isna(df_combined.iloc[i]['consumoFamilias']) and i >= 4:
            base = df_combined.iloc[i - 4]['consumoFamilias']
            expectation = df_combined.iloc[i]['consumoFamilias_exp_mediana']
            if pd.notna(base) and pd.notna(expectation):
                df_combined.iloc[i, df_combined.columns.get_loc('consumoFamilias')] = base * (1 + expectation / 100)

    df_combined['consumoFamilias'] = df_combined['consumoFamilias'].interpolate(method='cubic')
    df_combined = df_combined[['consumoFamilias']]
    df_combined.to_pickle(f'{path_data}/interim/consumoFamilias-trimestral.pkl')

    return df_combined


def features_industrial_gdp(path_data):
    """
    Projects year-end quarterly index of Industrial GDP based on annual expectations from the Central Bank
    and interpolates values for intermediate quarters.

    Parameters:
    - path_data: Path to store and retrieve raw and processed data files.

    Returns:
    - Combined DataFrame with projected Industrial GDP index.
    """
    df_obs = coleta_dados_sidra.make_pibIndustrial(path_data)
    df_exp = coleta_dados_olinda.make_focus_pibIndustrial(path_data)
    df_obs = df_obs[df_obs.index >= '2013-12-01']

    idx_range = pd.date_range(start=df_obs.index[0], end=df_exp.index[-1], freq='QS') + pd.DateOffset(months=2)
    df_combined = pd.DataFrame(index=idx_range)

    df_combined = df_combined.join(df_obs).join(df_exp).sort_index()

    for i in range(len(df_combined)):
        if pd.isna(df_combined.iloc[i]['pibIndustrial']) and i >= 4:
            base = df_combined.iloc[i - 4]['pibIndustrial']
            expectation = df_combined.iloc[i]['pibIndustrial_exp_mediana']
            if pd.notna(base) and pd.notna(expectation):
                df_combined.iloc[i, df_combined.columns.get_loc('pibIndustrial')] = base * (1 + expectation / 100)

    df_combined['pibIndustrial'] = df_combined['pibIndustrial'].interpolate(method='cubic')
    df_combined = df_combined[['pibIndustrial']]
    df_combined.to_pickle(f'{path_data}/interim/pibIndustrial-trimestral.pkl')

    return df_combined


def features_unemployment(path_data):
    """
    Combines observed and expected quarterly unemployment rates.

    Parameters:
    - path_data: Path to store and retrieve raw and processed data files.

    Returns:
    - Combined DataFrame with observed and expected unemployment rates.
    """
    df_obs = coleta_dados_sidra.make_desocupacao(path_data)
    df_exp = coleta_dados_olinda.make_focus_desocupacao(path_data).rename(columns={'desocupação_exp_mediana': 'desocupacao'})
    df_obs = df_obs[df_obs.index > '2013-12-01']

    df_combined = pd.concat([df_obs, df_exp]).sort_values('Trimestre')
    df_combined.to_pickle(f'{path_data}/interim/desocupacao-trimestral.pkl')

    return df_combined


def features_ipca(path_data):
    """
    Projects monthly IPCA index based on year-over-year expectations from the Central Bank,
    then selects values corresponding to the end of each quarter (March, June, September, December).

    Parameters:
    - path_data: Path to store and retrieve raw and processed data files.

    Returns:
    - Quarterly DataFrame with projected IPCA values.
    """
    df_obs = coleta_dados_sidra.make_ipca(path_data)
    df_exp = coleta_dados_olinda.make_focus_ipca(path_data)
    df_obs = df_obs[df_obs.index > '2013-12-01']

    df_combined = pd.merge(df_obs, df_exp, on='Mes', how='outer').sort_index()

    for i in range(len(df_combined)):
        if pd.isna(df_combined.iloc[i]['ipca']) and i >= 1:
            base = df_combined.iloc[i - 1]['ipca']
            expectation = df_combined.iloc[i]['ipca_exp_mediana']
            if pd.notna(base) and pd.notna(expectation):
                df_combined.iloc[i, df_combined.columns.get_loc('ipca')] = base * (1 + expectation / 100)

    df_combined.reset_index(inplace=True)
    df_quarterly = df_combined[df_combined['Mes'].dt.month.isin([3, 6, 9, 12])]
    df_quarterly['Trimestre'] = df_quarterly['Mes'].dt.to_period('Q').dt.to_timestamp() + pd.DateOffset(months=2)
    df_quarterly.index = df_quarterly['Trimestre']
    df_quarterly = df_quarterly.drop(columns=['Mes', 'ipca_exp_mediana', 'Trimestre'])

    df_quarterly.to_pickle(f'{path_data}/interim/ipca-trimestral.pkl')
    return df_quarterly


def features_selic(path_data):
    """
    Combines observed and expected quarterly Selic rate data.

    Parameters:
    - path_data: Path to store and retrieve raw and processed data files.

    Returns:
    - Combined DataFrame with observed and expected Selic rates.
    """
    df_obs = coleta_dados_sgs.make_selic(path_data)
    df_exp = coleta_dados_olinda.make_focus_selic(path_data).rename(columns={'Mediana': 'tx_selic'})

    df_combined = pd.concat([df_obs, df_exp]).sort_index()
    df_combined.to_pickle(f'{path_data}/interim/selic-trimestral.pkl')

    return df_combined
