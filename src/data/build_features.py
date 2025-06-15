# Document responsible for combining and projecting GDP data
# Import libraries
import warnings
import pandas as pd
import numpy as np
import coleta_dados_olinda
import coleta_dados_sidra
warnings.filterwarnings("ignore")

def project_gdp_features(path_data):
    """
    Projects quarterly GDP index values based on expected year-over-year changes released by the Central Bank of Brazil.

    Parameters:
    - df_obs: DataFrame with columns ['Quarter', 'gdp'] containing observed deseasonalized quarterly GDP index values.
    - df_exp: DataFrame with columns ['Quarter', 'gdp_median_expectation'] containing year-over-year expectations (%).

    Returns:
    - Combined DataFrame with projected GDP index values.
    """
    # Load observed and expectations datasets
    df_obs = coleta_dados_sidra.get_ibge_gdp(path_data)
    df_exp = coleta_dados_olinda.get_focus_gdp(path_data)
    df_obs = df_obs[df_obs.index > '2013-12-01']  # Filter data starting from 2014

    # Merge the datasets into a single DataFrame
    df_gdp = pd.merge(df_obs, df_exp, on='Quarter', how='outer').sort_values('Quarter')
    df_gdp = df_gdp.sort_index()

    # Fill in missing index values using the expected variation
    for i in range(len(df_gdp)):
        if pd.isna(df_gdp.iloc[i]['gdp']) and i >= 4:
            gdp_base = df_gdp.iloc[i - 4]['gdp']  # Index value 4 quarters before
            expectation = df_gdp.iloc[i]['gdp_median_expectation']  # Expected variation

            if pd.notna(gdp_base) and pd.notna(expectation):
                df_gdp.iloc[i, df_gdp.columns.get_loc('gdp')] = gdp_base * (1 + expectation / 100)  # Projected index

    # Optionally calculate 12-month accumulated variation
    # df_gdp['gdp_12m_change'] = df_gdp['gdp'].rolling(window=4).sum().pct_change(4)

    # Save to Pickle
    df_gdp = df_gdp[['gdp']]
    df_gdp.to_pickle(f'{path_data}/interim/gdp-quarterly.pkl')

    return df_gdp
