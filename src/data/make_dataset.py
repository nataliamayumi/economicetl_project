import pandas as pd
import numpy as np
import os
import pmdarima as pm

import build_features
import coleta_dados_sidra

def arima_comercio(path_data, lags, order, average):
    """
    Builds and fits an ARIMA model to the 'pibComercio' series,
    generates a forecast, and saves the resulting DataFrame.

    Parameters:
    - path_data (str): Path to the dataset directory.
    - lags (int): AR term (p) in the ARIMA model.
    - order (int): Differencing term (d) in the ARIMA model.
    - average (int): MA term (q) in the ARIMA model.

    Returns:
    - pd.DataFrame: Original plus forecasted 'pibComercio' data.
    """
    # Load commerce GDP data
    pib_comercio = coleta_dados_sidra.make_pibComercio(path_data)

    # Fit ARIMA model
    model = pm.ARIMA(order=(lags, order, average)).fit(pib_comercio.pibComercio)

    # Forecast next 8 periods
    n_periods = 8
    forecast = model.predict(n_periods=n_periods, return_conf_int=False)
    forecast_df = pd.DataFrame({'pibComercio': forecast}, index=pd.date_range(start=pib_comercio.index[-1] + pd.offsets.MonthBegin(), periods=n_periods, freq='MS'))

    # Combine actual and forecasted data
    df_combined = pd.concat([pib_comercio, forecast_df])
    df_combined = df_combined[df_combined.index >= '2013-12-01']

    # Save to disk
    output_path = os.path.join(path_data, 'interim', 'df_pib_comercio_arima.pkl')
    df_combined.to_pickle(output_path)

    return df_combined


def make_dataset(path_data, lags, order, average, retries=3):
    """
    Creates and saves the full dataset with engineered features and forecasted commerce GDP.
    Includes retry logic in case of data download or processing failures.

    Parameters:
    - path_data (str): Base path for reading and saving data.
    - lags, order, average (int): ARIMA model parameters.
    - retries (int): Number of retries in case of failure.

    Returns:
    - pd.DataFrame: The final assembled dataset.
    """
    dataset = None

    for attempt in range(1, retries + 1):
        try:
            # Feature engineering
            pib = build_features.features_pib(path_data)
            family_consumption = build_features.features_consumoFamilias(path_data)
            industrial_gdp = build_features.features_pibIndustrial(path_data)
            unemployment = build_features.features_desocupacao(path_data)
            ipca = build_features.features_ipca(path_data)
            selic = build_features.features_selic(path_data)
            commerce_gdp = arima_comercio(path_data, lags, order, average)

            # Assemble dataset
            dataset = pd.DataFrame(index=pib.index)
            dataset = (
                dataset
                .join(pib, how='left')
                .join(family_consumption, how='left')
                .join(industrial_gdp, how='left')
                .join(unemployment, how='left')
                .join(ipca, how='left')
                .join(selic, how='left')
                .join(commerce_gdp, how='left')
            )

            # Fill missing values in 'unemployment' after the last valid entry
            last_valid = dataset['desocupacao'].last_valid_index()
            if last_valid:
                last_value = dataset.loc[last_valid, 'desocupacao']
                dataset.loc[last_valid:, 'desocupacao'] = dataset.loc[last_valid:, 'desocupacao'].fillna(last_value)

            # Save final dataset
            output_path = os.path.join(path_data, 'processed', 'df_projecoes.pkl')
            dataset.to_pickle(output_path)
            break  # success, exit retry loop

        except Exception as e:
            print(f"Attempt {attempt} failed with error: {e}")
            if attempt == retries:
                try:
                    print("Loading previously saved dataset as fallback.")
                    fallback_path = os.path.join(path_data, 'processed', 'df_projecoes.pkl')
                    dataset = pd.read_pickle(fallback_path)
                except Exception as final_error:
                    raise RuntimeError("All attempts failed and no backup dataset was found.") from final_error

    return dataset
