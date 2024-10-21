# Esse script limpa os dados do censo, deixando apenas as colunas de interesse.

import polars as pl

def load_census_data(year):
    path = f'data/microdados_ed_basica_{year}.csv'
    
    df = ( pl.scan_csv(path,  ignore_errors=True,
                truncate_ragged_lines=True, separator=';').select([
            pl.col('CO_ENTIDADE'),
            pl.col('QT_MAT_FUND_AI'),
            pl.col('QT_MAT_FUND_AF')
        ])
    )
    
    return df


years = ['2017', '2019', '2021']

for year in years:
    data = load_census_data(year)
    data.sink_csv(f'microdados_ed_basica_{year}.csv', include_header=True, separator=',')
