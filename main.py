import polars as pl

import data_loader as dl
import dataframe_processor as dp

#######################################################################################################################
# VARIÁVEIS GLOBAIS
#######################################################################################################################

STATE = ''
ADMIN_DEP = 'Total'
# Nomes dos municipios são diferentes entre amostras agregadas e amostras das escolas.
MUNICIPALITY_AGG = 'Ribeirão Preto'
MUNICIPALITY_SCHOOL = 'Ribeirão preto'
YEAR = '2021'

#######################################################################################################################

########################################################################################################################
# DEFINIR OS OITO DATAFRAMES A SEREM RESULTADOS NO FINAL
########################################################################################################################
# Diferença de LP da amostra agregada x calculado para anos iniciais
diff_lp_initial_years = pl.DataFrame({'lp' : []}, schema={'lp' : pl.Float64})
# Diferença de LP da amostra agregada x calculado para anos finais
diff_lp_final_years = pl.DataFrame({'lp' : []}, schema={'lp' : pl.Float64})
# Diferença de MAT da amostra agregada x calculado para anos iniciais
diff_mat_initial_years = pl.DataFrame({'mat' : []}, schema={'mat' : pl.Float64})
# Diferença de MAT da amostra agregada x calculado para anos finais
diff_mat_final_years = pl.DataFrame({'mat' : []}, schema={'mat' : pl.Float64})

# Valor de LP da amostra agregada e valor de LP calculado das amostras granulares para os anos iniciais
# para ser usado no scatterplot.
scatterplot_lp_initial_years = pl.DataFrame({'value_agg' : [] , 'value_gran' : []},
                                            schema={'value_agg' : pl.Float64, 'value_gran': pl.Float64})
# Valor de LP da amostra agregada e valor de LP calculado das amostras granulares para os anos finais
# para ser usado no scatterplot.
scatterplot_lp_final_years = pl.DataFrame({'value_agg' : [] , 'value_gran' : []},
                                          schema={'value_agg' : pl.Float64, 'value_gran': pl.Float64})
# Valor de MAT da amostra agregada e valor de MAT calculado das amostras granulares para os anos iniciais
# para ser usado no scatterplot.
scatterplot_mat_initial_years = pl.DataFrame({'value_agg' : [] , 'value_gran' : []},
                                             schema={'value_agg' : pl.Float64, 'value_gran': pl.Float64})
# Valor de MAT da amostra agregada e valor de MAT calculado das amostras granulares para os anos finais
# para ser usado no scatterplot.
scatterplot_mat_final_years = pl.DataFrame({'value_agg' : [] , 'value_gran' : []},
                                           schema={'value_agg' : pl.Float64, 'value_gran': pl.Float64})
########################################################################################################################

#######################################################################################################################
# CARREGAR OS DADOS E REALIZAR AS OPERAÇÕES
#######################################################################################################################
aggregate_data = dl.load_aggregate_data(MUNICIPALITY_AGG, STATE, ADMIN_DEP, YEAR)

# A partir daqui, os dados agregados já estão separados em anos finais e iniciais.
aggregate_initial_years, aggregate_final_years = dp.separate_initial_from_final_years_agg(aggregate_data)

print('Aggregate data initial years:')
print(aggregate_initial_years)

print('Aggregate data final years:')
print(aggregate_final_years)

# Os dados granulares precisam ser carregados, unidos aos dados do censo, e separados em anos finais e anos iniciais.
granular_data = dl.load_granular_data(MUNICIPALITY_SCHOOL, STATE, ADMIN_DEP, YEAR)

granular_data = dl.load_census_data(granular_data, YEAR)

granular_initial_years, granular_final_years = dp.separate_initial_from_final_years_granular(granular_data)

granular_initial_years = dp.process_granular_data(granular_initial_years)
granular_final_years = dp.process_granular_data(granular_final_years)

print('Initial Years Granular Data Post-processing:')
print(granular_initial_years)

print('Final Years Granular Data Post-processing:')
print(granular_final_years)
#######################################################################################################################

#######################################################################################################################
# PEGAR OS IAAs DOS DADOS AGREGADOS E DOS CALCULADOS PELOS DADOS GRANULARES E INSERIR AS DIFERENÇAS E VALORES
# NA INTEGRA NOS DATAFRAMES CORRESPONDENTES
#######################################################################################################################

diff_lp_initial_years, diff_mat_initial_years = dp.calculate_differences( aggregate_initial_years, granular_initial_years
                                                                         , diff_lp_initial_years, diff_mat_initial_years)

diff_lp_final_years, diff_mat_final_years = dp.calculate_differences(aggregate_final_years, granular_final_years
                                                                     , diff_lp_final_years, diff_mat_final_years)

scatterplot_lp_initial_years, scatterplot_mat_initial_years = dp.concatenate_columns(aggregate_initial_years, granular_initial_years,
                                                      scatterplot_lp_initial_years, scatterplot_mat_initial_years)

scatterplot_lp_final_years, scatterplot_mat_final_years = dp.concatenate_columns(aggregate_final_years, granular_final_years,
                                                                                 scatterplot_lp_final_years, scatterplot_mat_final_years)



