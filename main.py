import polars as pl

import data_loader as dl
import dataframe_processor as dp
import data_viz as dv

#######################################################################################################################
# VARIÁVEIS GLOBAIS
#######################################################################################################################

STATE = ''
ADMIN_DEP = 'Total'
# Nomes dos municipios são diferentes entre amostras agregadas e amostras das escolas, mas códigos de municipio não
municipios_codes_list = dl.load_all_municipalities_agg().to_series(0).to_list()
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
invalid_municipalities = []

########################################################################################################################

#######################################################################################################################
# CARREGAR OS DADOS E REALIZAR AS OPERAÇÕES
#######################################################################################################################
    
for cod_municipio in municipios_codes_list:
    print(f'Fazendo o município {cod_municipio}')
    
    aggregate_data = dl.load_aggregate_data(cod_municipio, STATE, ADMIN_DEP, YEAR)
        
    # Os dados granulares precisam ser carregados, unidos aos dados do censo, e separados em anos finais e anos iniciais.
    granular_data = dl.load_granular_data(cod_municipio, STATE, ADMIN_DEP, YEAR)

    
    # A partir daqui, os dados agregados já estão separados em anos finais e iniciais.
    aggregate_initial_years, aggregate_final_years = dp.separate_initial_from_final_years_agg(aggregate_data)
    
    if aggregate_final_years.shape != (1, 2) or aggregate_initial_years.shape != (1,2):
        invalid_municipalities.append((cod_municipio, 'Shape Errado'))
        continue

    granular_data = dl.load_census_data(granular_data, YEAR)
    
    if granular_data.shape[1] != 7:
        invalid_municipalities.append((cod_municipio, 'Shape Errado'))
        continue 
    
    granular_initial_years, granular_final_years = dp.separate_initial_from_final_years_granular(granular_data)
    
    granular_initial_years = dp.process_granular_data(granular_initial_years)
    granular_final_years = dp.process_granular_data(granular_final_years)
  
    if granular_final_years.shape != (1,2) or granular_initial_years.shape != (1,2):
        invalid_municipalities.append((cod_municipio, 'Shape Errado'))
        continue
    
    
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

    print(f'Finalizado para o município {cod_municipio}')


dv.show_error(diff_lp_final_years, 'dif_lp_af')
dv.show_error(diff_lp_initial_years, 'dif_lp_ai')
dv.show_error(diff_mat_initial_years, 'dif_mat_ai')
dv.show_error(diff_mat_final_years, 'dif_mat_af')

dv.show_comp_scatterplot(scatterplot_lp_final_years, 'lp_af')
dv.show_comp_scatterplot(scatterplot_lp_initial_years, 'lp_ai')
dv.show_comp_scatterplot(scatterplot_mat_initial_years, 'mat_ai')
dv.show_comp_scatterplot(scatterplot_mat_final_years, 'mat_af')

df = pl.DataFrame(
    {
        "cod_municipio": [t[0] for t in invalid_municipalities],
        "message": [t[1] for t in invalid_municipalities]
    }
)

df.write_csv('data/erros_municipios.csv')

diff_lp_final_years.write_csv('data/dif_lp_af.csv')
diff_lp_initial_years.write_csv('data/dif_lp_ai.csv')
diff_mat_final_years.write_csv('data/dif_mat_af.csv')
diff_mat_initial_years.write_csv('data/dif_mat_ai.csv')

scatterplot_lp_final_years.write_csv('data/scatter_lp_af.csv')
scatterplot_lp_initial_years.write_csv('data/scatter_lp_ai.csv')
scatterplot_mat_final_years.write_csv('data/scatter_mat_af.csv')
scatterplot_mat_initial_years.write_csv('data/scatter_mat_ai.csv')







