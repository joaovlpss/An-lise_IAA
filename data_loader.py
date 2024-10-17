import polars as pl

def query_aggregate_data(municipality='', state='', admin_dependency='Total', year='2017'):
    """
    Dado um escopo de localização (estado ou município) e rede (estadual, municipal, federal ou total) e um ano,
    retorna uma query SQL que recupera todas as amostras agregadas daquele escopo, rede e ano.

    Esse méto.do só deve ser chamado internamente.

    :param municipality: Nome completo do município em PT-BR (mutualmente exclusivo com estado).
    :param state: Abreviação da UF do estado (Ex: SP) (mutualmente exclusivo com municipio).
    :param admin_dependency: Dep. admimnistrativa das escolas (pode ser 'Total') para todas.
    :param year: Ano de interesse.
    :return: Query SQL para pesquisar o escopo desejado no banco local.
    """
    if municipality and state:
        raise ValueError('Can only query aggregate data from either a state or a municipality.')

    if municipality:
        return f"""
        SELECT nome_municipio, etapa, lp, mat FROM indicadores_aprendizado_adequado iaa 
        INNER JOIN amostras_municipios am ON iaa.id_amostra = am.id 
        WHERE (iaa.ano = '{year}')
            AND (iaa.localizacao = 'Total')
            AND (iaa.rede = '{admin_dependency}')
            AND (iaa.etapa != 'Ensino Médio')
            AND (am.nome_municipio = '{municipality}')
        """
    elif state:
        return f"""
            SELECT uf, etapa, lp, mat FROM indicadores_aprendizado_adequado iaa 
            INNER JOIN amostras_estados ae  ON iaa.id_amostra = ae.id 
            WHERE (iaa.ano = '{year}')
                AND (iaa.localizacao = 'Total')
                AND (iaa.rede = '{admin_dependency}')
                AND (iaa.etapa != 'Ensino Médio')
                AND (ae.uf = '{state}')
        """

def query_granular_data(municipality='', state='', admin_dependency='Total', year='2017'):
    """
    Dado um escopo de localização (estado ou município) e rede (estadual, municipal, federal ou total) e um ano,
    retorna uma query SQL que recupera todas as amostras granulares daquele escopo, rede e ano.

    Esse méto.do só deve ser chamado internamente.

    :param municipality: Nome completo do município em PT-BR (mutualmente exclusivo com estado).
    :param state: Abreviação da UF do estado (Ex: SP) (mutualmente exclusivo com municipio).
    :param admin_dependency: Dep. admimnistrativa das escolas (pode ser 'Total') para todas.
    :param year: Ano de interesse.
    :return: Query SQL para pesquisar o escopo desejado no banco local.
    """
    if municipality and state:
        raise ValueError('Can only query granular data from either a state or a municipality.')

    if municipality and (admin_dependency == 'Total'):
        return f"""
                SELECT ae.id, nome_escola, etapa, lp, mat FROM indicadores_aprendizado_adequado iaa 
                INNER JOIN amostras_escolas ae ON iaa.id_amostra = ae.id 
                WHERE (iaa.ano = '{year}')
                    AND (iaa.etapa != 'Ensino Médio')
                    AND (ae.nome_municipio = '{municipality}')
                """
    elif municipality and (admin_dependency != 'Total'):
        return f"""
                SELECT ae.id, nome_escola, etapa, lp, mat FROM indicadores_aprendizado_adequado iaa 
                    INNER JOIN amostras_escolas ae ON iaa.id_amostra = ae.id 
                    WHERE (iaa.ano = '{year}')
                        AND (iaa.rede = '{admin_dependency}')
                        AND (iaa.etapa != 'Ensino Médio')
                        AND (ae.nome_municipio = '{municipality}')
                """

    if state and (admin_dependency == 'Total'):
        return f"""
                SELECT ae.id, nome_escola, etapa, lp, mat FROM indicadores_aprendizado_adequado iaa 
                INNER JOIN amostras_escolas ae ON iaa.id_amostra = ae.id 
                WHERE (iaa.ano = '{year}')
                    AND (iaa.etapa != 'Ensino Médio')
                    AND (ae.uf = '{state}')
                """
    elif state and (admin_dependency != 'Total'):
        return f"""
                SELECT ae.id, nome_escola, uf, etapa, lp, mat FROM indicadores_aprendizado_adequado iaa 
                    INNER JOIN amostras_escolas ae ON iaa.id_amostra = ae.id 
                    WHERE (iaa.ano = '{year}')
                        AND (iaa.etapa != 'Ensino Médio')
                        AND (iaa.rede = '{admin_dependency}')
                        AND (ae.uf = '{state}')
        """

def load_aggregate_data(municipality='', state='', admin_dependency='Total', year='2017'):
    """
    Dado um escopo de município ou estado, dep. administrativa e ano, retorna um dataframe polars com os dados
    desejados, a partir das amostras agregadas.

    :param municipality: Nome completo do município. Mutualmente exclusivo com estado.
    :param state: Abreviação da Unidade Federativa (ex: 'SP'). Mutualmente exclusivo com municipio.
    :param admin_dependency: Dep. Administrativa desejada. Pode ser 'total'.
    :param year: Ano de interesse
    :return: Dataframe polars com os dados de interesse.
    """
    uri = "sqlite://data/banco_pca.sqlite3"
    query = query_aggregate_data(municipality=municipality, state=state, admin_dependency=admin_dependency, year=year)
    return pl.read_database_uri(query=query, uri=uri)

def load_granular_data(municipality='', state='', admin_dependency='Total', year='2017'):
    """
    Dado um escopo de município ou estado, dep. administrativa e ano, retorna um dataframe polars com os dados
    desejados, a partir das amostras granulares (ou seja, das escolas individuais).

    :param municipality: Nome completo do município. Mutualmente exclusivo com estado.
    :param state: Abreviação da Unidade Federativa (ex: 'SP'). Mutualmente exclusivo com municipio.
    :param admin_dependency: Dep. Administrativa desejada. Pode ser 'total'.
    :param year: Ano de interesse
    :return: Dataframe polars com os dados de interesse.
    """
    uri = "sqlite://data/banco_pca.sqlite3"
    query = query_granular_data(municipality=municipality, state=state, admin_dependency=admin_dependency, year=year)
    return pl.read_database_uri(query=query, uri=uri)

def load_census_data(df_granular_data, year=''):
    """
    Dado um dataframe com os dados granulares da escola, e o ano a qual esses dados se referem, concatena ao dataframe
    os dados do censo escolar no mesmo ano, para todas (e somente) as escolas representadas dataframe. Utiliza a coluna
    'id' da tabela de dados granulares para fazer a filtragem.

    :param df_granular_data: Um dataframe de dados granulares, advindo de `load_granular_data()`.
    :param year: Ano a qual o DF se refere.
    :return: O mesmo dataframe, com duas novas colunas: QT_MAT_FUND_AI e QT_MAT_FUND_AF, com as matriculas dos anos
    iniciais e finais, respectivamente.
    """
    census_df = (
        pl.read_csv(f'data/microdados_ed_basica_{year}.csv', ignore_errors=True, encoding='latin1',
                    truncate_ragged_lines=True, separator=';')
        .select([
            pl.col('CO_ENTIDADE'),
            pl.col('QT_MAT_FUND_AI'),
            pl.col('QT_MAT_FUND_AF')
        ])
        .filter(
            pl.col('CO_ENTIDADE').is_in(df_granular_data['id'])
        )
    ).lazy().collect()

    census_df = census_df.rename({'CO_ENTIDADE': 'id'})

    df_granular_data = df_granular_data.join(census_df, on='id', how='left')

    return df_granular_data

def load_all_municipalities_agg():
    """Retorna um dataframe com uma coluna com todos os municípios do banco de dados."""

    query = """SELECT DISTINCT nome_municipio FROM amostras_municipios am"""
    uri = "sqlite://data/banco_pca.sqlite3"
    return pl.read_database_uri(query=query, uri=uri)

def load_all_municipalities_gran():
    """Retorna um dataframe com uma coluna com todos os municípios do banco de dados."""

    query = """SELECT DISTINCT nome_municipio FROM amostras_escolas ae"""
    uri = "sqlite://data/banco_pca.sqlite3"
    return pl.read_database_uri(query=query, uri=uri)