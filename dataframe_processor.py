import polars as pl
import polars.selectors as cs

def separate_initial_from_final_years_agg(df_aggregate):
    """ Dado um DF de dados agregados, separar em dois DFs filtrados pela etapa. """

    aggregate_data_initial_years = (
        df_aggregate.filter(
            pl.col('etapa') == 'Anos Iniciais'
        ).select(
            pl.col('lp'),
            pl.col('mat'),
        )
    ).lazy().collect()

    aggregate_data_final_years = (
        df_aggregate.filter(
            pl.col('etapa') == 'Anos Finais'
        ).select(
            pl.col('lp'),
            pl.col('mat'),
        )
    ).lazy().collect()

    return aggregate_data_initial_years, aggregate_data_final_years

def separate_initial_from_final_years_granular(df_granular):
    """ Dado um DF de dados granulares (escolas), separar em dois DFs filtrados pela etapa. """

    granular_data_initial_years = (
        df_granular.filter(
            pl.col('etapa') == 'Anos Iniciais'
        ).select(
            pl.col('nome_escola'),
            pl.col('lp'),
            pl.col('mat'),
            pl.col('QT_MAT_FUND_AI')
        )
    ).lazy().collect()

    granular_data_final_years = (
        df_granular.filter(
            pl.col('etapa') == 'Anos Finais'
        ).select(
            pl.col('nome_escola'),
            pl.col('lp'),
            pl.col('mat'),
            pl.col('QT_MAT_FUND_AF')
        )
    ).lazy().collect()

    return granular_data_initial_years, granular_data_final_years

def process_granular_data(granular_data_df):
    """
    Dado um dataframe com os dados granulares, já filtrado pela etapa (anos iniciais ou anos finais), multiplica as
    colunas MT e LP pela coluna de matriculas, para cada linha do DF, então soma o resultado e divide pelo somatório
    da coluna de matrículas.

    :param granular_data_df: DF com dados granulares já filtrados por etapa.
    :return: DF com colunas 'Media MAT' e 'Media LP', representativos da estimativa de IAA para as escolas daquele
    escopo geografico, etapa e ano.
    """

    processed_df = (
        granular_data_df.select([
            ((pl.col('lp') * cs.last()).sum() / cs.last().sum()).alias('lp'),
            (( pl.col('mat') * cs.last() ).sum() / cs.last().sum() ).alias('mat')
        ])
    )
    return processed_df

def concatenate_columns(df_aggregate, df_granular, df_scatterplot_1, df_scatterplot_2):
    """
    Copia os valores da primeira coluna de df_aggregate e df_granular para a primeira e segunda colunas de
    df_scatterplot_1, respectivamente.

    Também copia os valores da segunda coluna de df_aggregate e df_granular na primeira e segunda colunas de
    df_scatterplot_2, respectivamente.

    Os processos de cópia realizam a mesma concatenação vertical, alterando os df's passados como parâmetro.

    :param df_aggregate: Df com os dados agregados. São esperadas duas colunas com uma linha.
    :param df_granular: Df com os dados granulares. São esperadas duas colunas com uma linha.
    :param df_scatterplot_1: Df a receber as primeiras colunas de df_aggregate e df_granular. Essa nova linha será
    da forma [df_aggregate_column_1, df_granular_column_1]
    :param df_scatterplot_2: Df a receber as segundas colunas de df_aggregate e df_granular. Essa nova linha será
    da forma [df_aggregate_column_2, df_granular_column_2]
    :return: df_scatterplot_1 alterado com uma nova linha com as primeiras colunas de df_aggregate e df_granular.
    df_scatterplot_2 alterado com uma nova linha com as segundas colunas de df_aggregate e df_granular.
    """
    # Saber se os dataframes possuem o shape esperado
    assert df_aggregate.shape == (1, 2), "df_aggregate should have 1 row and 2 columns"
    assert df_granular.shape == (1, 2), "df_granular should have 1 row and 2 columns"

    # Criar novas linhas
    new_row_1 = pl.DataFrame({
        df_scatterplot_1.columns[0]: [df_aggregate.item(0, 0)],
        df_scatterplot_1.columns[1]: [df_granular.item(0, 0)]
    })

    new_row_2 = pl.DataFrame({
        df_scatterplot_2.columns[0]: [df_aggregate.item(0, 1)],
        df_scatterplot_2.columns[1]: [df_granular.item(0, 1)]
    })

    # Concatenar as novas linhas
    df_scatterplot_1 = pl.concat([df_scatterplot_1, new_row_1], how="vertical")
    df_scatterplot_2 = pl.concat([df_scatterplot_2, new_row_2], how="vertical")

    return df_scatterplot_1, df_scatterplot_2

def calculate_differences(df_aggregate, df_granular, df_diff_1, df_diff_2):
    """
    Calcula a diferença entre a primeira coluna de df_aggregate e df_granular, assim  como a diferença entre a
    segunda coluna dos mesmos dfs. Guarda essas diferenças em df_diff_1 e df_diff_2, respectivamente, concatenando
    verticalmente nesses DFs (ou seja, alterando-os).

    :param df_aggregate: Df com os dados agregados. São esperadas duas colunas com uma linha.
    :param df_granular: Df com os dados granulares. São esperadas duas colunas com uma linha.
    :param df_diff_1: O Df a receber a diferença entre as primeiras colunas de aggregate e granular.
    :param df_diff_2 O Df a receber a diferença entre as segundas colunas de aggregate e granular.
    :return: df_diff_1 alterado com uma nova linha com a diferença entre as primeiras colunas de df_aggregate e
    df_granular. df_diff_2 alterado com uma nova linha com a diferença entre as segundas colunass de df_aggregate e
    df_granular.
    """
    # Saber se os dataframes possuem o shape esperado
    assert df_aggregate.shape == (1, 2), "df_aggregate should have 1 row and 2 columns"
    assert df_granular.shape == (1, 2), "df_granular should have 1 row and 2 columns"

    diff_df = df_aggregate - df_granular

    # Calcular as diferenças
    diff_1 = diff_df.select(pl.col(diff_df.columns[0]))
    diff_2 = diff_df.select(pl.col(diff_df.columns[1]))

    print(diff_1)
    print(diff_2)

    # Concatenar as diferenças aos DFs
    df_diff_1 = pl.concat([df_diff_1, diff_1], how="vertical")
    df_diff_2 = pl.concat([df_diff_2, diff_2], how="vertical")

    return df_diff_1, df_diff_2




    return