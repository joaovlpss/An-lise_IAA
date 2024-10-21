import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import os


def show_error(error_df, name):
    """
    Dado um dataframe com uma coluna de números float, plota o histograma
    desse dataframe com o flavor de "erro de amostras agregadas vs calculadas"

    :param error_df: Um dataframe com uma única coluna de números ponto flutuante.
    :param name: Nome para salvar o arquivo de imagem.
    :returns: O histograma correspondente.
    """
    error_list = error_df.to_series(0).to_list()

    plt.figure(figsize=(10, 6))
    sns.histplot(data=error_list, binwidth=0.5,
                 binrange=[min(error_list) - 1, max(error_list) + 1],
                 stat='frequency', color='purple')

    plt.title('Erros das amostras calculadas vs amostras agregadas')
    plt.xlabel('Valores de erro')
    plt.ylabel('Frequência')

    os.makedirs('data', exist_ok=True)

    # Salva o gráficox
    plt.savefig(f'data/error_{name}.png')
    plt.close()


def show_comp_scatterplot(comp_df, name):
    """
    Dado um dataframe com duas colunas de números float, plota o scatterplot
    desse dataframe com a primeira coluna no eixo Y e a segunda coluna no eixo X.

    :param comp_df: Um dataframe com duas colunas de valores float.
    :param name: Nome para salvar o arquivo de imagem.
    :returns: O scatterplot correspondente com a primeira coluna no eixo Y e a segunda no eixo X.
    """
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=comp_df, x=comp_df.columns[1], y=comp_df.columns[0])

    plt.title('Comparação de valores')
    plt.xlabel('Valores calculados')
    plt.ylabel('Valores agregados')

    os.makedirs('data', exist_ok=True)

    # Salva o gráfico
    plt.savefig(f'data/scatter_{name}.png')
    plt.close()