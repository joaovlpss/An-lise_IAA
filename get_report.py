import pandas as pd
import numpy as np

# Listar arquivos e nomes das colunas
files = [
    ('data/dif_lp_ai.csv', 'lp'),
    ('data/dif_lp_af.csv', 'lp'),
    ('data/dif_mat_ai.csv', 'mat'),
    ('data/dif_mat_af.csv', 'mat')
]


def print_stats(df, file_name, column_name):
    print(f"Column info:\n{df[column_name].describe()}\n")

    # Checar valores infinitos (nunca deve ocorrer)
    inf_count = np.isinf(df[column_name]).sum()
    print(f"Number of infinite values: {inf_count}")

    # Removes valores infinitos (nunca deve ocorrer)
    if inf_count > 0:
        df = df[np.isfinite(df[column_name])]

    # Calcular estatísticas
    mean = df[column_name].mean()
    std = df[column_name].std()

    print(f"{file_name} - {column_name.upper()}:")
    print(f"Média: {mean}")
    print(f"Desvio Padrão: {std}")
    print()


for file_name, column_name in files:
    df = pd.read_csv(file_name)

    # Se houverem problemas de null, tentar limpar os dados.
    null_count = df[column_name].isnull().sum()
    if null_count > 0:
        print(f"Aviso: {null_count} null values achados em {file_name}")
        df = df.dropna(subset=[column_name])

    if len(df) > 0:
        print_stats(df, file_name, column_name)
    else:
        print(f"Error: Sem dados válidos em {file_name}")

    # Print the first few rows for debugging
    print("Primeiras colunas:")
    print(df.head())
    print("\nÚltimas colunas:")
    print(df.tail())
    print("\n" + "=" * 50 + "\n")