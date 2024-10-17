Ferramenta de Análise de Amostras Granulares vs Agregadas
=========================================================

O objetivo desse trabalho de análise é enxergar se as amostras de indicadores de aprendizado adequado (IAA) dos resultados do SAEB em sua forma agregada (organizada por estados, municípios ou país, e fornecida pelo próprio INEP) pode ser inferida através das amostras granulares (indicadores de cada escola participante da prova), de forma que o mesmo possa ser realizado para futuras iterações da prova.

Metodologia
-----------

Programas escritos em linguagem Python auxiliam na formatação e manipulação das comparações e resultados obtidos da atividade citada na introdução. As principais funcionalidades dos programas estão descritas abaixo:

1\. Carregar, de um banco de dados local, as amostras agregadas por estado/município, para um dado ano.

2\. Carregar, do mesmo banco de dados, as amostras individuais das escolas, baseado no estado/município escolhido, no mesmo ano.

3\. Realizar manualmente as agregações, ajustando os valores das amostras granulares através dos dados do censo, para o mesmo ano. Esse cálculo é feito multiplicando o resultado de cada indicador pelo número de matriculados no censo do ano da prova.

4\. Comparar a diferença obtida entre os valores calculados e os valores das amostras agregadas.

5\. Ao final, resultar em:

5.1. Quatro tabelas de comparações entre valor dos dados agregados x valor dos dados calculados pelas amostras granulares, para os IAA's LP e MAT, para os anos finais e anos iniciais. Essas quatro tabelas correspondem a um ano específico do SAEB.

5.2. Quatro tabelas de subtrações entre valores dos dados agregados x valores dos dados calculados pelas amostras granulares, para os IAA's LP e MAT, para os anos finais e anos iniciais. Essas quatro tabelas correspondem a um ano específico do SAEB.

Descrição dos programas e funções desenvolvidos
-----------------------------------------------

‘data\_loader.py': Realiza as funções de carregamento dos dados a partir do banco sqlite local. Possui funções para:

1\. Obter os dados das amostras agregadas.

2\. Obter os dados das amostras granulares

3\. Obter os dados do censo para um dado ano, concatenando com uma tabela de dados granulares.

4\. Obter os nomes de todos os municípios nas amostras agregadas

5\. Obter os nomes de todos os municípios nas amostras granulares.

‘dataframe\_processor.py’: Realiza as funções de manipulação dos dados tabulares. Possui funções para:

1\. Separar uma tabela em duas, filtradas por anos inicias e anos finais, para os dados granulares.

2\. Separar uma tabela em duas, filtradas por anos inicias e anos finais, para os dados agregados.

3\. Processar uma tabela de dados granulares para fazer as operações descritas no passo 3.

4\. Calcular as diferenças entre os IAAs de dados granulares e dados agregados, guardando em novas tabelas para acesso posterior.

5\. Reorganizar os valores de IAAs de dados granulares e dados agregados, unindo IAAs de mesma categoria e separando de outras categorias, e guardando em novas tabelas para acesso posterior.

‘main.py’: Agrega as funcionalidades de ‘data\_loader.py’ e ‘dataframe\_processor.py’, inicializando as variáveis globais (nomes de municípios, estados, ano...) e chamando as funções na ordem necessária.