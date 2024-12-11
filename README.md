# Título
Detecção de anomalias entre odds de diferentes casas de apostas

# Conhecimentos em prática
Python; PostgreSQL; OOP; Web Scraping.

# Introdução
_Ao invés de criar modelos para competir com as casas de apostas, por que não utilizar as próprias previsões delas para ter bons resultados com as apostas?_

As odds são inversamente proporcionais às chances de acontecimento dos eventos esportivos. Eventos mais prováveis (menos prováveis) oferecerão um prêmio menor (maior). Contudo, as casas de apostas podem discordar das probabilidades de ocorrência dos eventos esportivos, gerando discrepâncias entre as odds oferecidas pelo mercado. Assim podemos fazer um ensemble com as odds (previsões) disponibilizadas e tornar as estimativas de ocorrência dos eventos mais precisas. Comparando cada odd com seu valor justo, podemos encontrar erros de precificação que gerem uma expectativa de retorno positiva no longo prazo. _E é exatamente aqui que este projeto se encaixa!_

# Visão Geral do Projeto

Neste projeto, eu realizo a coleta e o processamento (limpeza/tratamento e enriquecimento) dos dados das odds in-play 1X2, extraídos diretamente dos sites de diferentes casas de apostas. Os dados não estruturados extraídos são então transformados para integrar um Banco de Dados SQL (PostgreSQL).

Além disso, as informações são integradas de modo a reconhecer eventos idênticos como o mesmo evento, ainda que representados de formas diferentes entre os sites. Um falso-positivo, isto é, tratar dois eventos distintos como idênticos, pode ser muito prejudicial: pode indicar a existência de anomalias onde não existe. Um falso-negativo (tratar dois eventos idênticos como distintos) pode ter consequências negativas da mesma forma.

Por fim, as odds de um mesmo evento são combinadas para estimar o valor justo da probabilidade de ocorrência do evento. Essa informação, então, é utilizada para detectar erros de precificação (anomalias) que podem ser valiosos para os apostadores.

O intervalo de ingestão e obtenção das possíveis anomalias é definido como 60 segundos por padrão, mas pode ser facilmente alterado no parâmetro “sec_frequency_collection” em Config\general.json.

# Coleta

A coleta é realizada via Web Scraping. As bibliotecas utilizadas são: Selenium e bs4. Por enquanto (dezembro/2024) a extração de dados envolve os sites das casas de apostas: Betway, Betfair e 1xbet. O cadastro de novos sites está em andamento, assim como a operação das diferentes fontes em paralelo. 

# Processamento

O processamento utiliza a biblioteca Pandas. A integração dos dados se dá pelo nome dos times após uma série de etapas de normalização, que levam em consideração os padrões de apresentação de cada site. Para integrar o Python com o banco de dados na nuvem é utilizada a biblioteca psycopg2.

# Estimação do Valor Justo

(Em andamento). Utilizará o método do IQR, robusto a outliers. Odds superiores ao Q3 + 1,5 * IQR serão identificadas como anômalas.

# Banco de Dados

O SGBD PostgreSQL foi utilizado neste projeto. Além das anomalias identificadas, a série temporal das odds também é armazenada. Isso possibilita futuras análises em relação aos padrões de comportamento das estimativas.

Cada fonte é cadastrada como um schema, que conterá suas próprias tabelas. Dentro do schema, cada esporte tem sua própria série de tabelas: {esporte}_competitions, {esporte}_teams, {esporte}_games, {esporte}_odds e {esporte}_scores. A escolha de criar uma tabela separada para cada esporte, ao invés de dicionar uma coluna "sport_id", teve como finalidade otimizar as consultas (tabelas menores e mais específicas).

Também existe o schema main, que integra as informações relativas aos diversos schemas, como as fontes, esportes, países, moeda, bem como a tabela de controle. 


# Observabilidade

Cada operação bem-sucedida no banco de dados (cadastro de novos jogos, odds, etc) insere dados em uma outra tabela de controle: isso permite a rápida visualização do andamento da coleta e processamento dos dados, assim como possíveis quebras no pipeline. 

Esses fatores são essenciais tendo em vista a futura adição de novas fontes e novos esportes no projeto. É impraticável acompanhar tamanho volume de informação de forma manual, além de ser muito prejudicial não identificar bugs prontamente. Tendo isso em vista, um dashboard em Power BI está sendo desenvolvido para possibilitar uma visualização do status do pipeline em tempo real.

OBS: Os dados da tabela de controle são apagados conforme se tornam obsoletos (isto é, quando o jogo finaliza e as apostas são liquidadas). Isso permite controlar os custos de armazenamento.

# Disclaimer

Esse projeto tem propósito educacional e foi realizado com o propósito de praticar meus conhecimentos em Engenharia de Dados e integrar meu portfólio. Sua utilização está permitida unicamente mediante autorização por escrito.

# Referências

KAUNITZ, L.; ZHONG, S.; KREINER, J. Beating the bookies with their own numbers and how the online sports betting market is rigged. arXivpreprint arXiv:1710.02824, 2017.

MATEJEK, B. A Computational Analysis of Arbitrage Opportunities in Sports Gambling. 2013. Disponível em: https://api.semanticscholar.org/CorpusID:17241865.
