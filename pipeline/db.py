"""Módulo de conexão com o projeto_1.duckdb, importado pelas quatro etapas.

SPEC

Expõe exatamente isto, com estes nomes:
  PASTA_RAW                          Path da pasta raw/ com os cinco .csv.gz
  conectar()                         abre o projeto_1.duckdb, criando se faltar
  criar_schema(con, nome)            CREATE SCHEMA IF NOT EXISTS
  gravar(con, df, schema, tabela)    grava e devolve a contagem lida do banco

D1. CAMINHOS
    PASTA_RAW e o projeto_1.duckdb são resolvidos a partir da localização do
    próprio db.py (a pasta acima de pipeline/), nunca do diretório corrente.
    Por quê: rodar o script de outra pasta não pode criar um projeto_1.duckdb
    vazio no lugar errado, que parece ingestão quebrada.

D2. TRÊS FUNÇÕES E UMA CONSTANTE, NADA MAIS
    db.py não lê CSV, não conhece nome de tabela, não imprime nada e não
    converte tipo.
    Por quê: quem decide tudo isso é cada etapa. Se o módulo compartilhado
    decidisse, a decisão ficaria escondida de todas elas.

D3. gravar
    Recria a tabela (CREATE OR REPLACE TABLE schema.tabela) com as colunas e
    os tipos do DataFrame, sem alterar nada: coluna str vira VARCHAR, nulo
    continua NULL. Não deixa view ou tabela temporária para trás. Devolve
    SELECT count(*) da tabela recém-gravada, lido do banco.
    Por quê: a contagem que aparece na tela tem que ser a do que foi gravado,
    não a do que estava na memória. Recriar em vez de acrescentar é o que
    torna a reexecução segura.

D4. CONEXÃO
    conectar() devolve a conexão aberta; quem chama é quem fecha. db.py não
    guarda conexão global.
    Por quê: o arquivo aceita um único processo escrevendo. Uma conexão
    global manteria o lock por mais tempo do que a etapa precisa, e importar
    o módulo já teria efeito colateral.

CRITÉRIOS DE ACEITE
- Importar pipeline.db não abre banco nem cria arquivo nenhum.
- conectar() chamado de dentro de outra pasta cria o projeto_1.duckdb na raiz
  do projeto, e não na pasta corrente.
- gravar() com um DataFrame de 3 linhas devolve 3, e rodar duas vezes continua
  devolvendo 3.
"""

from pathlib import Path

import duckdb
import pandas as pd

PASTA_RAW = Path(__file__).resolve().parent.parent / "raw"


def conectar() -> duckdb.DuckDBPyConnection:
    # O caminho fica aqui dentro porque D2 só admite uma constante no módulo.
    return duckdb.connect(str(PASTA_RAW.parent / "projeto_1.duckdb"))


def criar_schema(con: duckdb.DuckDBPyConnection, nome: str) -> None:
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{nome}"')


def gravar(
    con: duckdb.DuckDBPyConnection, df: pd.DataFrame, schema: str, tabela: str
) -> int:
    con.register("_df_gravar", df)
    try:
        con.execute(
            f'CREATE OR REPLACE TABLE "{schema}"."{tabela}" AS SELECT * FROM _df_gravar'
        )
    finally:
        con.unregister("_df_gravar")
    # fetchone() deixaria o resultado pendente, e o DuckDB recusa o próximo uso
    # da conexão enquanto isso.
    return con.execute(f'SELECT count(*) FROM "{schema}"."{tabela}"').fetchall()[0][0]
