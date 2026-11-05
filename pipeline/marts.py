"""Etapa 3: cria uma tabela no schema marts para cada arquivo de sql/marts/.

Executar com: uv run python -m pipeline.marts

SPEC

1. ESCOPO
   Lê de refined, escreve no schema marts. Nunca escreve em raw nem em refined.
   O schema marts é apagado e recriado a cada execução (DROP SCHEMA IF EXISTS
   marts CASCADE).
   Por quê: um mart cujo arquivo foi apagado não pode continuar existindo no
   banco.

2. UM ARQUIVO, UMA TABELA
   Lê todos os sql/marts/*.sql em ordem alfabética.
   Nome da tabela = nome do arquivo sem a extensão e sem o prefixo numérico:
     sql/marts/01_vendas_anuais.sql -> marts.vendas_anuais
   Executa: CREATE TABLE marts.<nome> AS <conteúdo do arquivo>
   Tira espaços e um ponto e vírgula do final do conteúdo, e nada mais.
   O Python não altera, não monta e não completa a SQL.
   Por quê: a regra de negócio mora inteira no arquivo .sql.

3. SAÍDA NA TELA
   Uma linha por mart, com a contagem vinda do banco, milhar com ponto:
     ✓ marts.vendas_anuais        24 linhas
   Linha final com o número de marts e o total de linhas.
   stdout reconfigurado para utf-8.
   Por quê: a contagem na tela tem que ser a do que foi gravado, e no Prompt de
   Comando do Windows o ✓ e os acentos saem embaralhados por padrão.

4. ERRO
   Antes de tudo: se faltar qualquer uma das cinco tabelas em refined, lista
   quais, diz para rodar "uv run python -m pipeline.transformacao" e encerra com
   sys.exit(1).
   Por quê: sem refined completo, o erro apareceria num .sql que está certo.

   Se um .sql falhar: mostra o nome do arquivo e a mensagem do DuckDB, não
   executa os seguintes, sys.exit(1).
   Por quê: o nome do arquivo é o que leva direto ao que precisa ser corrigido.

   Se um mart voltar com zero linhas: mostra o nome e encerra com sys.exit(1).
   Por quê: mart vazio é quase sempre um JOIN que não casou, e um dashboard vazio
   parece um negócio parado.

   Se sql/marts/ não tiver nenhum arquivo: avisa e sys.exit(1).
   Por quê: sem esta regra, o schema marts seria apagado e recriado vazio.

   Antes de executar qualquer SQL: arquivo sem prefixo NN_ ou dois arquivos que
   dão o mesmo nome de tabela -> lista quais e sys.exit(1).
   Por quê: o nome da tabela vem do nome do arquivo; se ele estiver errado, o
   problema é da pasta, e não há por que tocar no banco.

   Tudo roda numa transação só: se qualquer passo falhar (inclusive o mart
   vazio), o schema marts volta a ser o da última execução que deu certo.
   Por quê: um schema pela metade quebra o dashboard sem avisar.

CRITÉRIOS DE ACEITE
- Com um arquivo sql/marts/00_teste.sql contendo "SELECT 1 AS x", cria marts.teste
  com 1 linha.
- Apagar o arquivo e rodar de novo faz marts.teste desaparecer.
- Um arquivo com erro de sintaxe para tudo, com o nome do arquivo na tela, e marts
  continua exatamente como estava antes da execução.
- Um arquivo teste.sql, sem prefixo, para antes de executar qualquer SQL.
- Rodar duas vezes seguidas imprime exatamente a mesma coisa.
"""

import re
import sys
from pathlib import Path

import duckdb

from pipeline.db import PASTA_RAW, conectar, criar_schema
from pipeline.ingestao import formatar

ORIGEM = "refined"
DESTINO = "marts"
TABELAS_ORIGEM = (
    "clientes",
    "itens_divergentes",
    "itens_pedido",
    "pedidos_por_cliente",
    "produtos",
)
PASTA_MARTS = PASTA_RAW.parent / "sql" / "marts"
PADRAO_ARQUIVO = re.compile(r"\d{2}_(.+)\.sql", re.IGNORECASE)

# Com a contagem de 7 posições, reproduz o espaçamento do exemplo da seção 3.
LARGURA_MINIMA_NOME = 21


def verificar_refined(con: duckdb.DuckDBPyConnection) -> None:
    existentes = {
        linha[0]
        for linha in con.execute(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{ORIGEM}'"
        ).fetchall()
    }
    faltando = [tabela for tabela in TABELAS_ORIGEM if tabela not in existentes]
    if faltando:
        print(f"Tabelas não encontradas no schema {ORIGEM}:")
        for tabela in faltando:
            print(f"  {tabela}")
        print("Rode antes: uv run python -m pipeline.transformacao")
        sys.exit(1)


def listar_arquivos() -> list[tuple[Path, str]]:
    arquivos = sorted(
        (caminho for caminho in PASTA_MARTS.glob("*.sql") if caminho.is_file()),
        key=lambda caminho: caminho.name,
    )
    if not arquivos:
        print(f"Nenhum arquivo .sql em {PASTA_MARTS}")
        sys.exit(1)

    sem_prefixo = []
    por_nome: dict[str, list[str]] = {}
    marts = []
    for caminho in arquivos:
        achado = PADRAO_ARQUIVO.fullmatch(caminho.name)
        if achado is None:
            sem_prefixo.append(caminho.name)
            continue
        nome = achado.group(1)
        marts.append((caminho, nome))
        # DuckDB não diferencia maiúsculas em nome de tabela.
        por_nome.setdefault(nome.lower(), []).append(caminho.name)

    repetidos = [nomes for nomes in por_nome.values() if len(nomes) > 1]
    if sem_prefixo or repetidos:
        if sem_prefixo:
            print("Arquivos sem o prefixo NN_:")
            for nome in sem_prefixo:
                print(f"  {nome}")
        if repetidos:
            print("Arquivos que dão o mesmo nome de tabela:")
            for nomes in repetidos:
                print(f"  {', '.join(nomes)}")
        sys.exit(1)
    return marts


def criar_marts(
    con: duckdb.DuckDBPyConnection, marts: list[tuple[Path, str]]
) -> list[tuple[str, int]]:
    con.execute(f"DROP SCHEMA IF EXISTS {DESTINO} CASCADE")
    criar_schema(con, DESTINO)

    contagens = []
    for caminho, nome in marts:
        try:
            conteudo = caminho.read_text(encoding="utf-8")
            sql = conteudo.rstrip().removesuffix(";")
            con.execute(f"CREATE TABLE {DESTINO}.{nome} AS {sql}")
            linhas = con.execute(f"SELECT count(*) FROM {DESTINO}.{nome}").fetchall()
        except (duckdb.Error, UnicodeDecodeError) as erro:
            print(f"Falha em {caminho.name}:")
            print(erro)
            sys.exit(1)
        if linhas[0][0] == 0:
            print(f"Mart vazio: {DESTINO}.{nome} ({caminho.name})")
            sys.exit(1)
        contagens.append((f"{DESTINO}.{nome}", linhas[0][0]))
    return contagens


def imprimir_resultado(contagens: list[tuple[str, int]]) -> None:
    largura = max(LARGURA_MINIMA_NOME, *(len(nome) for nome, _ in contagens))
    for nome, linhas in contagens:
        print(f"✓ {nome:<{largura}} {formatar(linhas):>7} linhas")
    total = sum(linhas for _, linhas in contagens)
    print(f"Total: {formatar(len(contagens))} marts, {formatar(total)} linhas")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    con = conectar()
    try:
        verificar_refined(con)
        marts = listar_arquivos()

        con.begin()
        try:
            contagens = criar_marts(con, marts)
            con.commit()
        # BaseException porque as paradas previstas são sys.exit (SystemExit).
        except BaseException:
            con.rollback()
            raise
    finally:
        con.close()

    # Só depois do commit: um ✓ antes dele poderia ser desfeito pelo rollback.
    imprimir_resultado(contagens)


if __name__ == "__main__":
    main()
