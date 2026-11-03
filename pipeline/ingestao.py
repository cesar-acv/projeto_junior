"""Etapa 1: copia os cinco .csv.gz de raw/ para o schema raw do banco.

Executar com: uv run python -m pipeline.ingestao

SPEC

1. ESCOPO E NOMES
   Schema de destino: raw
   Mapa arquivo -> tabela, completo e literal:
     ecommerce_sales_customer_analytics_150k.csv.gz -> pedidos_por_cliente
     order_items.csv.gz                             -> itens_pedido
     customer_master.csv.gz                         -> clientes
     dataset_statistics.csv.gz                      -> estatisticas
     product_catalog.csv.gz                         -> produtos
   Por quê: os nomes vão para o português porque é o vocabulário digitado de
   memória num JOIN nas próximas aulas.

2. PROIBIÇÕES (a regra central desta etapa)
   Nenhuma conversão de tipo. Nenhuma linha descartada ou deduplicada. Nenhum
   nome de coluna alterado. Nenhum valor corrigido, preenchido ou normalizado.
   Data continua texto, nulo continua nulo.
   Por quê: a fronteira entre "o dado que chegou" e "o dado que eu decidi"
   precisa existir em algum lugar, e esse lugar é a passagem de raw para
   refined, na etapa 2. Se a correção acontecer aqui, a fronteira some.

3. REEXECUÇÃO
   Rodar dez vezes dá o mesmo resultado que rodar uma: a tabela é recriada
   (CREATE OR REPLACE), nunca acrescentada.
   Por quê: é o que permite errar sem medo.

4. LEITURA DO ARQUIVO
   encoding declarado como utf-8, nunca adivinhado. Os .csv.gz são lidos
   direto, sem descompactar. Campo vazio continua virando nulo; não vira string
   vazia.
   Por quê: adivinhar encoding pode embaralhar acento sem erro nenhum, e string
   vazia esconderia a ausência do dado que a etapa 2 precisa enxergar.

5. TIPAGEM
   Toda coluna entra como texto (dtype=str), inferência desligada.
   Por quê: inferência lê CEP com zero à esquerda como número e o zero
   desaparece sem aviso. A conversão acontece na etapa 2, onde é uma decisão
   registrada.

6. SAÍDA NA TELA
   Uma linha por tabela, com nome qualificado e contagem de linhas vinda do
   banco (não do DataFrame), milhar separado por ponto. Uma linha final com o
   total. O stdout é reconfigurado para utf-8.
   Por quê: essa saída é o único instrumento de conferência desta etapa, e no
   Prompt de Comando do Windows acento sai embaralhado por padrão.

7. ERRO
   Antes de abrir o banco, verifica se os cinco arquivos existem. Faltando
   qualquer um: lista os que faltam, diz em que pasta procurou e encerra com
   sys.exit(1). Não ingere parcialmente.
   Por quê: ingerir quatro de cinco tabelas em silêncio produz um dashboard
   errado que parece certo.

CRITÉRIOS DE ACEITE
- A execução imprime cinco linhas de tabela, mais uma com o total de 561.861
  registros.
- Rodar duas vezes seguidas imprime exatamente a mesma coisa.
- SELECT * FROM raw.itens_pedido devolve todas as colunas como VARCHAR.
- Remover um arquivo da pasta faz o script parar antes de gravar qualquer
  tabela.
- O script termina sozinho e devolve o terminal: nenhum processo fica de pé.
"""

import sys

import pandas as pd

from pipeline.db import PASTA_RAW, conectar, criar_schema, gravar

SCHEMA = "raw"
ARQUIVOS = {
    "ecommerce_sales_customer_analytics_150k.csv.gz": "pedidos_por_cliente",
    "order_items.csv.gz": "itens_pedido",
    "customer_master.csv.gz": "clientes",
    "dataset_statistics.csv.gz": "estatisticas",
    "product_catalog.csv.gz": "produtos",
}


def formatar(numero: int) -> str:
    return f"{numero:,}".replace(",", ".")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    faltando = [nome for nome in ARQUIVOS if not (PASTA_RAW / nome).is_file()]
    if faltando:
        print(f"Arquivos não encontrados em {PASTA_RAW}:")
        for nome in faltando:
            print(f"  {nome}")
        sys.exit(1)

    total = 0
    con = conectar()
    try:
        criar_schema(con, SCHEMA)
        for arquivo, tabela in ARQUIVOS.items():
            df = pd.read_csv(
                PASTA_RAW / arquivo,
                dtype=str,
                encoding="utf-8",
                # O padrão do pandas também anula textos como "NA" e "None",
                # o que seria corrigir valor: só o campo vazio vira nulo.
                keep_default_na=False,
                na_values=[""],
            )
            linhas = gravar(con, df, SCHEMA, tabela)
            total += linhas
            print(f"{SCHEMA}.{tabela}: {formatar(linhas)}")
    finally:
        con.close()

    print(f"Total: {formatar(total)}")


if __name__ == "__main__":
    main()
