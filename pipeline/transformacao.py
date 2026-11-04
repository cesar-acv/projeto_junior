"""Etapa 2: converte tipos e renomeia colunas de raw para refined.

Executar com: uv run python -m pipeline.transformacao

SPEC

1. ESCOPO
   Lê de raw, escreve no schema refined. Nunca escreve em raw.
   Entram quatro tabelas: clientes, itens_pedido, pedidos_por_cliente, produtos.
   Fica de fora: estatisticas.
   Por quê: raw guarda o dado que chegou e refined, o que foi decidido sobre
   ele; misturar os dois apagaria essa fronteira. estatisticas é uma linha de
   totais publicados, não um dado a transformar: o script nem a lê.

2. NOMES
   Colunas renomeadas para português, snake_case, com o mapa de MAPA, completo e
   literal. Nenhuma coluna fora do mapa; nenhuma descartada.
   Única exceção: item_seq, criada em refined.itens_pedido. Numera os itens de
   cada pedido a partir de 1, na ordem das linhas em raw.
   Por quê: é o vocabulário digitado de memória nos JOINs da aula 3. item_seq
   existe porque (pedido_id, produto_id) ainda não é única (seção 5).

3. TIPOS
   Datas: DATE, formato declarado '%Y-%m-%d'.
   Horas: TIME, formato declarado '%H:%M:%S'.
     Por quê: data e hora chegam em colunas separadas nas 138.116 linhas;
     manter separado permite agrupar por dia ou por faixa de horário sem
     extrair nada.
   Dinheiro: DECIMAL(10,2).
     Por quê: DOUBLE não representa centavos com exatidão. A soma de valor_total
     dá 177.134.263,73999885 em DOUBLE no DuckDB e 177.134.263,74 em DECIMAL,
     que é o total publicado em estatisticas. O maior valor individual é
     30.268,44.
   Contagens e medidas: INTEGER.
     Por quê: dias_entrega e dias_entrega_previstos chegam como "3.0", mas são
     sempre números inteiros, então a conversão não perde informação.
   Notas: DECIMAL(2,1).
     Por quê: as notas têm uma casa decimal e 102.016 das 113.559 notas de
     clientes não são inteiras; como INTEGER, perderiam essa casa.
   Margem: DECIMAL(5,2).
     Por quê: tem duas casas decimais e vai de -59,55 a 77,59.
   Taxa: DOUBLE.
     Por quê: é uma proporção de 0 a 0,6 com até 20 casas decimais, não
     dinheiro; arredondar agora perderia precisão no cálculo de outros valores.
   Sim/não: BOOLEAN.
     Por quê: só contém True ou False.
   Todo o resto continua VARCHAR, inclusive codigo_postal.
     Por quê: 2.376 dos 25.000 clientes têm código postal começando com zero; é
     identificador, nunca é somado, e como número perderia o zero.
   Nulos continuam nulos.
     Por quê: dias_entrega, dias_entrega_previstos e nota_cliente ficam nulos
     nos 24.557 pedidos que não foram concluídos; não viram zero.
   A conversão é feita em SQL pelo DuckDB (strptime, CAST), disparada pelo Python.
     Por quê: o dado já está no banco; não há por que trazê-lo para a memória só
     para converter tipo.

4. CONVERSÃO QUE FALHA
   Para tudo. Usa strptime e CAST, nunca try_strptime, TRY_CAST ou
   errors="coerce". Ao falhar: diz tabela, coluna e um exemplo do valor que não
   converteu, e encerra com sys.exit(1).
   Por quê: um nulo fabricado pela conversão fica indistinguível de um nulo
   verdadeiro do dado, para sempre.
   Limite conhecido: CAST para quando o texto não é número ou estoura o tipo,
   mas arredonda casas extras sem erro ('3.5' vira 4 em INTEGER). Com o dado de
   hoje isso não perde nada: os dias são sempre ".0", e o dinheiro com mais de 2
   casas é só ruído de float. Um dado novo seria arredondado em silêncio.

5. DUPLICATAS
   Chaves: pedidos_por_cliente (pedido_id), clientes (cliente_id), produtos
   (produto_id). itens_pedido: chave esperada (pedido_id, produto_id), ainda não
   única; ver a exceção abaixo.
   Linha inteira repetida: mantém uma e conta quantas saíram. Hoje: zero.
   Chave repetida com conteúdo diferente: para, mostra a chave e as linhas
   conflitantes, sys.exit(1). Não escolhe uma por conta própria.
   Por quê: escolher em silêncio é decidir sem dizer que decidiu.

   Exceção registrada: itens_pedido
     Pedido divergente é aquele em que a soma de valor_total dos itens difere de
     valor_total em pedidos_por_cliente. Hoje são 11.226 pedidos, e em todos os
     itens somam mais do que o pedido. Em 86 deles, 87 chaves
     (pedido_id, produto_id) se repetem com conteúdo diferente (174 linhas).
     Nenhuma dessas linhas é removida ou escolhida: refined.itens_pedido mantém
     as 397.569 linhas, e o pipeline não para nesses casos.
     Causa provável: pedido_id reaproveitado na geração do dataset, sem
     confirmação do autor.
     Por quê: os dados não permitem afirmar com certeza quais itens estão
     errados; a divergência fica registrada e visível em vez de ser corrigida
     por suposição.

   Tabela refined.itens_divergentes
     Todas as linhas de refined.itens_pedido dos pedidos divergentes (hoje 61.256
     linhas de 11.226 pedidos), com as mesmas colunas, nomes e tipos, mais:
       total_itens DECIMAL(12,2): soma de valor_total dos itens do pedido.
       total_pedido DECIMAL(10,2): valor_total em pedidos_por_cliente.
       desvio DECIMAL(12,2): total_itens - total_pedido.
       chave_repetida BOOLEAN: a chave (pedido_id, produto_id) da linha aparece
         mais de uma vez.
     total_itens, total_pedido e desvio se repetem em cada linha do pedido; para
     somar o desvio, considerar uma linha por pedido.
     Por quê: permite conferir a divergência sem alterar a tabela principal.

   Conferências; se qualquer uma falhar: para, mostra os valores encontrados e
   os esperados, sys.exit(1).
     - 11.226 pedidos divergentes, 87 chaves repetidas e 174 linhas nessas chaves.
     - O desvio somado por pedido é igual à diferença entre as somas de
       valor_total de itens_pedido e pedidos_por_cliente: 15.264.613,55.
     - Nenhum pedido tem desvio negativo.
     Por quê: a exceção vale para esta divergência, com este tamanho. Se o dado
     mudar, alguém precisa olhar de novo em vez de herdar a exceção sem saber.

6. NULOS
   Nenhuma linha é apagada por ter nulo. Nenhum nulo é preenchido.
   Por quê: o nulo é informação; pedido não concluído não tem dias de entrega
   nem nota. refined não corrige fato: quem decide o que fazer com ele é a
   pergunta que o mart responde.

7. SAÍDA NA TELA
   Uma linha por tabela, abaixo dela as colunas com nulo e a contagem, depois o
   total e, fora da soma, o bloco da divergência. Tabela sem coluna com nulo não
   imprime a linha "nulos mantidos". As contagens vêm do banco, milhar separado
   por ponto. stdout reconfigurado para utf-8.
   Por quê: a saída é o instrumento de conferência da etapa. O bloco fica fora
   da soma porque as linhas são cópias de refined.itens_pedido; somá-las contaria
   os mesmos itens duas vezes.

8. REEXECUÇÃO
   CREATE OR REPLACE TABLE, nunca acrescenta. Rodar dez vezes = rodar uma.
   Por quê: é o que permite errar sem medo.

9. ERRO
   Antes de transformar, verifica se as quatro tabelas de entrada existem em raw.
   Faltando qualquer uma: lista quais, diz para rodar
   "uv run python -m pipeline.ingestao" e encerra com sys.exit(1).
   Por quê: transformar parte das tabelas produz um refined incompleto que parece
   certo.

CRITÉRIOS DE ACEITE
- Quatro tabelas principais em refined; em cada uma, linhas que saem = linhas que
  entram (com este dado, 0 duplicatas em todas). Total: 561.860. Além delas,
  refined.itens_divergentes com 61.256 linhas de 11.226 pedidos, fora do total.
- DESCRIBE refined.pedidos_por_cliente mostra DATE em data_pedido, TIME em
  hora_pedido, DECIMAL(10,2) em valor_total e BOOLEAN em cliente_recorrente.
- Nulos depois = nulos antes, coluna por coluna (pedidos_por_cliente: dias_entrega
  24.557, dias_entrega_previstos 24.557, status_devolucao 128.654,
  motivo_devolucao 128.654, nota_cliente 24.557, sentimento_avaliacao 24.557,
  comentario_avaliacao 24.557, campanha 83.233, cupom 110.502; itens_pedido,
  clientes e produtos: nenhum nulo).
- sum(valor_total) em refined.itens_pedido bate com
  sum(CAST(net_sales AS DECIMAL(10,2))) em raw.itens_pedido, centavo por centavo:
  192.398.877,29.
- sum(valor_total) em refined.pedidos_por_cliente bate com
  sum(CAST(net_sales AS DECIMAL(10,2))) em raw.pedidos_por_cliente, centavo por
  centavo: 177.134.263,74.
- codigo_postal continua com o zero à esquerda: '00516' e não 516; 2.376 clientes
  em refined.clientes têm código começando com zero.
- Rodar duas vezes seguidas imprime exatamente a mesma coisa.
"""

import re
import sys
from decimal import Decimal

import duckdb

from pipeline.db import conectar, criar_schema
from pipeline.ingestao import formatar

ORIGEM = "raw"
DESTINO = "refined"

MAPA = {
    "pedidos_por_cliente": {
        "order_id": "pedido_id",
        "order_date": "data_pedido",
        "order_time": "hora_pedido",
        "order_status": "status_pedido",
        "sales_channel": "canal_venda",
        "customer_id": "cliente_id",
        "customer_name": "nome_cliente",
        "customer_age": "idade",
        "gender": "genero",
        "customer_segment": "segmento",
        "customer_type": "tipo_cliente",
        "customer_city": "cidade",
        "customer_state": "estado",
        "customer_country": "pais",
        "region": "regiao",
        "customer_postal_code": "codigo_postal",
        "payment_method": "forma_pagamento",
        "payment_status": "status_pagamento",
        "currency": "moeda",
        "shipping_method": "tipo_envio",
        "warehouse": "armazem",
        "delivery_days": "dias_entrega",
        "estimated_delivery_days": "dias_entrega_previstos",
        "delivery_status": "status_entrega",
        "return_status": "status_devolucao",
        "return_reason": "motivo_devolucao",
        "customer_rating": "nota_cliente",
        "review_sentiment": "sentimento_avaliacao",
        "customer_review": "comentario_avaliacao",
        "marketing_channel": "canal_marketing",
        "campaign_name": "campanha",
        "coupon_code": "cupom",
        "loyalty_points_earned": "pontos_ganhos",
        "loyalty_points_redeemed": "pontos_resgatados",
        "quantity": "quantidade",
        "gross_sales": "venda_bruta",
        "discount_amount": "desconto",
        "tax_amount": "imposto",
        "shipping_cost": "frete",
        "net_sales": "valor_total",
        "product_cost": "custo_produto",
        "profit": "lucro_informado",
        "profit_margin_percentage": "margem_informada_pct",
        "customer_lifetime_value": "valor_vitalicio_informado",
        "is_repeat_customer": "cliente_recorrente",
        "customer_order_count": "qtd_pedidos_informada",
    },
    "itens_pedido": {
        "order_id": "pedido_id",
        "product_id": "produto_id",
        "quantity": "quantidade",
        "unit_price": "preco_unitario",
        "discount_percentage": "taxa_desconto",
        "discount_amount": "desconto",
        "gross_sales": "venda_bruta",
        "tax_amount": "imposto",
        "shipping_cost": "frete",
        "net_sales": "valor_total",
        "product_cost": "custo_produto",
        "profit": "lucro_informado",
    },
    "clientes": {
        "customer_id": "cliente_id",
        "customer_name": "nome_cliente",
        "customer_age": "idade",
        "gender": "genero",
        "customer_segment": "segmento",
        "customer_city": "cidade",
        "customer_state": "estado",
        "customer_country": "pais",
        "region": "regiao",
        "customer_postal_code": "codigo_postal",
        "customer_acquisition_cost": "custo_aquisicao",
    },
    "produtos": {
        "product_id": "produto_id",
        "product_name": "nome_produto",
        "product_category": "categoria",
        "product_subcategory": "subcategoria",
        "brand": "marca",
        "supplier": "fornecedor",
        "unit_price": "preco_unitario",
        "product_cost": "custo_unitario",
        "product_rating": "nota_produto",
    },
}

DINHEIRO = "DECIMAL(10,2)"
NOTA = "DECIMAL(2,1)"

# Coluna ausente aqui continua VARCHAR.
TIPOS = {
    "pedidos_por_cliente": {
        "data_pedido": "DATE",
        "hora_pedido": "TIME",
        "venda_bruta": DINHEIRO,
        "desconto": DINHEIRO,
        "imposto": DINHEIRO,
        "frete": DINHEIRO,
        "valor_total": DINHEIRO,
        "custo_produto": DINHEIRO,
        "lucro_informado": DINHEIRO,
        "valor_vitalicio_informado": DINHEIRO,
        "idade": "INTEGER",
        "dias_entrega": "INTEGER",
        "dias_entrega_previstos": "INTEGER",
        "pontos_ganhos": "INTEGER",
        "pontos_resgatados": "INTEGER",
        "quantidade": "INTEGER",
        "qtd_pedidos_informada": "INTEGER",
        "nota_cliente": NOTA,
        "margem_informada_pct": "DECIMAL(5,2)",
        "cliente_recorrente": "BOOLEAN",
    },
    "itens_pedido": {
        "preco_unitario": DINHEIRO,
        "desconto": DINHEIRO,
        "venda_bruta": DINHEIRO,
        "imposto": DINHEIRO,
        "frete": DINHEIRO,
        "valor_total": DINHEIRO,
        "custo_produto": DINHEIRO,
        "lucro_informado": DINHEIRO,
        "quantidade": "INTEGER",
        "taxa_desconto": "DOUBLE",
    },
    "clientes": {
        "custo_aquisicao": DINHEIRO,
        "idade": "INTEGER",
    },
    "produtos": {
        "preco_unitario": DINHEIRO,
        "custo_unitario": DINHEIRO,
        "nota_produto": NOTA,
    },
}

# itens_pedido fica de fora: é a exceção registrada na seção 5.
CHAVES = {
    "pedidos_por_cliente": "order_id",
    "clientes": "customer_id",
    "produtos": "product_id",
}

PEDIDOS_DIVERGENTES = 11_226
CHAVES_REPETIDAS = 87
LINHAS_CHAVES_REPETIDAS = 174
DESVIO_TOTAL = Decimal("15264613.55")

MAX_CHAVES_MOSTRADAS = 5
LARGURA_NULOS = 77


def consultar(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    return con.execute(sql).fetchall()


def formatar_dinheiro(valor: Decimal) -> str:
    return f"{valor:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")


def expressao(coluna: str, tipo: str) -> str:
    if tipo == "DATE":
        return f"CAST(strptime(\"{coluna}\", '%Y-%m-%d') AS DATE)"
    if tipo == "TIME":
        return f"CAST(strptime(\"{coluna}\", '%H:%M:%S') AS TIME)"
    return f'CAST("{coluna}" AS {tipo})'


def tipo_de(tabela: str, destino: str) -> str:
    return TIPOS[tabela].get(destino, "VARCHAR")


def verificar_entrada(con: duckdb.DuckDBPyConnection) -> None:
    existentes = {
        linha[0]
        for linha in consultar(
            con,
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{ORIGEM}'",
        )
    }
    faltando = [tabela for tabela in MAPA if tabela not in existentes]
    if faltando:
        print(f"Tabelas não encontradas no schema {ORIGEM}:")
        for tabela in faltando:
            print(f"  {tabela}")
        print("Rode antes: uv run python -m pipeline.ingestao")
        sys.exit(1)


def verificar_conversoes(con: duckdb.DuckDBPyConnection) -> None:
    # Coluna por coluna porque o erro do DuckDB traz o valor, mas não a coluna.
    # O TRY_CAST localizaria o valor, porém é justamente o que a seção 4 proíbe.
    for tabela, colunas in MAPA.items():
        for origem, destino in colunas.items():
            tipo = tipo_de(tabela, destino)
            if tipo == "VARCHAR":
                continue
            try:
                consultar(
                    con,
                    f'SELECT count({expressao(origem, tipo)}) FROM {ORIGEM}."{tabela}"',
                )
            except (duckdb.ConversionException, duckdb.InvalidInputException) as erro:
                mensagem = str(erro)
                achado = re.search(r"""string ['"](.*?)['"]""", mensagem)
                exemplo = achado.group(1) if achado else mensagem
                print(f"Conversão falhou em {ORIGEM}.{tabela}, coluna {origem}")
                print(f"  destino: {destino} {tipo}")
                print(f"  valor que não converteu: {exemplo!r}")
                sys.exit(1)


def verificar_chaves(con: duckdb.DuckDBPyConnection) -> None:
    for tabela, chave in CHAVES.items():
        sem_repetidas = f'(SELECT DISTINCT * FROM {ORIGEM}."{tabela}")'
        repetidas = consultar(
            con,
            f'SELECT "{chave}" FROM {sem_repetidas} GROUP BY 1 '
            f"HAVING count(*) > 1 ORDER BY 1",
        )
        if not repetidas:
            continue
        print(
            f"Chave repetida com conteúdo diferente em {ORIGEM}.{tabela}: "
            f"{formatar(len(repetidas))} chave(s) em {chave}"
        )
        for (valor,) in repetidas[:MAX_CHAVES_MOSTRADAS]:
            print(f"  {chave} = {valor!r}")
            resultado = con.execute(
                f'SELECT * FROM {sem_repetidas} WHERE "{chave}" = ?', [valor]
            )
            nomes = [coluna[0] for coluna in resultado.description]
            for linha in resultado.fetchall():
                print(f"    {dict(zip(nomes, linha))}")
        sys.exit(1)


def criar_tabela(con: duckdb.DuckDBPyConnection, tabela: str) -> None:
    colunas = MAPA[tabela]
    selecao = []
    for origem, destino in colunas.items():
        tipo = tipo_de(tabela, destino)
        valor = f'"{origem}"' if tipo == "VARCHAR" else expressao(origem, tipo)
        selecao.append(f'{valor} AS "{destino}"')
    if tabela == "itens_pedido":
        selecao.append(
            'CAST(row_number() OVER (PARTITION BY "order_id" ORDER BY _linha) '
            "AS INTEGER) AS item_seq"
        )
    todas = ", ".join(f'"{origem}"' for origem in colunas)
    # rowid fixa qual linha sobrevive e a ordem de saída: sem isso, item_seq
    # poderia mudar de uma execução para outra.
    con.execute(f"""
        CREATE OR REPLACE TABLE {DESTINO}."{tabela}" AS
        SELECT {", ".join(selecao)}
        FROM (
            SELECT *, rowid AS _linha
            FROM {ORIGEM}."{tabela}"
            QUALIFY row_number() OVER (PARTITION BY {todas} ORDER BY rowid) = 1
        )
        ORDER BY _linha
    """)


def imprimir_nulos(con: duckdb.DuckDBPyConnection, tabela: str) -> None:
    destinos = list(MAPA[tabela].values())
    contagens = ", ".join(f'count(*) - count("{coluna}")' for coluna in destinos)
    nulos = consultar(con, f'SELECT {contagens} FROM {DESTINO}."{tabela}"')[0]
    itens = [
        f"{coluna} {formatar(quantidade)}"
        for coluna, quantidade in zip(destinos, nulos)
        if quantidade
    ]
    if not itens:
        return
    # Quebra entre colunas: textwrap poderia separar o nome da contagem.
    linhas = []
    atual = "    nulos mantidos:"
    for posicao, item in enumerate(itens):
        texto = item if posicao == len(itens) - 1 else f"{item},"
        if len(atual) + 1 + len(texto) > LARGURA_NULOS:
            linhas.append(atual)
            atual = f"    {texto}"
        else:
            atual = f"{atual} {texto}"
    linhas.append(atual)
    print("\n".join(linhas))


def criar_itens_divergentes(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE {DESTINO}.itens_divergentes AS
        WITH totais AS (
            SELECT pedido_id, sum(valor_total) AS total
            FROM {DESTINO}.itens_pedido
            GROUP BY pedido_id
        )
        SELECT
            i.*,
            CAST(t.total AS DECIMAL(12,2)) AS total_itens,
            p.valor_total AS total_pedido,
            CAST(t.total - p.valor_total AS DECIMAL(12,2)) AS desvio,
            count(*) OVER (PARTITION BY i.pedido_id, i.produto_id) > 1
                AS chave_repetida
        FROM {DESTINO}.itens_pedido AS i
        JOIN totais AS t USING (pedido_id)
        JOIN {DESTINO}.pedidos_por_cliente AS p USING (pedido_id)
        WHERE t.total <> p.valor_total
        ORDER BY i.pedido_id, i.item_seq
    """)


def conferir_divergencia(con: duckdb.DuckDBPyConnection) -> None:
    linhas, pedidos = consultar(
        con,
        f"SELECT count(*), count(DISTINCT pedido_id) FROM {DESTINO}.itens_divergentes",
    )[0]
    # Contadas em itens_pedido, não em itens_divergentes: uma chave repetida num
    # pedido que não diverge também precisa derrubar a conferência.
    chaves, linhas_chaves = consultar(
        con,
        f"""
        SELECT count(*), coalesce(sum(n), 0)
        FROM (
            SELECT count(*) AS n
            FROM {DESTINO}.itens_pedido
            GROUP BY pedido_id, produto_id
            HAVING count(*) > 1
        )
        """,
    )[0]
    desvio, negativos = consultar(
        con,
        f"""
        SELECT coalesce(sum(desvio), 0), count(*) FILTER (desvio < 0)
        FROM (SELECT DISTINCT pedido_id, desvio FROM {DESTINO}.itens_divergentes)
        """,
    )[0]
    itens, pedidos_total = consultar(
        con,
        f"""
        SELECT
            (SELECT sum(valor_total) FROM {DESTINO}.itens_pedido),
            (SELECT sum(valor_total) FROM {DESTINO}.pedidos_por_cliente)
        """,
    )[0]

    conferencias = [
        ("pedidos divergentes", pedidos, PEDIDOS_DIVERGENTES),
        ("chaves repetidas", chaves, CHAVES_REPETIDAS),
        ("linhas nas chaves repetidas", linhas_chaves, LINHAS_CHAVES_REPETIDAS),
        ("desvio somado por pedido", desvio, DESVIO_TOTAL),
        ("itens menos pedidos", itens - pedidos_total, DESVIO_TOTAL),
        ("pedidos com desvio negativo", negativos, 0),
    ]
    falhas = [item for item in conferencias if item[1] != item[2]]
    if falhas:
        print("Conferência da divergência falhou:")
        for nome, encontrado, esperado in falhas:
            print(f"  {nome}: encontrado {encontrado}, esperado {esperado}")
        sys.exit(1)

    print(
        f"{DESTINO}.itens_divergentes: {formatar(linhas)} linhas, "
        f"{formatar(pedidos)} pedidos"
    )
    print(
        f"  desvio: {formatar_dinheiro(desvio)} "
        f"(itens {formatar_dinheiro(itens)} | "
        f"pedidos {formatar_dinheiro(pedidos_total)})"
    )
    print(f"  chaves repetidas: {formatar(chaves)}")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    con = conectar()
    try:
        verificar_entrada(con)
        # Tudo o que pode parar por causa do dado vem antes da primeira escrita.
        verificar_conversoes(con)
        verificar_chaves(con)
        criar_schema(con, DESTINO)

        total_entrada = total_saida = 0
        for tabela in MAPA:
            criar_tabela(con, tabela)
            entrada = consultar(con, f'SELECT count(*) FROM {ORIGEM}."{tabela}"')[0][0]
            saida = consultar(con, f'SELECT count(*) FROM {DESTINO}."{tabela}"')[0][0]
            total_entrada += entrada
            total_saida += saida
            print(
                f"{f'{DESTINO}.{tabela}':<30}{formatar(entrada):>7} -> "
                f"{formatar(saida)} linhas  ({formatar(entrada - saida)} duplicatas)"
            )
            imprimir_nulos(con, tabela)
        print(
            f"Total: {formatar(total_entrada)} -> {formatar(total_saida)} linhas  "
            f"({formatar(total_entrada - total_saida)} duplicatas)"
        )

        criar_itens_divergentes(con)
        conferir_divergencia(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()
