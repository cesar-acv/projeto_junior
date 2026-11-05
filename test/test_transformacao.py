import io
import sys
from decimal import Decimal

import duckdb
import pytest

from pipeline import db, transformacao

# Pedido concluído, com todos os campos preenchidos. Os outros pedidos partem
# dele e mudam só o que importa para cada caso.
PEDIDO = {
    "order_id": "P1",
    "order_date": "2024-03-10",
    "order_time": "14:30:00",
    "order_status": "Completed",
    "sales_channel": "Online",
    "customer_id": "C1",
    "customer_name": "Ana",
    "customer_age": "30",
    "gender": "Female",
    "customer_segment": "Consumer",
    "customer_type": "New",
    "customer_city": "Recife",
    "customer_state": "PE",
    "customer_country": "Brazil",
    "region": "North",
    "customer_postal_code": "00516",
    "payment_method": "Credit Card",
    "payment_status": "Paid",
    "currency": "BRL",
    "shipping_method": "Standard",
    "warehouse": "W1",
    "delivery_days": "3.0",
    "estimated_delivery_days": "4.0",
    "delivery_status": "Early",
    "return_status": None,
    "return_reason": None,
    "customer_rating": "4.5",
    "review_sentiment": "Positive",
    "customer_review": "Bom",
    "marketing_channel": "Direct",
    "campaign_name": "Campanha_A",
    "coupon_code": "SAVE10",
    "loyalty_points_earned": "10",
    "loyalty_points_redeemed": "0",
    "quantity": "2",
    "gross_sales": "30.00",
    "discount_amount": "0.0",
    "tax_amount": "0.0",
    "shipping_cost": "0.0",
    "net_sales": "30.00",
    "product_cost": "12.0",
    "profit": "18.0",
    "profit_margin_percentage": "60.0",
    "customer_lifetime_value": "30.0",
    "is_repeat_customer": "False",
    "customer_order_count": "1",
}

ITEM = {
    "order_id": "P1",
    "product_id": "X1",
    "quantity": "1",
    "unit_price": "10.00",
    "discount_percentage": "0.0",
    "discount_amount": "0.0",
    "gross_sales": "10.00",
    "tax_amount": "0.0",
    "shipping_cost": "0.0",
    "net_sales": "10.00",
    "product_cost": "4.0",
    "profit": "6.0",
}

CLIENTE = {
    "customer_id": "C1",
    "customer_name": "Ana",
    "customer_age": "30",
    "gender": "Female",
    "customer_segment": "Consumer",
    "customer_city": "Recife",
    "customer_state": "PE",
    "customer_country": "Brazil",
    "region": "North",
    "customer_postal_code": "00516",
    "customer_acquisition_cost": "12.50",
}

PRODUTO = {
    "product_id": "X1",
    "product_name": "Caneca",
    "product_category": "Casa",
    "product_subcategory": "Cozinha",
    "brand": "Marca",
    "supplier": "Fornecedor",
    "unit_price": "10.00",
    "product_cost": "4.0",
    "product_rating": "4.2",
}


def dados():
    pedidos = [
        # P1: pedido concluído com dois itens que somam o valor do pedido.
        PEDIDO,
        # Linha duplicada: cópia exata de P1, tem que sair.
        dict(PEDIDO),
        # P2: pedido cancelado, sem entrega, sem avaliação, sem campanha.
        PEDIDO
        | {
            "order_id": "P2",
            "order_status": "Cancelled",
            "delivery_days": None,
            "estimated_delivery_days": None,
            "delivery_status": "Cancelled",
            "customer_rating": None,
            "review_sentiment": None,
            "customer_review": None,
            "campaign_name": None,
            "coupon_code": None,
            "net_sales": "5.00",
        },
        # P3: pedido divergente: o pedido diz 12,00 e os itens somam 15,00.
        PEDIDO | {"order_id": "P3", "net_sales": "12.00"},
    ]
    itens = [
        # P1, item 1.
        ITEM,
        # P1, item 2: o valor traz ruído de float e tem que virar 20,00.
        ITEM | {"product_id": "X2", "net_sales": "20.000000000000004"},
        # P2: um item com o mesmo valor do pedido.
        ITEM | {"order_id": "P2", "net_sales": "5.00"},
        # P3: o mesmo produto duas vezes com conteúdo diferente (chave repetida).
        ITEM | {"order_id": "P3", "net_sales": "7.00"},
        ITEM | {"order_id": "P3", "quantity": "2", "net_sales": "8.00"},
    ]
    clientes = [
        # Código postal com zero à esquerda.
        CLIENTE,
        # Outro cliente, sem nada de especial.
        CLIENTE | {"customer_id": "C2", "customer_postal_code": "12345"},
    ]
    produtos = [PRODUTO, PRODUTO | {"product_id": "X2", "product_name": "Livro"}]
    return {
        "pedidos_por_cliente": pedidos,
        "itens_pedido": itens,
        "clientes": clientes,
        "produtos": produtos,
    }


def preparar(tmp_path, monkeypatch, tabelas):
    monkeypatch.setattr(db, "PASTA_RAW", tmp_path / "raw")
    # Conferências calculadas sobre os dados inventados: 1 pedido divergente
    # (P3), 1 chave repetida em 2 linhas e desvio de 15,00 - 12,00.
    monkeypatch.setattr(transformacao, "PEDIDOS_DIVERGENTES", 1)
    monkeypatch.setattr(transformacao, "CHAVES_REPETIDAS", 1)
    monkeypatch.setattr(transformacao, "LINHAS_CHAVES_REPETIDAS", 2)
    monkeypatch.setattr(transformacao, "DESVIO_TOTAL", Decimal("3.00"))
    con = duckdb.connect(str(tmp_path / "projeto_1.duckdb"))
    con.execute("CREATE SCHEMA raw")
    for tabela, linhas in tabelas.items():
        colunas = list(transformacao.MAPA[tabela])
        definicao = ", ".join(f'"{coluna}" VARCHAR' for coluna in colunas)
        con.execute(f'CREATE TABLE raw."{tabela}" ({definicao})')
        marcadores = ", ".join("?" for _ in colunas)
        con.executemany(
            f'INSERT INTO raw."{tabela}" VALUES ({marcadores})',
            [[linha[coluna] for coluna in colunas] for linha in linhas],
        )
    con.close()


def capturar(monkeypatch):
    fluxo = io.TextIOWrapper(io.BytesIO(), encoding="utf-8", newline="\n")
    monkeypatch.setattr(sys, "stdout", fluxo)
    return fluxo


def lido(fluxo):
    fluxo.flush()
    return fluxo.buffer.getvalue().decode("utf-8")


def consultar(tmp_path, sql):
    con = duckdb.connect(str(tmp_path / "projeto_1.duckdb"), read_only=True)
    linhas = con.execute(sql).fetchall()
    con.close()
    return linhas


def tem_schema_refined(tmp_path):
    return consultar(
        tmp_path,
        "SELECT count(*) FROM duckdb_schemas() WHERE schema_name = 'refined'",
    ) == [(1,)]


def test_duplicata_exata_sai_e_e_contada(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch, dados())
    fluxo = capturar(monkeypatch)
    transformacao.main()
    saida = lido(fluxo)
    assert consultar(
        tmp_path, "SELECT pedido_id FROM refined.pedidos_por_cliente ORDER BY 1"
    ) == [("P1",), ("P2",), ("P3",)]
    assert "refined.pedidos_por_cliente         4 -> 3 linhas  (1 duplicatas)" in saida
    assert "Total: 13 -> 12 linhas  (1 duplicatas)" in saida


def test_data_vira_date_hora_vira_time_dinheiro_vira_decimal(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch, dados())
    capturar(monkeypatch)
    transformacao.main()
    tipos = {
        linha[0]: linha[1]
        for linha in consultar(tmp_path, "DESCRIBE refined.pedidos_por_cliente")
    }
    assert tipos["data_pedido"] == "DATE"
    assert tipos["hora_pedido"] == "TIME"
    assert tipos["valor_total"] == "DECIMAL(10,2)"
    assert tipos["cliente_recorrente"] == "BOOLEAN"
    assert tipos["nota_cliente"] == "DECIMAL(2,1)"
    assert tipos["dias_entrega"] == "INTEGER"
    assert tipos["codigo_postal"] == "VARCHAR"


def test_colunas_saem_com_os_nomes_do_mapa_e_item_seq_no_fim(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch, dados())
    capturar(monkeypatch)
    transformacao.main()
    colunas = [
        linha[0] for linha in consultar(tmp_path, "DESCRIBE refined.itens_pedido")
    ]
    assert colunas == [*transformacao.MAPA["itens_pedido"].values(), "item_seq"]


def test_valores_convertidos_mantem_o_que_dizem(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch, dados())
    capturar(monkeypatch)
    transformacao.main()
    linha = consultar(
        tmp_path,
        "SELECT CAST(data_pedido AS VARCHAR), CAST(hora_pedido AS VARCHAR), "
        "dias_entrega, nota_cliente, cliente_recorrente, codigo_postal "
        "FROM refined.pedidos_por_cliente WHERE pedido_id = 'P1'",
    )
    assert linha == [("2024-03-10", "14:30:00", 3, Decimal("4.5"), False, "00516")]


def test_valor_que_nao_converte_encerra_sem_virar_nulo(tmp_path, monkeypatch):
    tabelas = dados()
    # Data em formato diferente do declarado: tem que parar, não virar nulo.
    tabelas["pedidos_por_cliente"][0] = PEDIDO | {"order_date": "31/12/2024"}
    preparar(tmp_path, monkeypatch, tabelas)
    fluxo = capturar(monkeypatch)
    with pytest.raises(SystemExit) as parada:
        transformacao.main()
    saida = lido(fluxo)
    assert parada.value.code == 1
    assert "raw.pedidos_por_cliente, coluna order_date" in saida
    assert "'31/12/2024'" in saida
    assert not tem_schema_refined(tmp_path)


def test_dinheiro_que_nao_e_numero_encerra(tmp_path, monkeypatch):
    tabelas = dados()
    # Texto onde deveria haver valor: CAST para DECIMAL tem que estourar.
    tabelas["itens_pedido"][0] = ITEM | {"net_sales": "dez reais"}
    preparar(tmp_path, monkeypatch, tabelas)
    fluxo = capturar(monkeypatch)
    with pytest.raises(SystemExit):
        transformacao.main()
    assert "'dez reais'" in lido(fluxo)
    assert not tem_schema_refined(tmp_path)


def test_nulo_continua_nulo_e_nao_vira_zero(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch, dados())
    capturar(monkeypatch)
    transformacao.main()
    assert consultar(
        tmp_path,
        "SELECT dias_entrega, dias_entrega_previstos, nota_cliente, cupom "
        "FROM refined.pedidos_por_cliente WHERE pedido_id = 'P2'",
    ) == [(None, None, None, None)]


def test_saida_lista_os_nulos_mantidos(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch, dados())
    fluxo = capturar(monkeypatch)
    transformacao.main()
    # Só P2 tem os campos de entrega e avaliação nulos; devolução é nula nos três.
    assert lido(fluxo).splitlines()[1:5] == [
        "    nulos mantidos: dias_entrega 1, dias_entrega_previstos 1,",
        "    status_devolucao 3, motivo_devolucao 3, nota_cliente 1,",
        "    sentimento_avaliacao 1, comentario_avaliacao 1, campanha 1, cupom 1",
        "refined.itens_pedido                5 -> 5 linhas  (0 duplicatas)",
    ]


def test_soma_de_dinheiro_bate_centavo_por_centavo(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch, dados())
    capturar(monkeypatch)
    transformacao.main()
    # 10,00 + 20,00 + 5,00 + 7,00 + 8,00 nos itens; 30,00 + 5,00 + 12,00 nos pedidos.
    assert consultar(
        tmp_path,
        "SELECT (SELECT sum(valor_total) FROM refined.itens_pedido), "
        "(SELECT sum(valor_total) FROM refined.pedidos_por_cliente)",
    ) == [(Decimal("50.00"), Decimal("47.00"))]


def test_chave_repetida_com_conteudo_diferente_encerra_sem_escolher(
    tmp_path, monkeypatch
):
    tabelas = dados()
    # Mesmo cliente_id com outro nome: não dá para saber qual está certo.
    tabelas["clientes"].append(CLIENTE | {"customer_name": "Ana Maria"})
    preparar(tmp_path, monkeypatch, tabelas)
    fluxo = capturar(monkeypatch)
    with pytest.raises(SystemExit) as parada:
        transformacao.main()
    saida = lido(fluxo)
    assert parada.value.code == 1
    assert "customer_id = 'C1'" in saida
    assert "Ana Maria" in saida
    assert not tem_schema_refined(tmp_path)


def test_item_seq_numera_os_itens_de_cada_pedido(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch, dados())
    capturar(monkeypatch)
    transformacao.main()
    assert consultar(
        tmp_path,
        "SELECT pedido_id, item_seq, valor_total FROM refined.itens_pedido "
        "ORDER BY pedido_id, item_seq",
    ) == [
        ("P1", 1, Decimal("10.00")),
        ("P1", 2, Decimal("20.00")),
        ("P2", 1, Decimal("5.00")),
        ("P3", 1, Decimal("7.00")),
        ("P3", 2, Decimal("8.00")),
    ]


def test_itens_divergentes_tem_so_o_pedido_divergente_com_o_desvio(
    tmp_path, monkeypatch
):
    preparar(tmp_path, monkeypatch, dados())
    fluxo = capturar(monkeypatch)
    transformacao.main()
    assert consultar(
        tmp_path,
        "SELECT pedido_id, item_seq, total_itens, total_pedido, desvio, "
        "chave_repetida FROM refined.itens_divergentes ORDER BY item_seq",
    ) == [
        ("P3", 1, Decimal("15.00"), Decimal("12.00"), Decimal("3.00"), True),
        ("P3", 2, Decimal("15.00"), Decimal("12.00"), Decimal("3.00"), True),
    ]
    assert lido(fluxo).splitlines()[-3:] == [
        "refined.itens_divergentes: 2 linhas, 1 pedidos",
        "  desvio: 3,00 (itens 50,00 | pedidos 47,00)",
        "  chaves repetidas: 1",
    ]


def test_conferencia_que_nao_bate_encerra_mostrando_encontrado_e_esperado(
    tmp_path, monkeypatch
):
    preparar(tmp_path, monkeypatch, dados())
    # Esperado diferente do que os dados dão: 1 pedido divergente, não 2.
    monkeypatch.setattr(transformacao, "PEDIDOS_DIVERGENTES", 2)
    fluxo = capturar(monkeypatch)
    with pytest.raises(SystemExit) as parada:
        transformacao.main()
    assert parada.value.code == 1
    assert "pedidos divergentes: encontrado 1, esperado 2" in lido(fluxo)


def test_tabela_de_entrada_faltando_encerra_e_manda_rodar_a_ingestao(
    tmp_path, monkeypatch
):
    tabelas = dados()
    del tabelas["produtos"]
    preparar(tmp_path, monkeypatch, tabelas)
    fluxo = capturar(monkeypatch)
    with pytest.raises(SystemExit) as parada:
        transformacao.main()
    saida = lido(fluxo)
    assert parada.value.code == 1
    assert "  produtos" in saida
    assert "uv run python -m pipeline.ingestao" in saida


def test_rodar_duas_vezes_imprime_o_mesmo(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch, dados())
    primeira = capturar(monkeypatch)
    transformacao.main()
    segunda = capturar(monkeypatch)
    transformacao.main()
    assert lido(primeira) == lido(segunda)
    assert consultar(tmp_path, "SELECT count(*) FROM refined.itens_pedido") == [(5,)]


def test_nunca_escreve_em_raw(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch, dados())
    antes = consultar(tmp_path, "SELECT * FROM raw.pedidos_por_cliente ORDER BY ALL")
    capturar(monkeypatch)
    transformacao.main()
    depois = consultar(tmp_path, "SELECT * FROM raw.pedidos_por_cliente ORDER BY ALL")
    assert antes == depois


def test_formatar_dinheiro_usa_ponto_no_milhar_e_virgula_nos_centavos():
    assert transformacao.formatar_dinheiro(Decimal("1234567.5")) == "1.234.567,50"
