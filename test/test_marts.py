import io
import sys
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from pipeline import db, marts

SQL_MARTS = Path(__file__).resolve().parent.parent / "sql" / "marts"


def preparar(tmp_path, monkeypatch, arquivos, sem_tabela=None):
    monkeypatch.setattr(db, "PASTA_RAW", tmp_path / "raw")
    pasta = tmp_path / "sql" / "marts"
    pasta.mkdir(parents=True)
    monkeypatch.setattr(marts, "PASTA_MARTS", pasta)
    for nome, conteudo in arquivos.items():
        (pasta / nome).write_text(conteudo, encoding="utf-8")
    con = duckdb.connect(str(tmp_path / "projeto_1.duckdb"))
    con.execute("CREATE SCHEMA refined")
    # O script só confere se as cinco existem; o conteúdo aqui é o mínimo.
    for tabela in marts.TABELAS_ORIGEM:
        if tabela != sem_tabela:
            con.execute(f"CREATE TABLE refined.{tabela} (pedido_id VARCHAR)")
    # Dois pedidos, para os marts de teste terem o que contar.
    con.execute("INSERT INTO refined.pedidos_por_cliente VALUES ('P1'), ('P2')")
    con.close()
    return pasta


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


def estado_marts(tmp_path):
    con = duckdb.connect(str(tmp_path / "projeto_1.duckdb"), read_only=True)
    tabelas = [
        linha[0]
        for linha in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'marts' "
            "ORDER BY 1"
        ).fetchall()
    ]
    estado = {
        tabela: con.execute(f"SELECT * FROM marts.{tabela} ORDER BY ALL").fetchall()
        for tabela in tabelas
    }
    con.close()
    return estado


def test_arquivo_00_teste_cria_mart_com_uma_linha(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch, {"00_teste.sql": "SELECT 1 AS x"})
    fluxo = capturar(monkeypatch)
    marts.main()
    assert estado_marts(tmp_path) == {"teste": [(1,)]}
    # Nome completado até 21 posições, um espaço e a contagem em 7 posições.
    assert lido(fluxo).splitlines() == [
        "✓ " + "marts.teste".ljust(21) + " " + "1".rjust(7) + " linhas",
        "Total: 1 marts, 1 linhas",
    ]


def test_arquivo_apagado_faz_o_mart_sumir_na_execucao_seguinte(tmp_path, monkeypatch):
    pasta = preparar(
        tmp_path,
        monkeypatch,
        {
            "00_teste.sql": "SELECT 1 AS x",
            "01_pedidos.sql": "SELECT pedido_id FROM refined.pedidos_por_cliente",
        },
    )
    capturar(monkeypatch)
    marts.main()
    (pasta / "00_teste.sql").unlink()
    marts.main()
    assert list(estado_marts(tmp_path)) == ["pedidos"]


def test_ponto_e_virgula_e_espacos_no_fim_sao_tirados(tmp_path, monkeypatch):
    # Comentário no início e ";" seguido de espaços e linhas em branco no fim.
    preparar(
        tmp_path,
        monkeypatch,
        {
            "01_contagem.sql": "-- quantos pedidos\nSELECT count(*) AS n\n"
            "FROM refined.pedidos_por_cliente;  \n\n"
        },
    )
    capturar(monkeypatch)
    marts.main()
    assert estado_marts(tmp_path) == {"contagem": [(2,)]}


def test_arquivo_sem_prefixo_para_antes_de_executar_qualquer_sql(tmp_path, monkeypatch):
    # Um arquivo válido e um sem prefixo: nem o válido pode ser executado.
    preparar(
        tmp_path,
        monkeypatch,
        {"01_valido.sql": "SELECT 1 AS x", "teste.sql": "SELECT 1 AS x"},
    )
    fluxo = capturar(monkeypatch)
    with pytest.raises(SystemExit) as parada:
        marts.main()
    assert parada.value.code == 1
    assert "  teste.sql" in lido(fluxo)
    assert consultar(
        tmp_path, "SELECT count(*) FROM duckdb_schemas() WHERE schema_name = 'marts'"
    ) == [(0,)]


def test_dois_arquivos_com_o_mesmo_nome_de_tabela_param_antes_de_executar(
    tmp_path, monkeypatch
):
    preparar(
        tmp_path,
        monkeypatch,
        {"01_vendas.sql": "SELECT 1 AS x", "02_VENDAS.sql": "SELECT 2 AS x"},
    )
    fluxo = capturar(monkeypatch)
    with pytest.raises(SystemExit):
        marts.main()
    assert "01_vendas.sql, 02_VENDAS.sql" in lido(fluxo)


def test_erro_de_sintaxe_para_com_o_nome_do_arquivo_e_marts_fica_como_estava(
    tmp_path, monkeypatch
):
    pasta = preparar(tmp_path, monkeypatch, {"00_teste.sql": "SELECT 1 AS x"})
    capturar(monkeypatch)
    marts.main()
    antes = estado_marts(tmp_path)

    (pasta / "01_quebrado.sql").write_text("SELEC 1 FROM", encoding="utf-8")
    fluxo = capturar(monkeypatch)
    with pytest.raises(SystemExit) as parada:
        marts.main()
    assert parada.value.code == 1
    assert "Falha em 01_quebrado.sql" in lido(fluxo)
    assert estado_marts(tmp_path) == antes


def test_mart_vazio_encerra_e_deixa_marts_como_estava(tmp_path, monkeypatch):
    pasta = preparar(tmp_path, monkeypatch, {"00_teste.sql": "SELECT 1 AS x"})
    capturar(monkeypatch)
    marts.main()
    antes = estado_marts(tmp_path)

    # JOIN que não casa: nenhum pedido se chama 'P9'.
    (pasta / "01_vazio.sql").write_text(
        "SELECT pedido_id FROM refined.pedidos_por_cliente WHERE pedido_id = 'P9'",
        encoding="utf-8",
    )
    fluxo = capturar(monkeypatch)
    with pytest.raises(SystemExit) as parada:
        marts.main()
    assert parada.value.code == 1
    assert "Mart vazio: marts.vazio (01_vazio.sql)" in lido(fluxo)
    assert estado_marts(tmp_path) == antes


def test_pasta_sem_arquivo_sql_encerra(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch, {})
    fluxo = capturar(monkeypatch)
    with pytest.raises(SystemExit) as parada:
        marts.main()
    assert parada.value.code == 1
    assert "Nenhum arquivo .sql" in lido(fluxo)


def test_tabela_faltando_em_refined_encerra_e_manda_rodar_a_transformacao(
    tmp_path, monkeypatch
):
    preparar(
        tmp_path, monkeypatch, {"00_teste.sql": "SELECT 1 AS x"}, sem_tabela="produtos"
    )
    fluxo = capturar(monkeypatch)
    with pytest.raises(SystemExit) as parada:
        marts.main()
    saida = lido(fluxo)
    assert parada.value.code == 1
    assert "  produtos" in saida
    assert "uv run python -m pipeline.transformacao" in saida


def test_rodar_duas_vezes_imprime_o_mesmo(tmp_path, monkeypatch):
    preparar(
        tmp_path,
        monkeypatch,
        {
            "00_teste.sql": "SELECT 1 AS x",
            "01_pedidos.sql": "SELECT pedido_id FROM refined.pedidos_por_cliente",
        },
    )
    primeira = capturar(monkeypatch)
    marts.main()
    segunda = capturar(monkeypatch)
    marts.main()
    assert lido(primeira) == lido(segunda)
    assert lido(primeira).splitlines()[-1] == "Total: 2 marts, 3 linhas"


# Regras de negócio dos arquivos de sql/marts/, rodadas sobre tabelas inventadas.


def banco_de_pedidos(tmp_path, pedidos, itens=()):
    con = duckdb.connect(str(tmp_path / "marts.duckdb"))
    con.execute("CREATE SCHEMA refined")
    con.execute(
        "CREATE TABLE refined.pedidos_por_cliente (pedido_id VARCHAR, "
        "data_pedido DATE, status_pedido VARCHAR, status_pagamento VARCHAR, "
        "forma_pagamento VARCHAR, status_entrega VARCHAR, "
        "valor_total DECIMAL(10,2))"
    )
    con.execute(
        "CREATE TABLE refined.itens_pedido (pedido_id VARCHAR, produto_id VARCHAR, "
        "valor_total DECIMAL(10,2))"
    )
    con.executemany(
        "INSERT INTO refined.pedidos_por_cliente VALUES (?, ?, ?, ?, ?, ?, ?)",
        pedidos,
    )
    if itens:
        con.executemany("INSERT INTO refined.itens_pedido VALUES (?, ?, ?)", itens)
    return con


def rodar_mart(con, arquivo):
    sql = (SQL_MARTS / arquivo).read_text(encoding="utf-8")
    linhas = con.execute(sql).fetchall()
    con.close()
    return linhas


def test_pedido_com_dois_itens_conta_uma_vez_nas_vendas_anuais(tmp_path):
    con = banco_de_pedidos(
        tmp_path,
        # Um pedido concluído de 100,00.
        [("P1", "2024-05-10", "Completed", "Paid", "Pix", "On Time", "100.00")],
        # Os dois itens dele, que somam o valor do pedido.
        [("P1", "X1", "40.00"), ("P1", "X2", "60.00")],
    )
    assert rodar_mart(con, "01_vendas_anuais.sql") == [(2024, 1, Decimal("100.00"))]


def test_cancelado_fica_fora_das_vendas_anuais(tmp_path):
    con = banco_de_pedidos(
        tmp_path,
        [
            # Concluído: entra.
            ("P1", "2024-05-10", "Completed", "Paid", "Pix", "On Time", "100.00"),
            # Cancelado no mesmo ano: não entra nem na contagem nem no valor.
            ("P2", "2024-06-01", "Cancelled", "Refunded", "Pix", "Cancelled", "50.00"),
            # Concluído em outro ano: vira outra linha.
            ("P3", "2025-01-02", "Completed", "Paid", "Pix", "Delayed", "30.00"),
        ],
    )
    assert rodar_mart(con, "01_vendas_anuais.sql") == [
        (2024, 1, Decimal("100.00")),
        (2025, 1, Decimal("30.00")),
    ]


def test_cancelado_e_nao_pago_ficam_fora_da_receita_por_pagamento(tmp_path):
    con = banco_de_pedidos(
        tmp_path,
        [
            # Dois pedidos concluídos e pagos no cartão: 50,00 + 100,00.
            ("P1", "2024-05-10", "Completed", "Paid", "Credit Card", "Early", "50.00"),
            ("P2", "2024-05-11", "Completed", "Paid", "Credit Card", "Early", "100.00"),
            # Concluído e pago no Pix: 50,00.
            ("P3", "2024-05-12", "Completed", "Paid", "Pix", "On Time", "50.00"),
            # Cancelado: fora da receita.
            ("P4", "2024-05-13", "Cancelled", "Paid", "Pix", "Cancelled", "999.00"),
            # Concluído, mas sem pagamento confirmado: fora da receita.
            ("P5", "2024-05-14", "Completed", "Pending", "Pix", "On Time", "999.00"),
        ],
        # P1 com dois itens: o pedido continua contando uma vez.
        [("P1", "X1", "20.00"), ("P1", "X2", "30.00")],
    )
    # Cartão: 150,00 de 200,00 = 75%; ticket (50 + 100) / 2 = 75,00.
    assert rodar_mart(con, "03_receita_por_tipo_de_pagamento.sql") == [
        ("Credit Card", 2, Decimal("150.00"), 75.0, 75.0),
        ("Pix", 1, Decimal("50.00"), 25.0, 50.0),
    ]


def test_entrega_no_prazo_ou_antecipada_conta_no_prazo_e_atrasada_nao(tmp_path):
    con = banco_de_pedidos(
        tmp_path,
        [
            # Entregue no dia previsto: no prazo.
            ("P1", "2024-05-10", "Completed", "Paid", "Pix", "On Time", "10.00"),
            # Entregue antes do previsto: também no prazo.
            ("P2", "2024-05-11", "Completed", "Paid", "Pix", "Early", "10.00"),
            # Entregue depois do previsto: fora do prazo.
            ("P3", "2024-05-12", "Completed", "Paid", "Pix", "Delayed", "10.00"),
            ("P4", "2024-05-13", "Completed", "Paid", "Pix", "Delayed", "10.00"),
            # Cancelado: não foi entregue, então não entra na conta.
            ("P5", "2024-05-14", "Cancelled", "Paid", "Pix", "Cancelled", "10.00"),
        ],
    )
    # 2 no prazo de 4 entregas = 50%.
    assert rodar_mart(con, "04_entregas_no_prazo_por_ano.sql") == [(2024, 4, 50.0)]
