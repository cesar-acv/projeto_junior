import gzip
import io
import sys

import duckdb
import pytest

from pipeline import db, ingestao

# Conteúdo inventado, com poucas linhas, para cada um dos cinco arquivos.
CSVS = {
    "ecommerce_sales_customer_analytics_150k.csv.gz": (
        "order_id,customer_postal_code,coupon_code,return_reason\n"
        # Pedido comum: todos os campos preenchidos.
        "P1,12345,SAVE10,Wrong Product\n"
        # Código postal com zero à esquerda e campo vazio no cupom.
        "P2,00516,,Other\n"
        # Texto "NA" e "None": é dado, não pode virar nulo.
        "P3,99999,NA,None\n"
    ),
    "order_items.csv.gz": (
        "order_id,product_id,net_sales\n"
        # Pedido com dois itens: duas linhas.
        "P1,X1,10.00\n"
        "P1,X2,20.00\n"
    ),
    "customer_master.csv.gz": ("customer_id,customer_name\nC1,Ana\n"),
    "dataset_statistics.csv.gz": ("Total Transactions\n3\n"),
    "product_catalog.csv.gz": ("product_id,product_name\nX1,Caneca\nX2,Livro\n"),
}


def preparar(tmp_path, monkeypatch, sem=None):
    pasta = tmp_path / "raw"
    pasta.mkdir()
    for nome, conteudo in CSVS.items():
        if nome == sem:
            continue
        with gzip.open(pasta / nome, "wt", encoding="utf-8", newline="") as arquivo:
            arquivo.write(conteudo)
    monkeypatch.setattr(db, "PASTA_RAW", pasta)
    monkeypatch.setattr(ingestao, "PASTA_RAW", pasta)


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


def test_cada_csv_vira_tabela_com_as_mesmas_linhas(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch)
    capturar(monkeypatch)
    ingestao.main()
    contagens = {
        tabela: consultar(tmp_path, f"SELECT count(*) FROM raw.{tabela}")[0][0]
        for tabela in ingestao.ARQUIVOS.values()
    }
    assert contagens == {
        "pedidos_por_cliente": 3,
        "itens_pedido": 2,
        "clientes": 1,
        "estatisticas": 1,
        "produtos": 2,
    }


def test_toda_coluna_entra_como_varchar_e_mantem_zero_a_esquerda(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch)
    capturar(monkeypatch)
    ingestao.main()
    tipos = consultar(
        tmp_path,
        "SELECT DISTINCT data_type FROM information_schema.columns "
        "WHERE table_schema = 'raw'",
    )
    postal = consultar(
        tmp_path,
        "SELECT customer_postal_code FROM raw.pedidos_por_cliente "
        "WHERE order_id = 'P2'",
    )
    assert tipos == [("VARCHAR",)]
    assert postal == [("00516",)]


def test_campo_vazio_vira_nulo_e_texto_na_continua_texto(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch)
    capturar(monkeypatch)
    ingestao.main()
    linhas = consultar(
        tmp_path,
        "SELECT order_id, coupon_code, return_reason "
        "FROM raw.pedidos_por_cliente ORDER BY order_id",
    )
    assert linhas == [
        ("P1", "SAVE10", "Wrong Product"),
        ("P2", None, "Other"),
        ("P3", "NA", "None"),
    ]


def test_arquivo_faltando_encerra_antes_de_gravar_qualquer_tabela(
    tmp_path, monkeypatch
):
    preparar(tmp_path, monkeypatch, sem="order_items.csv.gz")
    fluxo = capturar(monkeypatch)
    with pytest.raises(SystemExit) as parada:
        ingestao.main()
    assert parada.value.code == 1
    assert "order_items.csv.gz" in lido(fluxo)
    assert not (tmp_path / "projeto_1.duckdb").exists()


def test_saida_mostra_contagem_por_tabela_e_total(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch)
    fluxo = capturar(monkeypatch)
    ingestao.main()
    assert lido(fluxo).splitlines() == [
        "raw.pedidos_por_cliente: 3",
        "raw.itens_pedido: 2",
        "raw.clientes: 1",
        "raw.estatisticas: 1",
        "raw.produtos: 2",
        "Total: 9",
    ]


def test_rodar_duas_vezes_imprime_o_mesmo_e_nao_acrescenta(tmp_path, monkeypatch):
    preparar(tmp_path, monkeypatch)
    primeira = capturar(monkeypatch)
    ingestao.main()
    segunda = capturar(monkeypatch)
    ingestao.main()
    assert lido(primeira) == lido(segunda)
    assert consultar(tmp_path, "SELECT count(*) FROM raw.itens_pedido") == [(2,)]


def test_formatar_separa_milhar_com_ponto():
    assert ingestao.formatar(1234567) == "1.234.567"
    assert ingestao.formatar(999) == "999"
