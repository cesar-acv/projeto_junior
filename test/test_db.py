import importlib

import duckdb
import pandas as pd

from pipeline import db


def test_importar_db_nao_abre_banco_nem_cria_arquivo(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    importlib.reload(db)
    assert list(tmp_path.iterdir()) == []


def test_conectar_cria_o_banco_na_raiz_do_projeto_e_nao_na_pasta_corrente(
    tmp_path, monkeypatch
):
    projeto = tmp_path / "projeto"
    outra_pasta = tmp_path / "outra_pasta"
    projeto.mkdir()
    outra_pasta.mkdir()
    monkeypatch.setattr(db, "PASTA_RAW", projeto / "raw")
    monkeypatch.chdir(outra_pasta)

    db.conectar().close()

    assert (projeto / "projeto_1.duckdb").is_file()
    assert list(outra_pasta.iterdir()) == []


def test_criar_schema_duas_vezes_nao_falha(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "PASTA_RAW", tmp_path / "raw")
    con = db.conectar()
    db.criar_schema(con, "raw")
    db.criar_schema(con, "raw")
    schemas = con.execute(
        "SELECT count(*) FROM duckdb_schemas() WHERE schema_name = 'raw'"
    ).fetchall()
    con.close()
    assert schemas == [(1,)]


def test_gravar_devolve_a_contagem_e_rodar_duas_vezes_nao_acrescenta(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(db, "PASTA_RAW", tmp_path / "raw")
    # Três linhas quaisquer: o que importa é a contagem.
    df = pd.DataFrame({"id": ["1", "2", "3"]}, dtype=str)
    con = db.conectar()
    db.criar_schema(con, "raw")
    primeira = db.gravar(con, df, "raw", "exemplo")
    segunda = db.gravar(con, df, "raw", "exemplo")
    gravadas = con.execute("SELECT count(*) FROM raw.exemplo").fetchall()
    con.close()
    assert (primeira, segunda, gravadas) == (3, 3, [(3,)])


def test_gravar_mantem_texto_como_varchar_e_nulo_como_nulo(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "PASTA_RAW", tmp_path / "raw")
    df = pd.DataFrame(
        {
            # Zero à esquerda: só sobrevive se a coluna continuar texto.
            "codigo_postal": ["00516", "12345"],
            # Ausência do dado: tem que continuar nula, não virar "".
            "cupom": ["SAVE10", None],
        },
        dtype=str,
    )
    con = db.conectar()
    db.criar_schema(con, "raw")
    db.gravar(con, df, "raw", "exemplo")
    tipos = con.execute("DESCRIBE raw.exemplo").fetchall()
    linhas = con.execute(
        "SELECT codigo_postal, cupom FROM raw.exemplo ORDER BY codigo_postal"
    ).fetchall()
    sobra = con.execute(
        "SELECT count(*) FROM duckdb_views() WHERE NOT internal"
    ).fetchall()
    con.close()
    assert [(linha[0], linha[1]) for linha in tipos] == [
        ("codigo_postal", "VARCHAR"),
        ("cupom", "VARCHAR"),
    ]
    assert linhas == [("00516", "SAVE10"), ("12345", None)]
    assert sobra == [(0,)]


def test_conectar_devolve_conexao_que_quem_chama_fecha(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "PASTA_RAW", tmp_path / "raw")
    con = db.conectar()
    assert isinstance(con, duckdb.DuckDBPyConnection)
    con.close()
    # Fechada a primeira, outra conexão de escrita abre sem esbarrar em lock.
    outra = duckdb.connect(str(tmp_path / "projeto_1.duckdb"))
    outra.close()
    assert (tmp_path / "projeto_1.duckdb").is_file()
