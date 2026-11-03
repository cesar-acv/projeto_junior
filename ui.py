"""Abre o projeto_1.duckdb no navegador pela extensão ui do DuckDB.

Executar com: uv run python ui.py

SPEC

U1. O QUE FAZ
    Abre o projeto_1.duckdb da raiz do projeto, instala e carrega a extensão ui
    do DuckDB (INSTALL ui; LOAD ui;) e chama CALL start_ui(), que sobe o servidor
    em http://localhost:4213 e abre o navegador.
    Por quê: explorar o banco pelo navegador em vez de ler tabela no terminal.

U2. FICA DE PÉ ATÉ CTRL+C
    O script mantém o processo rodando, imprime a URL e a instrução "Ctrl+C
    para encerrar", e no Ctrl+C fecha a conexão e sai limpo, sem traceback.
    Por quê: a extensão serve enquanto a conexão estiver viva, e o arquivo
    .duckdb aceita um único processo escrevendo. Com o ui.py de pé, a ingestão
    falha com "Cannot open file" e, no Windows, "O arquivo já está sendo usado
    por outro processo". Saber exatamente como desligar o servidor faz parte da
    ferramenta.

U3. NÃO CRIA BANCO
    Se projeto_1.duckdb não existir, diz que é preciso rodar a ingestão antes
    e encerra com sys.exit(1). Por isso não usa conectar() de pipeline/db.py, que
    cria o arquivo quando ele falta.
    Por quê: um banco vazio aberto no navegador parece ingestão quebrada.

U4. SEPARAÇÃO
    ui.py não importa pipeline/ingestao.py, e a ingestão não importa ui.py.
    Por quê: a ingestão não sobe servidor, não abre navegador e não depende da
    extensão ui para rodar.

U5. SAÍDA NA TELA
    stdout reconfigurado para utf-8.
    Por quê: no Prompt de Comando do Windows acento sai embaralhado por padrão.

U6. PORTA OCUPADA
    Se a porta 4213 já estiver em uso, diz isso e encerra com sys.exit(1), antes
    de abrir o banco.
    Por quê: sem essa checagem o script imprime a URL normalmente, e o navegador
    mostra o banco de quem ocupa a porta, como outro projeto. Parece que este
    banco está errado, e nada avisa.

CRITÉRIOS DE ACEITE
- uv run python ui.py abre o navegador em localhost:4213 e o schema raw aparece
  com as cinco tabelas.
- Ctrl+C devolve o terminal sem traceback, e logo depois a ingestão roda sem
  erro de lock.
- Renomear projeto_1.duckdb faz o ui.py parar com a mensagem, sem criar
  arquivo novo.
- Com a porta 4213 ocupada, o ui.py para com a mensagem, sem abrir o banco nem o
  navegador.
"""

import socket
import sys
import time
from pathlib import Path

import duckdb

BANCO = Path(__file__).resolve().parent / "projeto_1.duckdb"
PORTA = 4213


def porta_ocupada() -> bool:
    try:
        # "localhost" e não um IP: o servidor da extensão escuta em ::1.
        socket.create_connection(("localhost", PORTA), timeout=1).close()
    except OSError:
        return False
    return True


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    if not BANCO.is_file():
        print(f"Banco não encontrado: {BANCO}")
        print("Rode a ingestão antes: uv run python -m pipeline.ingestao")
        sys.exit(1)

    if porta_ocupada():
        print(f"A porta {PORTA} já está em uso, talvez por outro ui.py.")
        print("Encerre esse processo com Ctrl+C e rode de novo.")
        sys.exit(1)

    con = duckdb.connect(str(BANCO))
    try:
        con.execute("INSTALL ui; LOAD ui;")
        con.execute("CALL start_ui()").fetchall()
        print(f"http://localhost:{PORTA}", flush=True)
        print("Ctrl+C para encerrar", flush=True)
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        con.close()


if __name__ == "__main__":
    main()
