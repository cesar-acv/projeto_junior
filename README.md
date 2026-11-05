# projeto_junior

Projeto do curso EmpregaDados. Aqui eu monto um pipeline de dados com DuckDB para analisar as vendas de um e-commerce. Os dados vêm do Kaggle: [E-Commerce Sales and Customer Analytics](https://www.kaggle.com/datasets/datascikhan/e-commerce-sales-and-customer-analytics).

## Como funciona

O banco `projeto_1.duckdb` é montado em três etapas, cada uma num schema:

- **raw**: os cinco CSVs da pasta `raw/` carregados do jeito que vieram, tudo como texto.
- **refined**: as colunas renomeadas para português e convertidas para o tipo certo (data, hora, dinheiro em `DECIMAL`, notas). Ficam aqui `clientes`, `pedidos_por_cliente`, `itens_pedido`, `produtos` e `itens_divergentes`.
- **marts**: os indicadores prontos. Cada arquivo de `sql/marts/` vira uma tabela, como vendas por ano, receita por forma de pagamento, entregas no prazo e margem por categoria.

## Como executar

Requer o [uv](https://docs.astral.sh/uv/). Na pasta do projeto, rode na ordem:

```
uv run python -m pipeline.ingestao
uv run python -m pipeline.transformacao
uv run python -m pipeline.marts
```

Para abrir o banco no navegador, em http://localhost:4213:

```
uv run python ui.py
```

Use Ctrl+C para encerrar. Enquanto a interface estiver aberta, o banco fica travado e as etapas do pipeline não conseguem gravar nele.

## Testes

```
uv run pytest --cov=pipeline
```

Os testes usam dados pequenos criados no próprio teste. Eles não dependem do `projeto_1.duckdb` nem dos CSVs.

## Um ponto de atenção nos dados

Em 11.226 dos 138.116 pedidos, a soma dos itens em `itens_pedido` passa do valor total do pedido em `pedidos_por_cliente`. A diferença soma 15.264.613,55. A página do Kaggle não explica o motivo. O mais provável é que alguns códigos de pedido tenham sido usados mais de uma vez na geração dos dados.

Não apaguei nenhuma linha da tabela refined.itens_pedido. Os itens desses pedidos estão separados em `refined.itens_divergentes`, para conferência. Nos indicadores por produto e categoria, esses pedidos ficam de fora. Os totais financeiros usam `pedidos_por_cliente`.