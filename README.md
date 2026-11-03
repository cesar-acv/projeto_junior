# projeto_junior
Projeto do curso EmpregaDados para análise de vendas e E-Commerce retirados do link https://www.kaggle.com/datasets/datascikhan/e-commerce-sales-and-customer-analytics

## Como executar

Requer o [uv](https://docs.astral.sh/uv/). Na pasta do projeto:

```
uv run python -m pipeline.ingestao
uv run python ui.py
```

O primeiro comando cria o `projeto_1.duckdb` com as cinco tabelas do schema `raw`.
O segundo abre o banco no navegador, em http://localhost:4213. Use Ctrl+C para encerrar.