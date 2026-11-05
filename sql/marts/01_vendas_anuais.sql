--Qual o volume de vendas anuais?
--Grão: um ano de compras
SELECT YEAR(data_pedido) AS ano,
       COUNT(*) AS pedidos,
       SUM(valor_total) AS vendas_totais
FROM refined.pedidos_por_cliente
WHERE status_pedido = 'Completed'
GROUP BY ano
ORDER BY ano;