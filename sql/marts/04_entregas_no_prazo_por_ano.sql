--Entregas no prazo por ano
--Grão: ano de entrega
SELECT YEAR(data_pedido) AS ano,
       COUNT(*) AS entregas,
       ROUND(100.0 * COUNT(*) FILTER (WHERE status_entrega IN ('On Time', 'Early')) / COUNT(*), 2) AS pct_no_prazo
FROM refined.pedidos_por_cliente
WHERE status_pedido = 'Completed'
GROUP BY ano
ORDER BY ano;