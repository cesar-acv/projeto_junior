--Vendas por dia da semana
--Grão: dia da semana
SELECT ISODOW(data_pedido) AS num_dia,
       CASE ISODOW(data_pedido)
           WHEN 1 THEN 'Segunda'
           WHEN 2 THEN 'Terça'
           WHEN 3 THEN 'Quarta'
           WHEN 4 THEN 'Quinta'
           WHEN 5 THEN 'Sexta'
           WHEN 6 THEN 'Sábado'
           WHEN 7 THEN 'Domingo'
       END AS dia_semana,
       COUNT(*) AS pedidos,
       SUM(valor_total) AS receita,
       ROUND(SUM(valor_total) / COUNT(*), 2) AS ticket_medio,
       ROUND(SUM(valor_total) / COUNT(DISTINCT data_pedido), 2) AS receita_media_por_dia,
       ROUND(100.0 * SUM(valor_total) / SUM(SUM(valor_total)) OVER (), 2) AS pct_receita
FROM refined.pedidos_por_cliente
WHERE status_pedido = 'Completed'
GROUP BY num_dia, dia_semana
ORDER BY num_dia;