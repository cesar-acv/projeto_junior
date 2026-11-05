--Venda por faixa etária
--Grão: idade
SELECT CASE
           WHEN idade < 25 THEN '18-24'
           WHEN idade < 35 THEN '25-34'
           WHEN idade < 45 THEN '35-44'
           WHEN idade < 55 THEN '45-54'
           WHEN idade < 65 THEN '55-64'
           ELSE '65+'
       END AS faixa_etaria,
       COUNT(DISTINCT cliente_id) AS clientes,
       COUNT(*) AS pedidos,
       SUM(valor_total) AS receita,
       ROUND(SUM(valor_total) / COUNT(*), 2) AS ticket_medio,
       ROUND(SUM(valor_total) / COUNT(DISTINCT cliente_id), 2) AS receita_por_cliente,
       ROUND(100.0 * SUM(valor_total) / SUM(SUM(valor_total)) OVER (), 2) AS pct_receita
FROM refined.pedidos_por_cliente
WHERE status_pedido = 'Completed'
GROUP BY faixa_etaria
ORDER BY faixa_etaria;