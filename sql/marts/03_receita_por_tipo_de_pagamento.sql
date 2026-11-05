--Receita por método de pagamento
--Grão: metodo de pagamento
SELECT forma_pagamento,
       COUNT(*) AS pedidos,
       SUM(valor_total) AS receita,
       ROUND(100.0 * SUM(valor_total) / SUM(SUM(valor_total)) OVER (), 2) AS participacao_pct,
       ROUND(AVG(valor_total), 2) AS ticket_medio
FROM refined.pedidos_por_cliente
WHERE status_pedido = 'Completed'
AND status_pagamento = 'Paid'
GROUP BY forma_pagamento
ORDER BY receita DESC;