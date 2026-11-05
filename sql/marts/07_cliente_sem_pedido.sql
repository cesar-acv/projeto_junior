--Cliente cadastrado que não fez pedido
--Grão: cliente
WITH resumo_cliente AS (
    SELECT c.cliente_id,
           COUNT(p.pedido_id) AS total_pedidos,
           COUNT(p.pedido_id) FILTER (WHERE p.status_pedido = 'Completed') AS pedidos_concluidos
    FROM refined.clientes AS c
    LEFT JOIN refined.pedidos_por_cliente AS p USING (cliente_id)
    GROUP BY c.cliente_id
)
SELECT COUNT(*) AS clientes_cadastrados,
       COUNT(*) FILTER (WHERE pedidos_concluidos = 0) AS sem_pedido_concluido,
       COUNT(*) FILTER (WHERE total_pedidos = 0) AS nunca_fizeram_pedido,
       COUNT(*) FILTER (WHERE total_pedidos > 0 AND pedidos_concluidos = 0) AS so_pedidos_nao_concluidos,
       ROUND(100.0 * COUNT(*) FILTER (WHERE pedidos_concluidos = 0) / COUNT(*), 2) AS pct_sem_pedido_concluido,
FROM resumo_cliente;