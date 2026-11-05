--Evolução anual por categoria
--Grão: categoria
WITH vendas_ano AS (
    SELECT YEAR(p.data_pedido) AS ano,
           pr.categoria,
           SUM(i.valor_total) AS receita
    FROM refined.itens_pedido AS i
    JOIN refined.pedidos_por_cliente AS p USING (pedido_id)
    JOIN refined.produtos AS pr USING (produto_id)
    WHERE p.status_pedido = 'Completed'
      AND i.pedido_id NOT IN (SELECT pedido_id FROM refined.itens_divergentes)
    GROUP BY ano, pr.categoria
)
SELECT categoria,
       ano,
       receita,
       ROUND(100.0 * (receita - LAG(receita) OVER w) / LAG(receita) OVER w, 2) AS crescimento_pct
FROM vendas_ano
WINDOW w AS (PARTITION BY categoria ORDER BY ano)
ORDER BY categoria, ano;