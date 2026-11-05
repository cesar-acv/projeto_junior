--Relação de nota de produto por quantidade de receita
--Grão: nota
WITH receita_produto AS (
    SELECT pr.produto_id,
           pr.nota_produto,
           SUM(i.quantidade) AS unidades,
           SUM(i.valor_total) AS receita
    FROM refined.produtos AS pr
    LEFT JOIN refined.itens_pedido AS i
           ON i.produto_id = pr.produto_id
          AND i.pedido_id NOT IN (SELECT pedido_id FROM refined.itens_divergentes)
          AND i.pedido_id IN (SELECT pedido_id FROM refined.pedidos_por_cliente
                              WHERE status_pedido = 'Completed')
    GROUP BY pr.produto_id, pr.nota_produto
)
SELECT CASE
           WHEN nota_produto < 3.0 THEN '1. abaixo de 3,0'
           WHEN nota_produto < 3.5 THEN '2. 3,0 a 3,4'
           WHEN nota_produto < 4.0 THEN '3. 3,5 a 3,9'
           WHEN nota_produto < 4.5 THEN '4. 4,0 a 4,4'
           ELSE '5. 4,5 a 5,0'
       END AS faixa_nota,
       COUNT(*) AS produtos,
       SUM(unidades) AS unidades,
       SUM(receita) AS receita,
       ROUND(SUM(receita) / COUNT(*), 2) AS receita_por_produto,
       ROUND(100.0 * SUM(receita) / SUM(SUM(receita)) OVER (), 2) AS pct_receita
FROM receita_produto
GROUP BY faixa_nota
ORDER BY faixa_nota;