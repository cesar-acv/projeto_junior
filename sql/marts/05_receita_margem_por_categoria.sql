--Receita e margem por categoria
--Grão: categoria de produto
SELECT pr.categoria,
       SUM(i.valor_total) AS receita,
       SUM(i.venda_bruta - i.desconto - i.custo_produto) AS lucro,
       ROUND(100.0 * SUM(i.venda_bruta - i.desconto - i.custo_produto)
             / SUM(i.venda_bruta - i.desconto), 2) AS margem_pct
FROM refined.itens_pedido AS i
JOIN refined.pedidos_por_cliente AS p USING (pedido_id)
JOIN refined.produtos AS pr USING (produto_id)
WHERE p.status_pedido = 'Completed'
  AND i.pedido_id NOT IN (SELECT pedido_id FROM refined.itens_divergentes)
GROUP BY pr.categoria
ORDER BY receita DESC;