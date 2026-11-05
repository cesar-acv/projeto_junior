--Qual a media da nota por situação de entrega
--Grão: situação de entrega
SELECT status_entrega,
       COUNT(*) AS pedidos,
       ROUND(AVG(nota_cliente), 2) AS nota_media
FROM refined.pedidos_por_cliente
WHERE status_pedido = 'Completed'
GROUP BY status_entrega
ORDER BY nota_media DESC;