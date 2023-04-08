with tb_pedido as(
  select
    distinct t1.idPedido,
    t2.idVendedor
  from
    silver.olist.pedido t1
    left join silver.olist.item_pedido t2 on t1.idPedido = t2.idPedido
  where
    t1.dtPedido < '{date}'
    and t1.dtPedido >= add_months('{date}', -6)
    and idVendedor is not null
),
tb_join as(
  select
    t1.*,
    t2.vlNota
  from
    tb_pedido t1
    left join silver.olist.avaliacao_pedido t2 on t1.idPedido = t2.idPedido
),
tb_summary as(
  select
    idVendedor,
    avg(vlNota) as avgNota,
    percentile(vlNota, 0.5) as medianNota,
    max(vlNota) as maxNota,
    min(vlNota) as minNota,
    count(vlNota) / count(idPedido) as pctAvaliacao
  from
    tb_join
  group by
    1
)
select
  '{date}' as dtReference
  , NOW() as dtREference
  , *
from
  tb_summary