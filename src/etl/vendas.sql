with tb_pedido_item as(
select t2.*
        , t1.dtPedido
from silver.olist.pedido t1
left join silver.olist.item_pedido t2 on t1.idPedido = t2.idPedido
where t1.dtPedido < '{date}' and t1.dtPedido >= add_months('{date}', -6)
and t2.idVendedor is not null
),
tb_summary as(
select idVendedor
      , count(distinct idPedido) as qtdPedidos
      , count(distinct date(dtPedido)) as qtdDias
      , count(idProduto) as qtItens
      , min(datediff('{date}', dtPedido)) as qtdRecencia
      , sum(vlPreco) / count(distinct idPedido) as avgTicket
      , avg(vlPreco) as avgValorProduto
      , max(vlPreco) as maxValorProduto
      , min(vlPreco) as minValorProduto
from tb_pedido_item
group by 1
),
tb_pedido_summary as(
select idVendedor
        , idPedido
        , sum(vlPreco) as vlPreco
from tb_pedido_item
group by 1, 2
),
tb_min_max as(
select idVendedor
        , min(vlPreco) as minVlPreco
        , max(vlPreco) as maxVlPreco
from tb_pedido_summary
group by 1
),
tb_life as(
select t2.idVendedor
        , sum(vlPreco) as LTV
        , max(datediff('{date}', dtPedido)) as qtdeDiasBase
from silver.olist.pedido t1
left join silver.olist.item_pedido t2 on t1.idPedido = t2.idPedido
where t1.dtPedido < '{date}' and t1.dtPedido >= add_months('{date}', -6)
and t2.idVendedor is not null
group by 1
),
tb_dtpedido as(
select distinct idVendedor
      , date(dtPedido) as dtPedido
from tb_pedido_item
order by 1, 2
),
tb_lag as(
select * 
      , lag(dtPedido) over (partition by idVendedor order by dtPedido) as lag1
from tb_dtpedido
),
tb_intervalo as(
select idVendedor
        , avg(datediff(dtPedido, lag1)) as avgIntervaloVendas
from tb_lag
group by 1
)
select '{date}' as dtReference
        , NOW() as dtIngestion
        , t1.*
        , t2.minVlPreco
        , t2.maxVlPreco
        , t3.LTV
        , t3.qtdeDiasBase
        , t4.avgIntervaloVendas
from tb_summary t1
left join tb_min_max t2 on t1.idVendedor = t2.idVendedor
left join tb_life t3 on t1.idVendedor = t3.idVendedor
left join tb_intervalo t4 on t1.idVendedor = t4.idVendedor