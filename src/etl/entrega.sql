with tb_pedido as(
select t1.idPedido
      , t2.idVendedor
      , t1.descSituacao
      , t1.dtPedido
      , t1.dtAprovado
      , t1.dtEntregue
      , t1.dtEstimativaEntrega
      , Sum(vlFrete) as TotalFrete
from silver.olist.pedido t1
left join silver.olist.item_pedido t2 on t1.idPedido = t2.idPedido
where dtPedido < '{date}' and dtPedido >= add_months('{date}', -6)
and idVendedor is not null
group by 1, 2, 3, 4, 5, 6, 7
)
select '{date}' as dtReference
        , NOW() as dtIngestion
        , idVendedor
        , count(case when date(coalesce(dtEntregue, '{date}')) > date(dtEstimativaEntrega) then idPedido end) 
            / count(case when descSituacao = 'delivered' then idPedido end) as pctPedidoAtraso
        , count(distinct case when descSituacao = 'canceled' then idPedido end) / count(distinct idPedido) as pctPedidoCancelado
        , avg(TotalFrete) as avgFrete
        , percentile(TotalFrete, 0.5) as medianFrete
        , max(TotalFrete) as maxFrete
        , min(TotalFrete) as minFrete
        , avg(datediff(coalesce(dtEntregue, '{date}'), dtAprovado)) as qtdDiasAprovadoEntrega
        , avg(datediff(coalesce(dtEntregue, '{date}'), dtPedido)) as qtdDiasPedidoEntrega
        , avg(datediff(dtEstimativaEntrega, coalesce(dtEntregue, '{date}'))) as qtdeDiasEntregaPromessa
from tb_pedido
group by 1, 2, 3