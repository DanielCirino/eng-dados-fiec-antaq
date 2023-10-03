WITH atracacoes_mes as (
		select
		dt.ano,
		dt.mes,
		dl.cod_uf ,
		dl.regiao_hidrografica,
		count(*) qtd_atracacoes,
		sum(af.tempo_espera_atracacao) tempo_espera_atracacao,
		sum(af.tempo_atracado) tempo_atracado
	from atracacoes_fato  af
		left join dim_tempo dt on CAST(af.dt_atracacao as date) = CAST (dt.[data] as date )
		left join dim_localizacao dl on dl.municipio =af.municipio
		left join dim_porto dp on dp.cod_porto_informante = af.cod_porto_informante
	where dt.ano in ('2023','2022','2021')
		group by
		dt.ano, dt.mes,
		dl.cod_uf ,
		dl.regiao_hidrografica
)
SELECT
	cod_uf,
	regiao_hidrografica,
	ano ano_atual,
	mes mes_atual,
	qtd_atracacoes,
	tempo_espera_atracacao,
	tempo_atracado,
	 LAG(ano,12) OVER ( ORDER BY ano, mes) AS ano_anterior,
	 LAG(mes,12) OVER ( ORDER BY ano, mes) AS mes_comparacao,
	 LAG(qtd_atracacoes,12) OVER ( ORDER BY ano, mes) AS qtd_atracacoes_periodo_anterior,
	 qtd_atracacoes - LAG(qtd_atracacoes,12) OVER (ORDER BY ano, mes) AS diferenca,
	 (qtd_atracacoes - LAG(qtd_atracacoes,12) OVER (ORDER BY ano, mes)) / CAST(qtd_atracacoes AS DECIMAL) AS variacao_percentual
from atracacoes_mes
where 1=1
	-- and cod_uf='CE'
order by ano,mes