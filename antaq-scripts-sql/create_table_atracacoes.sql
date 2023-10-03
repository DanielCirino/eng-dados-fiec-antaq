
-- Drop table

-- DROP TABLE antaq.dbo.atracacao;

CREATE TABLE antaq.dbo.atracacao (
	id_atracacao nvarchar(MAX)  NULL,
	cod_porto_informante nvarchar(MAX)  NULL,
	id_berco nvarchar(MAX)  NULL,
	nome_berco nvarchar(MAX)  NULL,
	nome_porto_informante nvarchar(MAX)  NULL,
	alias_porto_informante nvarchar(MAX)  NULL,
	complexo_portuario nvarchar(MAX)  NULL,
	tipo_autoridade_portuaria nvarchar(MAX)  NULL,
	dt_chegada datetime NULL,
	dt_atracacao datetime NULL,
	dt_inicio_operacao datetime NULL,
	dt_fim_operacao datetime NULL,
	dt_desatracacao datetime NULL,
	tipo_operacao nvarchar(MAX)  NULL,
	tipo_navegacao nvarchar(MAX)  NULL,
	nacionalidade_armador nvarchar(MAX)  NULL,
	contabiliza_movimentacao nvarchar(MAX)  NULL,
	terminal nvarchar(MAX)  NULL,
	municipio nvarchar(MAX)  NULL,
	nome_uf nvarchar(MAX)  NULL,
	cod_uf nvarchar(MAX)  NULL,
	regiao_hidrografica nvarchar(MAX)  NULL,
	cod_capitania nvarchar(MAX)  NULL,
	cod_IMO nvarchar(MAX)  NULL,
	tempo_espera_atracacao float NULL,
	tempo_espera_operacao float NULL,
	tempo_operacao float NULL,
	tempo_espera_desatracacao float NULL,
	tempo_atracado float NULL,
	tempo_estadia float NULL
);