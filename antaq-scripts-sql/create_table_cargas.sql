
-- Drop table

-- DROP TABLE antaq.dbo.carga;

CREATE TABLE antaq.dbo.carga (
	id_carga nvarchar(MAX) NULL,
	id_atracacao nvarchar(MAX) NULL,
	cod_porto_origem nvarchar(MAX) NULL,
	cod_porto_destino nvarchar(MAX) NULL,
	cod_mercadoria nvarchar(MAX) NULL,
	tipo_operacao nvarchar(MAX) NULL,
	tipo_acondicionamento nvarchar(MAX) NULL,
	estado_container nvarchar(MAX) NULL,
	tipo_navegacao nvarchar(MAX) NULL,
	flag_autorizacao nvarchar(MAX) NULL,
	flag_cabotagem nvarchar(MAX) NULL,
	flag_cabotagem_movimentacao nvarchar(MAX) NULL,
	tamanho_container nvarchar(MAX) NULL,
	flag_longo_curso nvarchar(MAX) NULL,
	tipo_operacao_carga nvarchar(MAX) NULL,
	flag_offshore nvarchar(MAX) NULL,
	flag_via_interior nvarchar(MAX) NULL,
	percurso_vias_interiores nvarchar(MAX) NULL,
	percurso_interiores nvarchar(MAX) NULL,
	unica_natureza nvarchar(MAX) NULL,
	unico_capitulo nvarchar(MAX) NULL,
	unica_mercadoria nvarchar(MAX) NULL,
	natureza_carga nvarchar(MAX) NULL,
	sentido_operacao nvarchar(MAX) NULL,
	qtd_TEU_movimentacao int NULL,
	qtd_unidades_movimentadas int NULL,
	peso_bruto float NULL,
	peso_liquido float NULL,
	ano_atracacao nvarchar(MAX) NOT NULL,
	mes_atracacao nvarchar(MAX) NOT NULL,
	cod_porto_atracacao nvarchar(MAX) NOT NULL,
	cod_uf_atracacao nvarchar(MAX) NOT NULL
);

