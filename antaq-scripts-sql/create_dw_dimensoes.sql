-- antaq_dw.dbo.dim_berco definition

-- Drop table

-- DROP TABLE antaq_dw.dbo.dim_berco;

CREATE TABLE antaq_dw.dbo.dim_berco (
	id_berco nvarchar(MAX) NULL,
	nome_berco nvarchar(MAX) NULL
);


-- antaq_dw.dbo.dim_localizacao definition

-- Drop table

-- DROP TABLE antaq_dw.dbo.dim_localizacao;

CREATE TABLE antaq_dw.dbo.dim_localizacao (
	municipio nvarchar(MAX) NULL,
	cod_uf nvarchar(MAX) NULL,
	nome_uf nvarchar(MAX) NULL,
	regiao_hidrografica nvarchar(MAX) NULL
);


-- antaq_dw.dbo.dim_porto definition

-- Drop table

-- DROP TABLE antaq_dw.dbo.dim_porto;

CREATE TABLE antaq_dw.dbo.dim_porto (
	cod_porto_informante nvarchar(MAX) NULL,
	cod_uf nvarchar(MAX) NULL
);


-- antaq_dw.dbo.dim_tempo definition

-- Drop table

-- DROP TABLE antaq_dw.dbo.dim_tempo;

CREATE TABLE antaq_dw.dbo.dim_tempo (
	[data] datetime NOT NULL,
	ano nvarchar(MAX) NOT NULL,
	mes nvarchar(MAX) NOT NULL,
	dia nvarchar(MAX) NOT NULL,
	ano_mes nvarchar(MAX) NOT NULL,
	trimestre nvarchar(MAX) NOT NULL
);