-- ============================================================================
-- queries.sql
-- Consultas no Amazon Athena sobre a tabela curated gerada pelo Glue Job
-- (a tabela já é criada automaticamente no Glue Catalog pelo job, então
-- normalmente NÃO é necessário rodar um CREATE TABLE manual — mas ele é
-- incluído aqui para fins de documentação/demonstração, caso se opte por
-- criar a tabela manualmente ou repará-la após alteração de partições).
-- ============================================================================

-- 1) (Opcional/demonstrativo) DDL equivalente ao que o Glue Job cria
--    automaticamente no banco "default" do Glue Data Catalog.
CREATE EXTERNAL TABLE IF NOT EXISTS default.petroleo_media_movel (
    data_referencia   date,
    preco_petroleo    double,
    media_movel_7d    double
)
PARTITIONED BY (
    ano int,
    mes int,
    dia int
)
STORED AS PARQUET
LOCATION 's3://SEU-BUCKET/curated/petroleo_media_movel/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- 2) Caso as partições não apareçam automaticamente (o Glue Job com
--    enableUpdateCatalog=True já cuida disso, mas fica registrado o
--    comando equivalente para um Crawler/CREATE TABLE manual):
MSCK REPAIR TABLE default.petroleo_media_movel;

-- 3) Seleção simples para validar a carga dos dados
SELECT
    data_referencia,
    preco_petroleo,
    media_movel_7d
FROM default.petroleo_media_movel
ORDER BY data_referencia DESC
LIMIT 20;

-- 4) Preço médio móvel por ano (visão agregada para análise histórica)
SELECT
    ano,
    ROUND(AVG(preco_petroleo), 2)   AS preco_medio_anual,
    ROUND(AVG(media_movel_7d), 2)   AS media_movel_media_anual,
    ROUND(MIN(preco_petroleo), 2)   AS preco_minimo,
    ROUND(MAX(preco_petroleo), 2)   AS preco_maximo
FROM default.petroleo_media_movel
GROUP BY ano
ORDER BY ano;

-- 5) Maiores variações diárias entre o preço e a média móvel (outliers)
SELECT
    data_referencia,
    preco_petroleo,
    media_movel_7d,
    ROUND(preco_petroleo - media_movel_7d, 3) AS desvio_vs_media_movel
FROM default.petroleo_media_movel
ORDER BY ABS(preco_petroleo - media_movel_7d) DESC
LIMIT 15;

-- 6) Últimos 12 meses (para o gráfico de tendência recente)
SELECT
    data_referencia,
    preco_petroleo,
    media_movel_7d
FROM default.petroleo_media_movel
WHERE data_referencia >= date_add('month', -12, current_date)
ORDER BY data_referencia;
