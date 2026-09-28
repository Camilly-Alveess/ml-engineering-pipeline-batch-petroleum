"""
glue_job_media_movel.py
------------------------
Job do AWS Glue (Spark) responsável pela camada de TRANSFORMAÇÃO (curated).

O que este job faz:
1. Lê os dados RAW (parquet) gravados pelo scraper no S3
   (s3://<bucket>/raw/petroleo/...).
2. Calcula a média móvel do preço do petróleo em uma janela de 7 dias
   (1 semana), usando Window Function do Spark.
3. Grava o resultado em Parquet no S3 (camada curated/refined), também
   particionado por dia.
4. Cataloga automaticamente a tabela resultante no Glue Data Catalog,
   no banco de dados "default", através do parâmetro
   `enableUpdateCatalog=True` + `catalogDatabase`/`catalogTableName`
   do DynamicFrameWriter — dispensando a necessidade de rodar um Crawler
   separado.

Como publicar/rodar:
    aws glue create-job \
        --name job-media-movel-petroleo \
        --role AWSGlueServiceRole-techchallenge \
        --command Name=glueetl,ScriptLocation=s3://<bucket>/scripts/glue_job_media_movel.py,PythonVersion=3 \
        --default-arguments '{"--enable-glue-datacatalog":"true","--job-language":"python"}' \
        --glue-version "4.0"

    aws glue start-job-run --job-name job-media-movel-petroleo
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --------------------------------------------------------------------------
# Parâmetros do Job
# --------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "s3_bucket",          # ex: tech-challenge-petroleo-raw
        "raw_path",           # ex: raw/petroleo/
        "curated_path",       # ex: curated/petroleo_media_movel/
        "catalog_database",   # ex: default
        "catalog_table",      # ex: petroleo_media_movel
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["s3_bucket"]
RAW_PATH = f"s3://{BUCKET}/{args['raw_path']}"
CURATED_PATH = f"s3://{BUCKET}/{args['curated_path']}"
CATALOG_DB = args["catalog_database"]
CATALOG_TABLE = args["catalog_table"]

WINDOW_DIAS = 7  # janela de 1 semana

# --------------------------------------------------------------------------
# 1. Leitura dos dados RAW
# --------------------------------------------------------------------------
df_raw = spark.read.parquet(RAW_PATH)

df_raw = (
    df_raw
    .withColumn("data_referencia", F.to_date("data_referencia"))
    .withColumn("preco_petroleo", F.col("preco_petroleo").cast("double"))
    .dropDuplicates(["data_referencia"])
)

# --------------------------------------------------------------------------
# 2. Cálculo da média móvel de 7 dias (Window Function)
# --------------------------------------------------------------------------
# Janela ordenada pela data, olhando os 6 dias anteriores + o dia atual
# (equivalente a uma janela de 7 dias corridos / 1 semana).
janela_semanal = (
    Window.orderBy(F.col("data_referencia").cast("timestamp").cast("long"))
    .rangeBetween(-(WINDOW_DIAS - 1) * 86400, 0)
)

df_transformado = df_raw.withColumn(
    "media_movel_7d",
    F.round(F.avg("preco_petroleo").over(janela_semanal), 3),
)

# Colunas de partição diária (derivadas da data de referência)
df_transformado = (
    df_transformado
    .withColumn("ano", F.year("data_referencia"))
    .withColumn("mes", F.month("data_referencia"))
    .withColumn("dia", F.dayofmonth("data_referencia"))
)

# --------------------------------------------------------------------------
# 3 & 4. Escrita no S3 (particionada) + catalogação automática no Glue Catalog
# --------------------------------------------------------------------------
dynamic_frame = DynamicFrame.fromDF(df_transformado, glueContext, "dynamic_frame")

sink = glueContext.getSink(
    connection_type="s3",
    path=CURATED_PATH,
    enableUpdateCatalog=True,       # ativa a catalogação automática
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["ano", "mes", "dia"],
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase=CATALOG_DB, catalogTableName=CATALOG_TABLE)
sink.writeFrame(dynamic_frame)

job.commit()
