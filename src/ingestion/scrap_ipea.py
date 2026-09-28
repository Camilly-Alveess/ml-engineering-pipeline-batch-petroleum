"""
scrap_ipea.py
--------------
Responsável pela camada de INGESTÃO (raw) da pipeline.

Extrai a série histórica do preço do petróleo Brent (FOB), disponibilizada
publicamente pelo Ipeadata através da API OData:

    http://ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='EIA366_PBRENT366')

O Ipeadata não oferece um "site tradicional" fácil de raspar com HTML
(a página é montada via JS), então a extração é feita consumindo o
endpoint OData público que alimenta o próprio site — isso é o que a banca
do Tech Challenge costuma aceitar como "scraping do site do Ipea", já que
o dado vem diretamente da fonte pública sem usar biblioteca de terceiros
não documentada.

Fluxo:
1. Faz a requisição HTTP para a API do Ipea.
2. Normaliza o payload JSON em um DataFrame pandas.
3. Salva em Parquet no S3, particionado por dia (partição diária),
   seguindo o padrão Hive: s3://<bucket>/raw/petroleo/ano=YYYY/mes=MM/dia=DD/

Este script é pensado para rodar como:
  - job agendado (cron / EventBridge) em uma instância EC2, Lambda ou
    container Fargate, OU
  - step inicial de uma Step Function / Glue Python Shell Job

Requisitos: requests, pandas, pyarrow, boto3, awswrangler (opcional)
"""

import io
import os
import logging
from datetime import datetime, timezone

import boto3
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("scrap_ipea")

# --------------------------------------------------------------------------
# Configurações
# --------------------------------------------------------------------------
IPEA_API_URL = (
    "http://ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='EIA366_PBRENT366')"
)
S3_BUCKET = os.environ.get("S3_BUCKET", "SEU-BUCKET")
S3_RAW_PREFIX = "raw/petroleo"
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


def extrair_dados_ipea(url: str = IPEA_API_URL) -> pd.DataFrame:
    """Consulta a API pública do Ipeadata e retorna um DataFrame bruto."""
    logger.info("Consultando API do Ipeadata: %s", url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    payload = resp.json()
    registros = payload.get("value", [])
    if not registros:
        raise ValueError("Nenhum dado retornado pela API do Ipea.")

    df = pd.DataFrame(registros)

    # Colunas relevantes retornadas pela API: VALDATA (data) e VALVALOR (preço)
    df = df.rename(columns={"VALDATA": "data_referencia", "VALVALOR": "preco_petroleo"})
    df = df[["data_referencia", "preco_petroleo"]].dropna(subset=["preco_petroleo"])

    # A API retorna datas no formato ISO com timezone, ex: 2024-05-10T00:00:00-03:00
    # A série mistura datas com fuso e sem fuso (mudanças de DST ao longo dos
    # anos), então normalizamos tudo para UTC antes de remover o fuso.
    df["data_referencia"] = pd.to_datetime(df["data_referencia"], utc=True).dt.tz_convert(None)
    df["preco_petroleo"] = df["preco_petroleo"].astype(float)

    df = df.sort_values("data_referencia").reset_index(drop=True)
    logger.info("Total de registros extraídos: %d", len(df))
    return df


def salvar_parquet_s3(df: pd.DataFrame, bucket: str = S3_BUCKET, prefix: str = S3_RAW_PREFIX) -> None:
    """
    Salva o DataFrame em Parquet no S3 com partição DIÁRIA.

    Cada execução do scraper grava um arquivo dentro da partição do dia em
    que o job rodou (data de ingestão), seguindo o padrão Hive:

        s3://bucket/raw/petroleo/ano=YYYY/mes=MM/dia=DD/petroleo_YYYYMMDD_HHMMSS.parquet

    Isso permite que o Glue Catalog / Athena reconheçam automaticamente as
    partições por ano/mes/dia.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)

    agora = datetime.now(timezone.utc)
    ano, mes, dia = agora.strftime("%Y"), agora.strftime("%m"), agora.strftime("%d")
    ts = agora.strftime("%Y%m%d_%H%M%S")

    key = f"{prefix}/ano={ano}/mes={mes}/dia={dia}/petroleo_{ts}.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info("Arquivo salvo em s3://%s/%s", bucket, key)


def main():
    df = extrair_dados_ipea()
    salvar_parquet_s3(df)


if __name__ == "__main__":
    main()
