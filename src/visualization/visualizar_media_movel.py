"""
visualizar_media_movel.py (versão CloudShell)
----------------------------------------------
Consulta a tabela default.petroleo_media_movel no Athena (via boto3, sem
dependências extras além de pandas/matplotlib) e gera o gráfico de preço
diário vs. média móvel de 7 dias.

Saída: media_movel_petroleo.png (na pasta atual)
"""

import io
import os
import time

import boto3
import matplotlib

matplotlib.use("Agg")  # sem interface gráfica (CloudShell)
import matplotlib.pyplot as plt
import pandas as pd

BUCKET = os.environ.get("S3_BUCKET", "SEU-BUCKET")
REGION = os.environ.get("AWS_REGION", "us-east-1")
DATABASE = "default"
OUTPUT_LOCATION = f"s3://{BUCKET}/athena-results/"

QUERY = """
SELECT data_referencia, preco_petroleo, media_movel_7d
FROM default.petroleo_media_movel
ORDER BY data_referencia
"""

athena = boto3.client("athena", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def consultar_athena(query: str) -> pd.DataFrame:
    execucao = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )
    query_id = execucao["QueryExecutionId"]
    print(f"Query enviada ao Athena: {query_id}")

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        estado = status["State"]
        if estado in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if estado != "SUCCEEDED":
        raise RuntimeError(f"Query {estado}: {status.get('StateChangeReason')}")

    # O Athena grava o resultado como CSV no S3
    chave = f"athena-results/{query_id}.csv"
    obj = s3.get_object(Bucket=BUCKET, Key=chave)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def plotar(df: pd.DataFrame, saida: str = "media_movel_petroleo.png") -> None:
    df["data_referencia"] = pd.to_datetime(df["data_referencia"])
    df = df.sort_values("data_referencia")

    # Visão dos últimos 5 anos para destacar a análise recente
    corte = df["data_referencia"].max() - pd.DateOffset(years=5)
    recente = df[df["data_referencia"] >= corte]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 10))

    ax1.plot(df["data_referencia"], df["preco_petroleo"],
             color="#8C4FFF", alpha=0.3, linewidth=0.8, label="Preço diário (Brent)")
    ax1.plot(df["data_referencia"], df["media_movel_7d"],
             color="#FF9900", linewidth=1.4, label="Média móvel (7 dias)")
    ax1.set_title("Preço do Petróleo Brent — Histórico completo", fontsize=13, weight="bold")
    ax1.set_ylabel("US$/barril")
    ax1.legend()
    ax1.grid(alpha=0.25)

    ax2.plot(recente["data_referencia"], recente["preco_petroleo"],
             color="#8C4FFF", alpha=0.3, linewidth=1, label="Preço diário (Brent)")
    ax2.plot(recente["data_referencia"], recente["media_movel_7d"],
             color="#FF9900", linewidth=2, label="Média móvel (7 dias)")
    ax2.set_title("Últimos 5 anos — média móvel suaviza a volatilidade diária",
                  fontsize=13, weight="bold")
    ax2.set_xlabel("Data")
    ax2.set_ylabel("US$/barril")
    ax2.legend()
    ax2.grid(alpha=0.25)

    plt.tight_layout()
    plt.savefig(saida, dpi=150)
    print(f"Gráfico salvo em {saida}")


if __name__ == "__main__":
    dados = consultar_athena(QUERY)
    print(f"{len(dados)} linhas carregadas do Athena")
    plotar(dados)
