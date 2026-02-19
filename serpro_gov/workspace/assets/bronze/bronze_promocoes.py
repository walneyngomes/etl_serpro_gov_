from dagster import asset, Definitions, AssetMaterialization, AssetKey
import json
from resources.api_client import api_client
import os
import requests
import psycopg2
from psycopg2.extras import execute_values

url = 'https://api.scpc.estaleiro.serpro.gov.br/v1/promocao-comercial/export/json?anoPromocao=2025'

@asset(required_resource_keys={"api_client"})
def bronze_promocoes(context):
        # 2. Conectar ao PostgreSQL
    data = requests.get(url).json()

    promocoes = [
        (
            p.get("numeroPromocao"),
            p.get("nome"),
            p.get("modalidade"),
            p.get("situacao"),
            p.get("dataInicio"),
            p.get("dataFim")
        )
        for p in data
    ]

    
    conn = psycopg2.connect(
        host="postgres",        # ou "127.0.0.1"
        port=5432,
        database="dagster_db",   # banco definido no container
        user="dagster_user",     # usuário definido no container
        password="dagster_pass"  # senha definida no container
    )

    cur = conn.cursor()

    # 3. Criar tabela se não existir
    cur.execute("""
        CREATE TABLE IF NOT EXISTS promocoes_bronzer (
            numero_promocao TEXT,
            nome_promocao TEXT,
            modalidade TEXT,
            situacao TEXT,
            data_inicio DATE,
            data_fim DATE
        )
    """)
    conn.commit()
    # 4. Inserir dados
    sql = "INSERT INTO promocoes_bronzer (numero_promocao, nome_promocao, modalidade, situacao, data_inicio, data_fim) VALUES %s"
    execute_values(cur, sql, promocoes)

    conn.commit()
    cur.close()
    conn.close()

    context.log_event(AssetMaterialization(asset_key=AssetKey("bronze_promocoes")))

    return data

