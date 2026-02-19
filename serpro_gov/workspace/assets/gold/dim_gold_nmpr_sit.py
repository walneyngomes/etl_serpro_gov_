from dagster import asset, Definitions, AssetMaterialization, AssetKey
import json
from resources.api_client import api_client
import os
import requests
import psycopg2
from psycopg2.extras import execute_values


@asset(required_resource_keys={"api_client"})
def silver_nmpr(context):
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
        CREATE TABLE IF NOT EXISTS dim_gold_nmpr_sit AS
        select nome_promocao, count(*) from promocoes_bronzer group by nome_promocao
    """)
    conn.commit()
    cur.close()
    conn.close()

    context.log_event(AssetMaterialization(asset_key=AssetKey("dim_gold_nmpr_sit")))

    
