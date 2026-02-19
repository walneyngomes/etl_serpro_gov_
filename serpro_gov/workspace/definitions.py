from dagster import Definitions
from assets.bronze.bronze_promocoes import bronze_promocoes
from resources.api_client import api_client
from assets.silver.silver_mod import silver_mod
from assets.silver.silver_nmpr_sit import silver_nmpr

defs = Definitions(
    assets=[bronze_promocoes,silver_mod,silver_nmpr],
    resources={
        "api_client": api_client
    }
)