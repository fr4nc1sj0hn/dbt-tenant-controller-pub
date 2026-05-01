import os
from provisioning.postgres import PostgresSchemaProvisioner

def get_schema_provisioner(session):
    backend = os.getenv("WAREHOUSE_BACKEND", "postgres")

    if backend == "postgres":
        return PostgresSchemaProvisioner(session)

    raise ValueError(f"Unsupported backend: {backend}")