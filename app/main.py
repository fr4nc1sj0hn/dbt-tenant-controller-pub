import os
from fastapi import FastAPI, Depends
from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus
from project_generator import DBTProjectGenerator
from provisioning.factory import get_schema_provisioner
from provisioning.base import SchemaProvisioner
from dotenv import load_dotenv
import subprocess


# Load environment variables
load_dotenv()

DBT_USER = os.getenv("DBT_USER", "admin")
DBT_USER_PASSWORD = os.getenv("DBT_USER_PASSWORD","")
POSTGRES_DB = os.getenv("POSTGRES_DB", "analytics")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres_local")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

# URL-encode the password
password_escaped = quote_plus(ADMIN_PASSWORD)

DATABASE_URL = f"postgresql://{ADMIN_USER}:{password_escaped}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

app = FastAPI()

BASE_PATH = "/app/tenants"
SCHEMA = os.getenv("DBT_SCHEMA", "staging_customer1")


dw_conn_params = {
    "host": POSTGRES_HOST,
    "user": DBT_USER,
    "password": DBT_USER_PASSWORD,
    "dbname": POSTGRES_DB,
    "port": POSTGRES_PORT
}

dbt_generator = DBTProjectGenerator(
    base_path=BASE_PATH, 
    conn_params=dw_conn_params
)

def get_db():
    with SessionLocal() as session:
        yield session


@app.post("/provision-tenant/{tenant_name}")
def provision_tenant(tenant_name: str, session=Depends(get_db)):
    """
    Creates raw, staging, and marts schemas for a tenant
    and grants dbt-friendly permissions
    """
    if not tenant_name.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid tenant name")


    provisioner = get_schema_provisioner(session)
    provisioner.provision_tenant(tenant_name)
    session.commit()

    dbt_generator.generate_dbt_project(tenant_name)

    return {
        "status": "success",
        "schemas": [
            f"raw_{tenant_name}",
            f"staging_{tenant_name}",
            f"marts_{tenant_name}"
        ],
        "backend": "postgres",
        "message": f"Tenant project created at {BASE_PATH}"
    }


@app.post("/generate-tenant/{tenant_name}")
def generate_tenant(tenant_name: str):
    """
    Generate a dynamic dbt project for the given tenant
    """
    dbt_generator.generate_dbt_project(tenant_name)
    return {"message": f"Tenant project created at {BASE_PATH}"}

@app.get("/test-db")
def test_db():
    with SessionLocal() as session:
        result = session.execute(text("SELECT NOW()"))
        return {"now": result.scalar()}

@app.post("/test-a-method/{tenant_name}")
def test_a_method(tenant_name: str, session=Depends(get_db)):

    dbt_generator._test_method(tenant_name)

@app.get("/debug-db-context")
def debug_db_context():
    with SessionLocal() as session:
        result = session.execute(text("""
            SELECT
                current_user,
                session_user,
                current_database(),
                inet_server_addr()::text AS server_ip,
                inet_client_addr()::text AS client_ip,
                current_setting('search_path')
        """)).mappings().one()

        return dict(result)
    
    
DBT_BASE_PATH = "/app/tenants"

@app.post("/run-dbt/{tenant}/{command}")
def run_dbt(tenant: str, command: str):
    project_path = os.path.join(DBT_BASE_PATH, tenant)

    if not os.path.exists(project_path):
        return {"error": "tenant project not found"}

    cmd = f"cd {project_path} && dbt {command}"

    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True
    )

    return {
        "command": cmd,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode
    }