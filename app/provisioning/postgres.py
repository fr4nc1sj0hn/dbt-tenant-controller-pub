from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.sql import quoted_name
from provisioning.base import SchemaProvisioner

class PostgresSchemaProvisioner(SchemaProvisioner):
    DEFAULT_ROLE = "dbt_user"
    DEFAULT_SCHEMAS = ["raw", "staging", "marts", "snapshots"]

    def __init__(self, session: Session, role: str = "dbt_user"):
        self.session = session
        self.role = role

    def _build_schema_names(self, tenant: str) -> list[str]:
        return [f"{prefix}_{tenant}" for prefix in self.DEFAULT_SCHEMAS]

    def _create_schema(self, schema_name: str) -> None:
        safe_schema = quoted_name(schema_name, quote=True)

        self.session.execute(
            text(f"CREATE SCHEMA IF NOT EXISTS {safe_schema};")
        )

    def _grant_schema_permissions(self, schema_name: str) -> None:
        safe_schema = quoted_name(schema_name, quote=True)

        self.session.execute(
            text(f"""
            GRANT USAGE ON SCHEMA {safe_schema} TO {self.role};
            GRANT CREATE ON SCHEMA {safe_schema} TO {self.role};
            """)
        )

        self.session.execute(
            text(f"""
            ALTER DEFAULT PRIVILEGES IN SCHEMA {safe_schema}
            GRANT SELECT, INSERT, UPDATE, DELETE
            ON TABLES TO {self.role};
            """)
        )

        self.session.execute(
            text(f"""
            ALTER DEFAULT PRIVILEGES IN SCHEMA {safe_schema}
            GRANT USAGE, SELECT
            ON SEQUENCES TO {self.role};
            """)
        )

    def provision_tenant(self, tenant: str) -> None:
        """Create new schema on the database for the tenant."""
        schemas = self._build_schema_names(tenant)

        for schema in schemas:
            self._create_schema(schema)
            self._grant_schema_permissions(schema)