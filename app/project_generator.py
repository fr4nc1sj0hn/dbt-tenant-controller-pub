import pandas as pd
import psycopg2
import logging
from typing import Dict, Any
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


logger = logging.getLogger(__name__)


class DBTProjectGenerator:
    # -------------------------
    # Init
    # -------------------------
    def __init__(self, base_path: str, conn_params: Dict[str, Any]):
        self.base_path = Path(base_path)
        self.conn_params = conn_params
        self.base_path.mkdir(parents=True, exist_ok=True)

        self._metadata = self._load_metadata()
        self._metadata_columns = self._load_metadata_columns()

        # Normalization/validation also derives:
        # - self._dimensions (table_type == dimensions)
        # - self._fact_model_name (single-fact invariant)
        # - self._fact_raw_table (raw sourcetablename for the fact)
        # - self._fact_staging_model (stagingtablename for the fact)
        self._validate_and_prepare_metadata()

        templates_path = Path(__file__).parent / "templates"
        self.jinja_env = Environment(
            loader=FileSystemLoader(templates_path),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    # -------------------------
    # Validation
    # -------------------------
    def _validate_and_prepare_metadata(self) -> None:
        """Validate metadata contract and derive commonly-used subsets.

        This generator is intentionally strict:
        - it treats the metadata tables as a contract
        - it fails fast with actionable errors

        PoC-friendly compatibility:
        - if `table_type` is missing in `core.metadata`, infer it from names
          (modelname == 'fact_transactions' OR sourcetablename contains 'transactions')
        - if test-flag columns are missing in `core.metadata_columns`, default them
        """

        # ---- core.metadata ----
        required_metadata_cols = {"sourcetablename", "stagingtablename", "modelname"}
        missing = required_metadata_cols - set(self._metadata.columns)
        if missing:
            raise ValueError(f"core.metadata missing columns: {sorted(missing)}")

        # Normalize strings
        for col in ["sourcetablename", "stagingtablename", "modelname"]:
            self._metadata[col] = self._metadata[col].astype(str).str.strip().str.lower()

        if self._metadata[list(required_metadata_cols)].isnull().any().any():
            raise ValueError("core.metadata contains nulls in required table-name columns")

        # Optional: filter active rows if present
        if "active" in self._metadata.columns:
            self._metadata = self._metadata[self._metadata["active"].fillna(True) == True]

        # Ensure table_type exists (infer if needed)
        if "table_type" not in self._metadata.columns:
            logger.warning(
                "core.metadata.table_type not found; inferring using PoC heuristics"
            )
            inferred = []
            for _, r in self._metadata.iterrows():
                is_fact = (
                    r.get("modelname", "") == "fact_transactions"
                    or "transactions" in r.get("sourcetablename", "")
                    or "fact" in r.get("modelname", "")
                )
                inferred.append("fact" if is_fact else "dimensions")
            self._metadata["table_type"] = inferred
        else:
            self._metadata["table_type"] = (
                self._metadata["table_type"].astype(str).str.strip().str.lower()
            )

        allowed_table_types = {"dimensions", "fact"}
        invalid_types = set(self._metadata["table_type"].unique()) - allowed_table_types
        if invalid_types:
            raise ValueError(
                f"core.metadata.table_type contains invalid values: {sorted(invalid_types)}; "
                f"allowed={sorted(allowed_table_types)}"
            )

        self._dimensions = self._metadata[self._metadata["table_type"] == "dimensions"].copy()
        facts = self._metadata[self._metadata["table_type"] == "fact"].copy()

        if facts.empty:
            raise ValueError("No fact row found in core.metadata (table_type='fact')")
        if len(facts) != 1:
            raise ValueError(
                f"Expected exactly one fact row in core.metadata; found {len(facts)}"
            )

        fact_row = facts.iloc[0]
        self._fact_model_name = str(fact_row["modelname"]).lower()
        self._fact_staging_model = str(fact_row["stagingtablename"]).lower()
        self._fact_raw_table = str(fact_row["sourcetablename"]).lower()

        # ---- core.metadata_columns ----
        required_colmeta_cols = {"sourcetablename", "column_name"}
        missing_colmeta = required_colmeta_cols - set(self._metadata_columns.columns)
        if missing_colmeta:
            raise ValueError(
                f"core.metadata_columns missing columns: {sorted(missing_colmeta)}"
            )

        self._metadata_columns["sourcetablename"] = (
            self._metadata_columns["sourcetablename"].astype(str).str.strip().str.lower()
        )
        self._metadata_columns["column_name"] = (
            self._metadata_columns["column_name"].astype(str).str.strip().str.lower()
        )

        # Ensure table_type exists for column metadata (default: raw)
        if "table_type" not in self._metadata_columns.columns:
            logger.warning(
                "core.metadata_columns.table_type not found; defaulting all to 'raw'"
            )
            self._metadata_columns["table_type"] = "raw"
        else:
            self._metadata_columns["table_type"] = (
                self._metadata_columns["table_type"].astype(str).str.strip().str.lower()
            )

        # Provide safe defaults for test flags if absent
        for flag, default in [("is_primary_key", False), ("is_unique", False)]:
            if flag not in self._metadata_columns.columns:
                self._metadata_columns[flag] = default
        if "is_nullable" not in self._metadata_columns.columns:
            # Default to nullable=True to avoid emitting not_null tests unexpectedly
            self._metadata_columns["is_nullable"] = True

        # Basic sanity: dimension models should exist in column metadata (not strictly required)
        # but helps catch obvious mismatches.
        dim_names = set(self._dimensions["modelname"].unique())
        if dim_names:
            known_sources = set(self._metadata_columns["sourcetablename"].unique())
            missing_dim_colmeta = sorted([d for d in dim_names if d not in known_sources])
            if missing_dim_colmeta:
                logger.warning(
                    "No core.metadata_columns rows found for some dimension modelnames: %s",
                    missing_dim_colmeta,
                )

    # -------------------------
    # Templates
    # -------------------------
    def _render_template(self, template_name: str, context: dict) -> str:
        template = self.jinja_env.get_template(template_name)
        return template.render(**context)

    def _load_template_source(self, template_name: str) -> str:
        source, _, _ = self.jinja_env.loader.get_source(
            self.jinja_env, template_name
        )
        return source

    # -------------------------
    # Filesystem
    # -------------------------
    def _create_folders(self, project_path: Path):
        for folder in [
            "models/staging",
            "models/marts",
            "seeds",
            "snapshots",
            "macros",
            "analysis",
            "tests",
        ]:
            (project_path / folder).mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Database
    # -------------------------
    def _load_metadata(self) -> pd.DataFrame:
        query = "SELECT * FROM core.metadata"
        with psycopg2.connect(**self.conn_params) as conn:
            return pd.read_sql(query, conn)

    def _load_metadata_columns(self) -> pd.DataFrame:
        query = "SELECT * FROM core.metadata_columns"
        with psycopg2.connect(**self.conn_params) as conn:
            return pd.read_sql(query, conn)

    # -------------------------
    # Generators for Docs and Tests
    # -------------------------

    def _generate_tests(self, column: pd.Series) -> list:
        tests = []

        # Defensive reads (metadata contracts evolve in PoCs)
        if bool(column.get("is_primary_key", False)):
            tests += ["not_null", "unique"]
            return tests
        
        if bool(column.get("is_unique", False)):
            tests.append("unique")

        if not bool(column.get("is_nullable", True)):
            tests.append("not_null")

        return tests
    
    def _get_model_description(self, model_name: str) -> str:
        model_name = model_name.lower()
        row = self._metadata[self._metadata["modelname"] == model_name]
        if not row.empty:
            return row.iloc[0].get("description", "") or ""
        return ""
    def _generate_fact_schema(self, dims: list) -> list:
        fact = self._fact_model_name
        column_metadata = self._metadata_columns.copy()

        # In some metadata designs, fact columns might be registered under the mart model name.
        # In others, they might be registered under the raw/staging name.
        # Prefer mart model name; fall back to raw fact table.
        cols = column_metadata[column_metadata["sourcetablename"] == fact]
        if cols.empty:
            cols = column_metadata[column_metadata["sourcetablename"] == self._fact_raw_table]

        fact_columns = []
        for dim in dims:
            fact_columns.append({
                "name": f"{dim}_sk",
                "description": f"Foreign key to {dim} dimension",
                "tests": [
                    "not_null", 
                    {
                        "relationships": {
                            "to": f"ref('{dim}')", 
                            "field": f"{dim}_sk"
                        }
                    }
                ],
            })
        for _, col in cols.iterrows():
            col_name = str(col.get("column_name", ""))
            if not col_name.endswith("_sk"):
                fact_columns.append({
                    "name": col_name,
                    "description": (col.get("description") or ""),
                    "tests": self._generate_tests(col)
                })
        return fact_columns
    # -------------------------
    # Macros / Config
    # -------------------------
    def _generate_macros(self, project_path: Path):
        content = self._load_template_source(
            "generate_schema_name_macro.sql.j2"
        )
        macro_file = project_path / "macros" / "generate_schema_name.sql"
        macro_file.write_text(content, encoding="utf-8")

    def _generate_packages_file(self, project_path: Path):
        content = self._load_template_source("packages.yml.j2")
        (project_path / "packages.yml").write_text(content, encoding="utf-8")

    def _generate_project_file(self, project_path: Path, tenant_name: str):
        content = self._render_template(
            "dbt_project.yml.j2",
            {"tenant_name": tenant_name},
        )
        (project_path / "dbt_project.yml").write_text(content, encoding="utf-8")

    def _generate_profiles_file(self, project_path: Path, tenant_name: str):
        context = {
            "tenant_name": tenant_name,
            "adapter_type": "postgres",
            "host": self.conn_params.get("host", "localhost"),
            "port": int(self.conn_params.get("port", 5432)),
            "user": self.conn_params.get("user", "postgres"),
            "password": self.conn_params.get("password", ""),
            "dbname": self.conn_params.get("dbname", "postgres"),
            "schema": tenant_name,
            "threads": 4,
        }
        content = self._render_template("profiles.yml.j2", context)
        (project_path / "profiles.yml").write_text(content, encoding="utf-8")

    def _generate_schema(self, project_path: Path):
        models = []

        dims = (
            self._dimensions["modelname"]
            .str.lower()
            .dropna()
            .unique()
            .tolist()
        )
        column_metadata = self._metadata_columns.copy()

        for dim in dims:
            cols = column_metadata[column_metadata["sourcetablename"] == dim]

            columns = []
            for _ , col_row in cols.iterrows():
                logger.debug(
                    "Generating tests for model=%s column=%s tests=%s",
                    dim,
                    col_row.get("column_name"),
                    self._generate_tests(col_row),
                )
                columns.append({
                    "name": col_row["column_name"],
                    "description": col_row.get("description", "") or "",
                    "tests": self._generate_tests(col_row)
                })

            models.append(
                {
                    "name": dim,
                    "description": self._get_model_description(dim),
                    "columns": columns
                }
            )

        fact_columns = self._generate_fact_schema(dims)

        models.append({
            "name": self._fact_model_name,
            "description": self._get_model_description(self._fact_model_name) or "Fact model",
            "columns": fact_columns
        })
        
        content = self._render_template(
            "schema.yml.j2",
            {"models": models}
        )
        output_path = project_path / "models" / "marts" / "schema.yml"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        output_path.write_text(content, encoding="utf-8")


    # -------------------------
    # Models
    # -------------------------
    def _generate_sources_file(self, models_path: Path, tenant_name: str):
        tables = (
            self._metadata["sourcetablename"]
            .str.lower()
            .unique()
            .tolist()
        )
        content = self._render_template(
            "sources.yml.j2",
            {"tenant_name": tenant_name, "tables": tables},
        )
        (models_path / "sources.yml").write_text(content, encoding="utf-8")

    def _generate_staging_models(self, models_path: Path, tenant_name: str):
        # Only generate dimension staging models here.
        # Fact staging is generated separately from fact metadata.
        for _, row in self._dimensions.iterrows():
            raw_table = row["sourcetablename"].lower()
            staging_table = row["stagingtablename"].lower()

            cols = (
                self._metadata_columns[
                    (self._metadata_columns["sourcetablename"].str.lower() == raw_table)
                    & (self._metadata_columns["table_type"] == "raw")
                ]["column_name"].tolist()
            )

            content = self._render_template(
                "staging_models.yml.j2",
                {
                    "model_name": staging_table,
                    "source_table": raw_table,
                    "tenant_name": tenant_name,
                    "columns": cols,
                },
            )

            file_path = models_path / "staging" / f"{staging_table}.sql"
            file_path.write_text(content, encoding="utf-8")

        self._generate_staging_model_fact(models_path, tenant_name)

    def _generate_staging_model_fact(
        self, models_path: Path, tenant_name: str
    ):
        content = self._render_template(
            "staging_models_fact.yml.j2",
            {
                "tenant_name": tenant_name,
                "schema": f"raw_{tenant_name}",
                "model_name": self._fact_staging_model,
                "source_table": self._fact_raw_table,
            },
        )
        file_path = models_path / "staging" / f"{self._fact_staging_model}.sql"
        file_path.write_text(content, encoding="utf-8")

    def _generate_mart_models(self, models_path: Path):

        for _, row in self._dimensions.iterrows():
            model_name = row["modelname"].lower()
            content = self._render_template(
                "mart_models_dims.yml.j2",
                {
                    "model_name": model_name
                },
            )

            file_path = models_path / "marts" / f"{model_name}.sql"
            file_path.write_text(content, encoding="utf-8")

    def _generate_mart_model_fact(self, models_path: Path):
        dimension_tables = (
            self._dimensions["modelname"]
            .str.lower()
            .unique()
            .tolist()
        )

        content = self._render_template(
            "mart_models_fact.yml.j2",
            {
                "model_name": self._fact_model_name,
                "staging_model": self._fact_staging_model,
                "dimension_tables": dimension_tables,
            },
        )
        file_path = models_path / "marts" / f"{self._fact_model_name}.sql"
        file_path.write_text(content, encoding="utf-8")

    def _generate_snapshot_models(
        self, project_path: Path, tenant_name: str
    ):
        snapshot_path = project_path / "snapshots"

        for _, row in self._dimensions.iterrows():
            model_name = row["modelname"].lower()
            staging_table = row["stagingtablename"].lower()

            content = self._render_template(
                "snapshot_models.yml.j2",
                {
                    "model_name": model_name,
                    "staging_model": staging_table,
                    "tenant_name": tenant_name,
                },
            )

            file_path = snapshot_path / f"snapshot_{model_name}.yml"
            file_path.write_text(content, encoding="utf-8")

    # -------------------------
    # Public API
    # -------------------------
    def generate_dbt_project(self, tenant_name: str) -> Path:
        project_path = self.base_path / tenant_name
        models_path = project_path / "models"

        self._create_folders(project_path)

        self._generate_project_file(project_path, tenant_name)
        self._generate_profiles_file(project_path, tenant_name)
        self._generate_macros(project_path)
        self._generate_packages_file(project_path)

        self._generate_sources_file(models_path, tenant_name)
        #self._generate_marts_tests(project_path)
        self._generate_schema(project_path)
        self._generate_staging_models(models_path, tenant_name)
        self._generate_snapshot_models(project_path, tenant_name)
        self._generate_mart_models(models_path)
        self._generate_mart_model_fact(models_path)
        #self._generate_docs(project_path)

        logger.info(
            "DBT project for tenant '%s' generated at %s",
            tenant_name,
            project_path,
        )
        return project_path