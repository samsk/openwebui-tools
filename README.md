# openwebui-tools

Small utility collection for OpenWebUI operations and maintenance.

---

## openwebui_db_migrate.py

Migrate OpenWebUI data between databases using SQLAlchemy.

### What it does

- copies data from source DB (usually sqlite) to target DB
- resolves foreign-key-safe table order
- skips framework tables by default (`alembic_version`, `migratehistory`)
- validates `migratehistory` parity before migration
- supports dry-run planning
- supports best-effort mode for row-level failures
- supports optional target cleanup before import
- resets PostgreSQL sequences after import

### Requirements

- Python 3.10+
- `sqlalchemy`
- proper DB driver for target/source database:
  - PostgreSQL: `psycopg` or `psycopg2-binary`
  - SQLite: built-in

Example install:

```bash
python3 -m pip install -U sqlalchemy psycopg2-binary
```

### Usage

#### Basic migration (SQLite -> PostgreSQL)

```bash
./openwebui_db_migrate.py \
  --source-url "sqlite:///path/to/webui.db" \
  --target-url "postgresql://user:pass@localhost:5432/openwebui"
```

#### Dry run (no writes, show diff)

```bash
./openwebui_db_migrate.py \
  --source-url "sqlite:///path/to/webui.db" \
  --target-url "postgresql://user:pass@localhost:5432/openwebui" \
  --dry-run
  ```

  
