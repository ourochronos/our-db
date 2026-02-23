# ⚠️ DEPRECATED

This library has been vendored into [Valence](https://github.com/ourochronos/valence) as of v1.2.0.
This repo is archived for reference only. All future development happens in the Valence monorepo.

---

# our-db

Database connectivity, configuration, and migration brick. Part of the [ourochronos](https://github.com/ourochronos) ecosystem.

## Installation

```bash
pip install our-db

# With async support
pip install our-db[async]
```

## Usage

```python
from our_db import get_cursor, get_config

# Database access
with get_cursor() as cur:
    cur.execute("SELECT * FROM my_table")
    rows = cur.fetchall()

# Configuration
config = get_config()
print(config.db_host, config.db_port)
```

### Async

```python
from our_db import async_cursor

async with async_cursor() as conn:
    rows = await conn.fetch("SELECT * FROM my_table")
```

### Migrations

```python
from our_db import MigrationRunner

runner = MigrationRunner(migrations_dir="./migrations")
runner.up()        # Apply pending
runner.status()    # Check status
```

## Development

```bash
make dev    # Install with dev dependencies
make test   # Run tests
make lint   # Run linters
```
