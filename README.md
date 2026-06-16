# agservice-storage

A FastAPI service that wraps an S3-compatible object store, enforces
per-service (domain) access control, and exposes abstract presigned URLs so
clients never deal with raw S3 keys or credentials.

## Concepts

### Domain isolation

Every object belongs to a **domain** (e.g. `agcore`, `agvideo`, `agimage`).
Write access is granted only when the authenticated client ID matches the
domain name; read access is open to any authenticated caller with a valid
domain reference.

### Static objects

Path is deterministic and computed by the service:

```
static/env=<PRODUCT_ENV>/project_id=<pid>/user_id=<uid>/domain=<domain>/<relative_key>
```

### Dynamic object groups

A group is created first (`POST /dynamic_object/new_group`), which mints a
UUID-based prefix and writes a `manifest.json` to S3 directly. Subsequent
upload/download calls reference the group ID plus a relative key within it.

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/static_object/upload` | Presigned PUT URL for a static object |
| POST | `/static_object/download` | Presigned GET URL for a static object |
| POST | `/dynamic_object/new_group` | Create a dynamic object group |
| POST | `/dynamic_object/upload` | Presigned PUT URL within a group |
| POST | `/dynamic_object/download` | Presigned GET URL within a group |
| POST | `/webhook/minio` | Receive MinIO event notifications |

All endpoints (except the webhook) require a valid bearer token issued by
Keycloak.

## External dependencies

| System | Purpose |
|--------|---------|
| S3-compatible store (MinIO/AWS) | Actual object storage backend |
| PostgreSQL | Persists storage metadata and dynamic object group records |
| Keycloak | JWT-based authentication and client identification |

## Configuration

All configuration is via environment variables:

| Variable | Description |
|----------|-------------|
| `PRODUCT_ENV` | Deployment environment label (`dev`, `prod`, ...) |
| `S3_ENDPOINT_URL` | S3 endpoint (e.g. `http://minio:9000`) |
| `S3_ACCESS_KEY` | S3 access key |
| `S3_SECRET_KEY` | S3 secret key |
| `S3_BUCKET_NAME` | Default bucket name |
| `S3_REGION` | S3 region (default `us-east-1`) |
| `KEYCLOAK_URL` | Keycloak base URL |
| `KEYCLOAK_REALM` | Keycloak realm name |
| `SQL_TYPE` | SQLAlchemy driver prefix (e.g. `postgresql`) |
| `SQL_HOST` | Database host |
| `SQL_PORT` | Database port |
| `SQL_USER` | Database user |
| `SQL_PASSWORD` | Database password |
| `SQL_DB` | Database name |

## Running locally

```sh
docker compose up
```

The service listens on port `10101` (mapped from container port `8000`).

Interactive API docs are available at `http://localhost:10101/docs`.

## Tech stack

- Python 3.10+
- FastAPI / Uvicorn
- SQLModel + psycopg2 (PostgreSQL)
- boto3 (S3)
- agpyutils (shared auth and storage type definitions)
