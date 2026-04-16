# Local lakehouse infrastructure

Brings up a self-contained Trino + Hive Metastore + MinIO + MCP-Trino stack on
your laptop so the migration accelerator can run end-to-end without any
cloud dependency.

## Topology

```
                                           +-----------------------+
                                           |   migration-agent     |
                                           |  (sql-migrate run)    |
                                           +-----+-----------+-----+
                                                 |           |
                                       MCP HTTP  |           |  Trino HTTP
                                       :9097     |           |  :8080
                                                 v           v
+----------------+  thrift:9083   +-------------------+   +----------+
| hive-metastore +<---------------+ trino             +-->| MinIO    |
|  (Hive 4)      |                |  catalogs:        |   | (S3 API) |
|                |                |   - lz_lakehouse  |   |  :9000   |
+--------+-------+                |   - oracle_       |   +-----+----+
         |                        |     homeloans     |         |
         | JDBC                   +---------+---------+         |
         v                                  | s3a               |
+----------------+                          +-------------------+
| Postgres 15    |                              (shared MinIO)
| (metastore db) |
+----------------+
```

- **Reuses** the existing `minio` container already running on the
  `udemy-airflow_75ad9c_ndsnet` network (root creds: `minio` / `minio123`).
- New buckets created automatically: `lz-lakehouse`, `oracle-homeloans`.
- Hive Metastore is backed by its own dedicated Postgres (no contention with
  Airflow's Postgres).
- Both Trino catalogs use the **Iceberg** connector → supports `INSERT`,
  `UPDATE`, `DELETE`, `MERGE`, and `CTAS` cleanly, which is what the
  migration accelerator emits.

## One-time bring-up

```bash
cd /Users/lohithreddy/Desktop/Migration_accelerator_versions/migration-accelerator-agent-main_V2

docker compose -f infrastructure/docker-compose.yml up -d

# Wait for Trino to report healthy (~30s on first start)
docker compose -f infrastructure/docker-compose.yml ps

# Create schemas (target + source)
docker exec -i trino trino < infrastructure/init/01_create_schemas.sql

# Verify catalogs
docker exec -it trino trino --execute "SHOW CATALOGS"
# Expect: jmx, lz_lakehouse, memory, oracle_homeloans, system, tpcds, tpch
```

## Wiring into the migration accelerator

Add to `config/.env`:

```bash
TRINO_MCP_SERVER_URL=http://localhost:9097/sse
TRINO_ADMIN_URL=http://localhost:8080
TRINO_ADMIN_USER=admin
```

`config/config.yaml` already targets `lz_lakehouse` / `lm_target_schema` once
the `target_routing` block from `enhancement.md` is added.

## Tear down

```bash
docker compose -f infrastructure/docker-compose.yml down
# Add -v to also drop the metastore Postgres volume:
docker compose -f infrastructure/docker-compose.yml down -v
```

## Notes / risks

1. **Buckets persist in MinIO** — `down -v` only drops the metastore DB, not
   MinIO data. Drop manually if needed:
   `docker exec minio mc rb --force local/lz-lakehouse`.
2. **Hive 4 schema init** runs on first start of `hive-metastore`; subsequent
   restarts skip it. If you wipe `hive_metastore_db_data` you'll need to let
   it re-init (~20s).
3. **MCP-Trino transport** — the compose uses HTTP (`:9097/sse`). The
   accelerator's `config.yaml` has `mcp.transport: sse`; this matches.
4. **Trino S3 properties** are inline (`s3.aws-access-key=minio`) for local
   convenience. For any non-laptop deployment, switch to env-var
   substitution or use IAM-style providers.
