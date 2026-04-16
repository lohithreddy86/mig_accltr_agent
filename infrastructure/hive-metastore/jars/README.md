# Hive Metastore JARs (not committed to git)

These JARs are required by the Hive metastore container in `docker-compose.yml`
but are **excluded from git** because:

1. `aws-java-sdk-bundle-1.12.367.jar` is **296 MB** — exceeds GitHub's 100 MB
   single-file limit.
2. They are stable third-party binaries — anyone can fetch them deterministically
   from Maven Central.

## Download (run from this directory)

```bash
cd infrastructure/hive-metastore/jars

# AWS SDK bundle  (~296 MB)
curl -LO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar

# Hadoop AWS    (~1 MB)
curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar

# PostgreSQL JDBC  (~1 MB) — pick any 42.x release
curl -L  https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -o postgresql.jar
```

After downloading, you can `docker compose -f infrastructure/docker-compose.yml up -d`.
