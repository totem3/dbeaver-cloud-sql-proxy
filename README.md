# DBeaver Cloud SQL Proxy Driver (PostgreSQL)

A small JDBC driver wrapper that configures the Cloud SQL JDBC connector (`com.google.cloud.sql.postgres.SocketFactory`) and connects PostgreSQL without launching an external `cloud-sql-proxy` process.

## Build (Fat Jar)

```bash
./scripts/build.sh
```

This creates `target/dbeaver-cloud-sql-proxy-driver.jar` as a fat jar (driver + dependencies, including Cloud SQL Socket Factory).

## DBeaver Setup

1. Open **Database > Driver Manager** and create a new driver.
2. Set **Driver Class** to `com.totem3.dbeaver.cloudsqlproxy.CloudSqlProxyDriver`.
3. Add driver file:
   - `target/dbeaver-cloud-sql-proxy-driver.jar`
4. URL template example (see note below):

```
jdbc:cloudsqlproxy:postgresql:///<database>?instanceConnectionName=<project:region:instance>
```

You can also set `instanceConnectionName` in **Driver Properties** instead of the URL.

## Driver Properties

- `instanceConnectionName` (required)
  - Cloud SQL instance connection name: `project:region:instance`
- `cloudSqlIpTypes` (optional)
  - Connector IP preference, e.g. `PUBLIC,PRIVATE` or `PRIVATE`
- `cloudSqlEnableIamAuth` (optional)
  - Enable Cloud SQL IAM DB auth (`true` / `false`)
- `cloudSqlUnixSocketPath` (optional)
  - Unix socket base path if your runtime supports unix sockets
- `cloudSqlRefreshStrategy` (optional)
  - Connector refresh strategy (default `lazy`)
- `delegateDriver` (optional)
  - JDBC driver class to delegate to (default `org.postgresql.Driver`)

## Authentication (ADC)

This driver relies on the Cloud SQL JDBC connector for authentication and metadata refresh. By default it uses **Application Default Credentials (ADC)**.

Common ways to provide ADC:

- `gcloud auth application-default login`
- Set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON path

## Notes

- DBeaver may open multiple JDBC connections for metadata/preview queries.
- Set `instanceConnectionName` in **Driver Properties** (URL template variables may not be expanded in some modes).

## Example

Driver Properties:

- `instanceConnectionName=my-project:asia-northeast1:my-instance`
- `cloudSqlEnableIamAuth=true`
- `cloudSqlIpTypes=PRIVATE`

URL:

```
jdbc:cloudsqlproxy:postgresql:///<database>
```

## Release (GitHub Actions)

Push a tag to publish the jar as a GitHub Release asset:

```bash
git tag v0.1.0
git push origin v0.1.0
```

## License

MIT
