# DBeaver Cloud SQL Proxy Driver (PostgreSQL)

A small JDBC driver wrapper that starts `cloud-sql-proxy` on a local port and connects PostgreSQL through it. This is useful when you want DBeaver to create the proxy tunnel automatically instead of using the socket factory approach.

## Build

```bash
./scripts/build.sh
```

This creates `build/dbeaver-cloud-sql-proxy-driver.jar`.

## DBeaver Setup

1. Open **Database > Driver Manager** and create a new driver.
2. Set **Driver Class** to `com.totem3.dbeaver.cloudsqlproxy.CloudSqlProxyDriver`.
3. Add driver files:
   - `build/dbeaver-cloud-sql-proxy-driver.jar`
   - PostgreSQL JDBC driver (download via DBeaver or add your own).
4. URL template example (see note below):

```
jdbc:cloudsqlproxy:postgresql:///<database>?instanceConnectionName=<project:region:instance>
```

You can also set `instanceConnectionName` in **Driver Properties** instead of the URL.

## Driver Properties

- `instanceConnectionName` (required)
  - Cloud SQL instance connection name: `project:region:instance`
- `cloudSqlProxyBinary` (recommended)
  - Absolute path to `cloud-sql-proxy` (GUI apps often do not inherit your shell PATH)
- `cloudSqlProxyArgs` (optional)
  - Extra arguments for the proxy, e.g. `--auto-iam-authn` (ADC is used by default)
- `cloudSqlProxyPort` (optional)
  - Local port for the proxy (0 or empty = auto)
- `cloudSqlProxyHost` (optional)
  - Bind address for the proxy (default `127.0.0.1`)
- `cloudSqlProxyReadyTimeoutMs` (optional)
  - Startup timeout in milliseconds (default `10000`)
- `delegateDriver` (optional)
  - JDBC driver class to delegate to (default `org.postgresql.Driver`)

## Authentication (ADC)

This driver relies on `cloud-sql-proxy` for authentication. If you do not pass `--credentials-file`, the proxy uses **Application Default Credentials (ADC)**.

Common ways to provide ADC:

- `gcloud auth application-default login`
- Set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON path

## Notes

- The proxy starts **per connection** and stops when the JDBC connection closes.
- DBeaver may open multiple connections for metadata and preview queries, so multiple proxy processes may appear.
- Authentication is handled by `cloud-sql-proxy` (ADC by default).
- In DBeaver, the safest setup is:
  - Set `instanceConnectionName` in **Driver Properties** (URL template variables may not be expanded in some modes).
  - Set `cloudSqlProxyBinary` to an **absolute path**, because DBeaver's PATH can be minimal.

## Example

Driver Properties:

- `instanceConnectionName=my-project:asia-northeast1:my-instance`
- `cloudSqlProxyArgs=--credentials-file=/Users/me/.config/gcloud/my-sa.json`

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
