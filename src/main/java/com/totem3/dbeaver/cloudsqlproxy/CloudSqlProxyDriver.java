package com.totem3.dbeaver.cloudsqlproxy;

import com.google.cloud.sql.ConnectorRegistry;
import java.io.UnsupportedEncodingException;
import java.io.File;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

public final class CloudSqlProxyDriver implements Driver {
    private static final Logger logger = Logger.getLogger(CloudSqlProxyDriver.class.getName());

    private static final String URL_PREFIX = "jdbc:cloudsqlproxy:";
    private static final String JDBC_POSTGRES_PREFIX = "jdbc:postgresql:";

    private static final String PROP_INSTANCE_CONNECTION_NAME = "instanceConnectionName";
    private static final String PROP_IP_TYPES = "cloudSqlIpTypes";
    private static final String PROP_ENABLE_IAM_AUTH = "cloudSqlEnableIamAuth";
    private static final String PROP_UNIX_SOCKET_PATH = "cloudSqlUnixSocketPath";
    private static final String PROP_REFRESH_STRATEGY = "cloudSqlRefreshStrategy";
    private static final String PROP_GOOGLE_CREDENTIALS_PATH = "cloudSqlGoogleCredentialsPath";
    private static final String PROP_DELEGATE_DRIVER = "delegateDriver";

    private static final String ENV_GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";
    private static final String ENV_CLOUDSDK_CONFIG = "CLOUDSDK_CONFIG";
    private static final String ENV_HOME = "HOME";
    private static final String ENV_APPDATA = "APPDATA";
    private static final String ENV_LOCALAPPDATA = "LOCALAPPDATA";
    private static final String ADC_FILE_NAME = "application_default_credentials.json";

    // Legacy properties from process-based implementation. These are ignored.
    private static final String PROP_PROXY_BINARY = "cloudSqlProxyBinary";
    private static final String PROP_PROXY_ARGS = "cloudSqlProxyArgs";
    private static final String PROP_PROXY_PORT = "cloudSqlProxyPort";
    private static final String PROP_PROXY_HOST = "cloudSqlProxyHost";
    private static final String PROP_READY_TIMEOUT_MS = "cloudSqlProxyReadyTimeoutMs";

    private static final String DEFAULT_DELEGATE_DRIVER = "org.postgresql.Driver";
    private static final String DEFAULT_SOCKET_FACTORY = "com.google.cloud.sql.postgres.SocketFactory";
    private static final String DEFAULT_SOCKET_FACTORY_HOST = "google";
    private static final String DEFAULT_REFRESH_STRATEGY = "lazy";

    private static final Set<String> CUSTOM_KEYS = new HashSet<>(Arrays.asList(
            PROP_INSTANCE_CONNECTION_NAME,
            PROP_IP_TYPES,
            PROP_ENABLE_IAM_AUTH,
            PROP_UNIX_SOCKET_PATH,
            PROP_REFRESH_STRATEGY,
            PROP_GOOGLE_CREDENTIALS_PATH,
            PROP_DELEGATE_DRIVER,
            PROP_PROXY_BINARY,
            PROP_PROXY_ARGS,
            PROP_PROXY_PORT,
            PROP_PROXY_HOST,
            PROP_READY_TIMEOUT_MS
    ));

    static {
        try {
            DriverManager.registerDriver(new CloudSqlProxyDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        ParsedUrl parsed = parseUrl(url);
        try {
            return connectOnce(parsed, info);
        } catch (SQLException firstFailure) {
            if (!shouldRetryWithFreshCredentials(firstFailure)) {
                throw firstFailure;
            }

            logger.warning("Authentication-related Cloud SQL connection failure detected; resetting connector registry and retrying once: "
                    + summarizeThrowable(firstFailure));
            ConnectorRegistry.reset();

            try {
                return connectOnce(parsed, info);
            } catch (SQLException retryFailure) {
                retryFailure.addSuppressed(firstFailure);
                throw retryFailure;
            }
        }
    }

    private Connection connectOnce(ParsedUrl parsed, Properties info) throws SQLException {
        Properties delegateProps = new Properties();
        if (info != null) {
            for (Map.Entry<Object, Object> entry : info.entrySet()) {
                delegateProps.put(entry.getKey(), entry.getValue());
            }
        }

        String instanceConnectionName = extractConfigValue(PROP_INSTANCE_CONNECTION_NAME, delegateProps, parsed.queryParams);
        if (isBlank(instanceConnectionName)) {
            throw new SQLException("Missing required property: instanceConnectionName");
        }

        String delegateDriver = firstNonBlank(
                extractConfigValue(PROP_DELEGATE_DRIVER, delegateProps, parsed.queryParams),
                DEFAULT_DELEGATE_DRIVER
        );

        String ipTypes = extractConfigValue(PROP_IP_TYPES, delegateProps, parsed.queryParams);
        String enableIamAuth = extractConfigValue(PROP_ENABLE_IAM_AUTH, delegateProps, parsed.queryParams);
        String unixSocketPath = extractConfigValue(PROP_UNIX_SOCKET_PATH, delegateProps, parsed.queryParams);
        String refreshStrategy = firstNonBlank(
                extractConfigValue(PROP_REFRESH_STRATEGY, delegateProps, parsed.queryParams),
                DEFAULT_REFRESH_STRATEGY
        );
        String googleCredentialsPath = extractConfigValue(PROP_GOOGLE_CREDENTIALS_PATH, delegateProps, parsed.queryParams);
        if (isBlank(googleCredentialsPath)) {
            googleCredentialsPath = resolveDefaultAdcCredentialsPath();
        }

        // Drop legacy process-based keys from delegate props if they were supplied.
        extractConfigValue(PROP_PROXY_BINARY, delegateProps, parsed.queryParams);
        extractConfigValue(PROP_PROXY_ARGS, delegateProps, parsed.queryParams);
        extractConfigValue(PROP_PROXY_PORT, delegateProps, parsed.queryParams);
        extractConfigValue(PROP_PROXY_HOST, delegateProps, parsed.queryParams);
        extractConfigValue(PROP_READY_TIMEOUT_MS, delegateProps, parsed.queryParams);

        addQueryParamsToProperties(parsed.queryParams, delegateProps);

        String database = parsed.database;
        if (isBlank(database)) {
            database = firstNonBlank(
                    stringValue(delegateProps.getProperty("database")),
                    stringValue(delegateProps.getProperty("dbname")),
                    ""
            );
        }

        ensureDriverLoaded(delegateDriver);

        delegateProps.setProperty("socketFactory", DEFAULT_SOCKET_FACTORY);
        delegateProps.setProperty("cloudSqlInstance", instanceConnectionName);
        if (!isBlank(ipTypes)) {
            delegateProps.setProperty("ipTypes", ipTypes);
        }
        if (!isBlank(enableIamAuth)) {
            delegateProps.setProperty("enableIamAuth", enableIamAuth);
        }
        if (!isBlank(googleCredentialsPath)) {
            delegateProps.setProperty(PROP_GOOGLE_CREDENTIALS_PATH, googleCredentialsPath);
        }
        if (!isBlank(unixSocketPath)) {
            delegateProps.setProperty("unixSocketPath", unixSocketPath);
        }
        if (!isBlank(refreshStrategy)) {
            delegateProps.setProperty("cloudSqlRefreshStrategy", refreshStrategy);
        }

        String delegateUrl = buildDelegateUrl(DEFAULT_SOCKET_FACTORY_HOST, database);
        return DriverManager.getConnection(delegateUrl, delegateProps);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        List<DriverPropertyInfo> props = new ArrayList<>();
        props.add(propertyInfo(PROP_INSTANCE_CONNECTION_NAME, "Cloud SQL instance connection name (project:region:instance)", true));
        props.add(propertyInfo(PROP_IP_TYPES, "IP preference for Cloud SQL connector (e.g. PUBLIC,PRIVATE or PRIVATE)", false));
        props.add(propertyInfo(PROP_ENABLE_IAM_AUTH, "Enable IAM DB auth (true/false)", false));
        props.add(propertyInfo(PROP_UNIX_SOCKET_PATH, "Optional unix socket path for environments with unix socket support", false));
        props.add(propertyInfo(PROP_REFRESH_STRATEGY, "Cloud SQL connector refresh strategy (default lazy)", false));
        props.add(propertyInfo(PROP_GOOGLE_CREDENTIALS_PATH, "Cloud SQL connector credential file path (defaults to ADC resolution)", false));
        props.add(propertyInfo(PROP_DELEGATE_DRIVER, "Delegate JDBC driver class (default org.postgresql.Driver)", false));
        return props.toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return logger;
    }

    private static DriverPropertyInfo propertyInfo(String name, String description, boolean required) {
        DriverPropertyInfo info = new DriverPropertyInfo(name, null);
        info.description = description;
        info.required = required;
        return info;
    }

    private static String buildDelegateUrl(String host, String database) {
        String db = database == null ? "" : database;
        if (db.startsWith("/")) {
            db = db.substring(1);
        }
        StringBuilder sb = new StringBuilder();
        sb.append(JDBC_POSTGRES_PREFIX).append("//").append(host).append("/");
        sb.append(db);
        return sb.toString();
    }

    private static ParsedUrl parseUrl(String url) throws SQLException {
        if (!acceptsJdbcPrefix(url)) {
            throw new SQLException("Unsupported URL: " + url);
        }
        String delegate = "jdbc:" + url.substring(URL_PREFIX.length());
        if (!delegate.startsWith(JDBC_POSTGRES_PREFIX)) {
            throw new SQLException("Only PostgreSQL is supported. Expected URL starting with jdbc:cloudsqlproxy:postgresql:");
        }

        String remainder = delegate.substring(JDBC_POSTGRES_PREFIX.length());
        String query = null;
        int queryIdx = remainder.indexOf('?');
        if (queryIdx >= 0) {
            query = remainder.substring(queryIdx + 1);
            remainder = remainder.substring(0, queryIdx);
        }

        String database = extractDatabase(remainder);
        Map<String, List<String>> params = parseQuery(query);
        return new ParsedUrl(database, params);
    }

    private static boolean acceptsJdbcPrefix(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    private static String extractDatabase(String remainder) {
        if (remainder == null || remainder.isEmpty()) {
            return "";
        }
        if (remainder.startsWith("//")) {
            int slashIdx = remainder.indexOf('/', 2);
            if (slashIdx < 0) {
                return "";
            }
            return remainder.substring(slashIdx + 1);
        }
        if (remainder.startsWith("/")) {
            return remainder.substring(1);
        }
        return remainder;
    }

    private static Map<String, List<String>> parseQuery(String query) {
        if (query == null || query.isEmpty()) {
            return new LinkedHashMap<>();
        }
        Map<String, List<String>> params = new LinkedHashMap<>();
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            if (pair.isEmpty()) {
                continue;
            }
            String[] kv = pair.split("=", 2);
            String key = urlDecode(kv[0]);
            String value = kv.length > 1 ? urlDecode(kv[1]) : "";
            params.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        }
        return params;
    }

    private static void addQueryParamsToProperties(Map<String, List<String>> params, Properties props) {
        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            String key = entry.getKey();
            if (CUSTOM_KEYS.contains(key)) {
                continue;
            }
            if (props.containsKey(key)) {
                continue;
            }
            List<String> values = entry.getValue();
            if (values != null && !values.isEmpty()) {
                props.setProperty(key, values.get(0));
            }
        }
    }

    private static String extractConfigValue(String key, Properties props, Map<String, List<String>> params) {
        String value = null;
        if (props != null && props.containsKey(key)) {
            Object v = props.get(key);
            value = v == null ? null : v.toString();
            props.remove(key);
        }
        if (isBlank(value) && params != null && params.containsKey(key)) {
            List<String> values = params.get(key);
            if (values != null && !values.isEmpty()) {
                value = values.get(0);
            }
        }
        return value;
    }

    private static String resolveDefaultAdcCredentialsPath() {
        String explicit = getEnvironmentValue(ENV_GOOGLE_APPLICATION_CREDENTIALS);
        if (!isBlank(explicit) && isExistingFile(explicit)) {
            return explicit;
        }
        if (!isBlank(explicit)) {
            return null;
        }

        String cloudSdkConfig = getEnvironmentValue(ENV_CLOUDSDK_CONFIG);
        String byCloudSdk = resolveAdcPathFromConfigDir(cloudSdkConfig);
        if (!isBlank(byCloudSdk) && isExistingFile(byCloudSdk)) {
            return byCloudSdk;
        }

        String home = getEnvironmentValue(ENV_HOME);
        if (!isBlank(home)) {
            String byHome = resolveAdcPathFromConfigDir(buildPath(home, ".config", "gcloud"));
            if (!isBlank(byHome) && isExistingFile(byHome)) {
                return byHome;
            }
        }

        String appData = firstNonBlank(
                getEnvironmentValue(ENV_APPDATA),
                getEnvironmentValue(ENV_LOCALAPPDATA)
        );
        if (!isBlank(appData)) {
            String byAppData = resolveAdcPathFromConfigDir(buildPath(appData, "gcloud"));
            if (!isBlank(byAppData) && isExistingFile(byAppData)) {
                return byAppData;
            }
        }
        return null;
    }

    private static String buildPath(String baseDir, String... segments) {
        File path = new File(baseDir);
        for (String segment : segments) {
            path = new File(path, segment);
        }
        return path.getPath();
    }

    private static String resolveAdcPathFromConfigDir(String baseDir) {
        if (isBlank(baseDir)) {
            return null;
        }
        return new File(baseDir, ADC_FILE_NAME).getPath();
    }

    private static boolean isExistingFile(String path) {
        return new File(path).isFile();
    }

    private static String getEnvironmentValue(String key) {
        String value = System.getenv(key);
        return stringValue(value);
    }

    private static boolean shouldRetryWithFreshCredentials(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (isAuthenticationFailure(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean isAuthenticationFailure(Throwable throwable) {
        String className = throwable.getClass().getName().toLowerCase(Locale.ROOT);
        if (className.contains("credential") || className.contains("auth")) {
            return true;
        }

        String message = throwable.getMessage();
        if (isBlank(message)) {
            return false;
        }

        String normalized = message.toLowerCase(Locale.ROOT);
        return normalized.contains("application default credentials")
                || normalized.contains("unable to obtain credentials")
                || normalized.contains("unable to load googlecredentials")
                || normalized.contains("credential")
                || normalized.contains("access token")
                || normalized.contains("refresh token")
                || normalized.contains("invalid_grant")
                || normalized.contains("reauth")
                || normalized.contains("oauth")
                || normalized.contains("unauthenticated")
                || normalized.contains("authentication");
    }

    private static String summarizeThrowable(Throwable throwable) {
        String message = throwable.getMessage();
        if (!isBlank(message)) {
            return message;
        }
        return throwable.getClass().getName();
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (!isBlank(value)) {
                return value;
            }
        }
        return null;
    }

    private static String stringValue(String value) {
        return isBlank(value) ? null : value;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static void ensureDriverLoaded(String driverClass) throws SQLException {
        if (isBlank(driverClass)) {
            throw new SQLException("Delegate driver class is empty");
        }
        try {
            Class.forName(driverClass, true, CloudSqlProxyDriver.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new SQLException("Delegate driver not found: " + driverClass, e);
        }
    }

    private static String urlDecode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unused")
    private static String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final class ParsedUrl {
        private final String database;
        private final Map<String, List<String>> queryParams;

        private ParsedUrl(String database, Map<String, List<String>> queryParams) {
            this.database = database;
            this.queryParams = queryParams == null ? new HashMap<>() : queryParams;
        }
    }
}
