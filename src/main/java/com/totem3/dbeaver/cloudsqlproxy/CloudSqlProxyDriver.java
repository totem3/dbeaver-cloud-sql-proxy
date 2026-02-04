package com.totem3.dbeaver.cloudsqlproxy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.UnsupportedEncodingException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class CloudSqlProxyDriver implements Driver {
    private static final Logger logger = Logger.getLogger(CloudSqlProxyDriver.class.getName());

    private static final String URL_PREFIX = "jdbc:cloudsqlproxy:";
    private static final String JDBC_POSTGRES_PREFIX = "jdbc:postgresql:";

    private static final String PROP_INSTANCE_CONNECTION_NAME = "instanceConnectionName";
    private static final String PROP_PROXY_BINARY = "cloudSqlProxyBinary";
    private static final String PROP_PROXY_ARGS = "cloudSqlProxyArgs";
    private static final String PROP_PROXY_PORT = "cloudSqlProxyPort";
    private static final String PROP_PROXY_HOST = "cloudSqlProxyHost";
    private static final String PROP_READY_TIMEOUT_MS = "cloudSqlProxyReadyTimeoutMs";
    private static final String PROP_DELEGATE_DRIVER = "delegateDriver";

    private static final int DEFAULT_READY_TIMEOUT_MS = 10000;
    private static final String DEFAULT_PROXY_BINARY = "cloud-sql-proxy";
    private static final String DEFAULT_PROXY_HOST = "127.0.0.1";
    private static final String DEFAULT_DELEGATE_DRIVER = "org.postgresql.Driver";

    private static final Set<String> CUSTOM_KEYS = new HashSet<>(Arrays.asList(
            PROP_INSTANCE_CONNECTION_NAME,
            PROP_PROXY_BINARY,
            PROP_PROXY_ARGS,
            PROP_PROXY_PORT,
            PROP_PROXY_HOST,
            PROP_READY_TIMEOUT_MS,
            PROP_DELEGATE_DRIVER
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

        Properties delegateProps = new Properties();
        if (info != null) {
            for (Map.Entry<Object, Object> entry : info.entrySet()) {
                delegateProps.put(entry.getKey(), entry.getValue());
            }
        }

        ParsedUrl parsed = parseUrl(url);

        String instanceConnectionName = extractConfigValue(PROP_INSTANCE_CONNECTION_NAME, delegateProps, parsed.queryParams);
        if (isBlank(instanceConnectionName)) {
            throw new SQLException("Missing required property: instanceConnectionName");
        }

        String proxyBinary = firstNonBlank(
                extractConfigValue(PROP_PROXY_BINARY, delegateProps, parsed.queryParams),
                System.getenv("CLOUD_SQL_PROXY_BINARY"),
                DEFAULT_PROXY_BINARY
        );

        String proxyArgs = extractConfigValue(PROP_PROXY_ARGS, delegateProps, parsed.queryParams);
        String proxyPortRaw = extractConfigValue(PROP_PROXY_PORT, delegateProps, parsed.queryParams);
        String proxyHost = extractConfigValue(PROP_PROXY_HOST, delegateProps, parsed.queryParams);

        String readyTimeoutRaw = extractConfigValue(PROP_READY_TIMEOUT_MS, delegateProps, parsed.queryParams);
        int readyTimeoutMs = parseIntOrDefault(readyTimeoutRaw, DEFAULT_READY_TIMEOUT_MS);

        String delegateDriver = firstNonBlank(
                extractConfigValue(PROP_DELEGATE_DRIVER, delegateProps, parsed.queryParams),
                DEFAULT_DELEGATE_DRIVER
        );

        int port = parsePort(proxyPortRaw);
        if (port <= 0) {
            port = allocatePort();
        }

        addQueryParamsToProperties(parsed.queryParams, delegateProps);

        String database = parsed.database;
        if (isBlank(database)) {
            database = firstNonBlank(
                    stringValue(delegateProps.getProperty("database")),
                    stringValue(delegateProps.getProperty("dbname")),
                    ""
            );
        }

        String connectHost = firstNonBlank(proxyHost, DEFAULT_PROXY_HOST);
        if ("0.0.0.0".equals(connectHost)) {
            connectHost = DEFAULT_PROXY_HOST;
        }
        String bindHost = isBlank(proxyHost) ? null : proxyHost;
        String delegateUrl = buildDelegateUrl(connectHost, port, database);

        ensureDriverLoaded(delegateDriver);

        ProxyProcess proxy = null;
        try {
            proxy = startProxy(proxyBinary, proxyArgs, bindHost, connectHost, port, instanceConnectionName, readyTimeoutMs);
            Connection delegate = DriverManager.getConnection(delegateUrl, delegateProps);
            return ProxyConnection.wrap(delegate, proxy);
        } catch (SQLException e) {
            if (proxy != null) {
                proxy.close();
            }
            throw e;
        } catch (RuntimeException e) {
            if (proxy != null) {
                proxy.close();
            }
            throw e;
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        List<DriverPropertyInfo> props = new ArrayList<>();
        props.add(propertyInfo(PROP_INSTANCE_CONNECTION_NAME, "Cloud SQL instance connection name (project:region:instance)", true));
        props.add(propertyInfo(PROP_PROXY_BINARY, "cloud-sql-proxy binary name or path", false));
        props.add(propertyInfo(PROP_PROXY_ARGS, "Extra arguments for cloud-sql-proxy (e.g. --credentials-file=...)", false));
        props.add(propertyInfo(PROP_PROXY_PORT, "Port for the local proxy listener (0 = auto)", false));
        props.add(propertyInfo(PROP_PROXY_HOST, "Local bind address for proxy (default 127.0.0.1)", false));
        props.add(propertyInfo(PROP_READY_TIMEOUT_MS, "Timeout waiting for proxy to be ready", false));
        props.add(propertyInfo(PROP_DELEGATE_DRIVER, "Delegate JDBC driver class (default org.postgresql.Driver)", false));
        return props.toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
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

    private static String buildDelegateUrl(String host, int port, String database) {
        String db = database == null ? "" : database;
        if (db.startsWith("/")) {
            db = db.substring(1);
        }
        StringBuilder sb = new StringBuilder();
        sb.append(JDBC_POSTGRES_PREFIX).append("//").append(host).append(":").append(port).append("/");
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

    private static int parsePort(String value) throws SQLException {
        if (isBlank(value)) {
            return 0;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("Invalid cloudSqlProxyPort: " + value, e);
        }
    }

    private static int parseIntOrDefault(String value, int defaultValue) throws SQLException {
        if (isBlank(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("Invalid integer value: " + value, e);
        }
    }

    private static int allocatePort() throws SQLException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new SQLException("Failed to allocate local port", e);
        }
    }

    private static ProxyProcess startProxy(String binary, String args, String bindHost, String connectHost, int port, String instanceName, int timeoutMs) throws SQLException {
        List<String> command = new ArrayList<>();
        command.add(binary);

        if (!isBlank(args)) {
            command.addAll(splitArgs(args));
        }

        if (!isBlank(bindHost)) {
            command.add("--address");
            command.add(bindHost);
        }

        command.add("--port");
        command.add(String.valueOf(port));
        command.add(instanceName);

        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);

        logger.info("Starting cloud-sql-proxy: " + maskCredentials(command));

        try {
            Process process = builder.start();
            Thread logThread = startLogThread(process.getInputStream());
            waitForReady(process, connectHost, port, timeoutMs);
            return new ProxyProcess(process, logThread);
        } catch (IOException e) {
            throw new SQLException("Failed to start cloud-sql-proxy", e);
        }
    }

    private static void waitForReady(Process process, String host, int port, int timeoutMs) throws SQLException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (!process.isAlive()) {
                int exitCode = process.exitValue();
                throw new SQLException("cloud-sql-proxy exited before becoming ready (exit code " + exitCode + ")");
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 200);
                return;
            } catch (IOException e) {
                // Not ready yet
            }
            sleep(100);
        }
        throw new SQLException("Timed out waiting for cloud-sql-proxy to become ready (" + timeoutMs + " ms)");
    }

    private static Thread startLogThread(InputStream stream) {
        Thread thread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    logger.info("[cloud-sql-proxy] " + line);
                }
            } catch (IOException e) {
                logger.log(Level.FINE, "Proxy log reader stopped", e);
            }
        }, "cloud-sql-proxy-log");
        thread.setDaemon(true);
        thread.start();
        return thread;
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

    private static List<String> splitArgs(String args) {
        if (isBlank(args)) {
            return Collections.emptyList();
        }
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = '\0';
        for (int i = 0; i < args.length(); i++) {
            char c = args.charAt(i);
            if (inQuotes) {
                if (c == quoteChar) {
                    inQuotes = false;
                } else {
                    current.append(c);
                }
                continue;
            }
            if (c == '\'' || c == '"') {
                inQuotes = true;
                quoteChar = c;
                continue;
            }
            if (Character.isWhitespace(c)) {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
                continue;
            }
            current.append(c);
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens;
    }

    private static String maskCredentials(List<String> command) {
        if (command == null || command.isEmpty()) {
            return "";
        }
        List<String> masked = new ArrayList<>();
        for (int i = 0; i < command.size(); i++) {
            String token = command.get(i);
            if (token.startsWith("--credentials-file")) {
                masked.add("--credentials-file=***");
                continue;
            }
            if ("--credentials-file".equals(token)) {
                masked.add(token);
                if (i + 1 < command.size()) {
                    masked.add("***");
                    i++;
                }
                continue;
            }
            masked.add(token);
        }
        return String.join(" ", masked);
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

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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

    private static final class ProxyProcess {
        private final Process process;
        private final Thread logThread;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private ProxyProcess(Process process, Thread logThread) {
            this.process = process;
            this.logThread = logThread;
        }

        private void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            if (process == null) {
                return;
            }
            process.destroy();
            try {
                if (!process.waitFor(3, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final class ProxyConnection implements InvocationHandler {
        private final Connection delegate;
        private final ProxyProcess proxyProcess;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private ProxyConnection(Connection delegate, ProxyProcess proxyProcess) {
            this.delegate = delegate;
            this.proxyProcess = proxyProcess;
        }

        private static Connection wrap(Connection delegate, ProxyProcess proxyProcess) {
            ProxyConnection handler = new ProxyConnection(delegate, proxyProcess);
            return (Connection) Proxy.newProxyInstance(
                    CloudSqlProxyDriver.class.getClassLoader(),
                    new Class[]{Connection.class},
                    handler
            );
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if ("close".equals(name)) {
                return handleClose();
            }
            if ("isWrapperFor".equals(name)) {
                Class<?> iface = (Class<?>) args[0];
                return iface.isInstance(proxy) || delegate.isWrapperFor(iface);
            }
            if ("unwrap".equals(name)) {
                Class<?> iface = (Class<?>) args[0];
                if (iface.isInstance(proxy)) {
                    return proxy;
                }
                return delegate.unwrap(iface);
            }
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }

        private Object handleClose() throws SQLException {
            if (!closed.compareAndSet(false, true)) {
                return null;
            }
            SQLException thrown = null;
            try {
                delegate.close();
            } catch (SQLException e) {
                thrown = e;
            } finally {
                proxyProcess.close();
            }
            if (thrown != null) {
                throw thrown;
            }
            return null;
        }
    }
}
