package org.apache.druid.indexing.pulsar;

public class ConsumerConfigDefaults {
    public static final String DEFAULT_AUTH_PLUGIN_CLASS_NAME = "";
    public static final String DEFAULT_AUTH_PARAMS = "";
    public static final String DEFAULT_OPERATION_TIMEOUT_MS = "100000";
    public static final String DEFAULT_STATS_INTERVAL_SECONDS = "100000";
    public static final Integer DEFAULT_NUM_IO_THREADS = 1;
    public static final Integer DEFAULT_NUM_LISTENER_THREADS = 1;
    public static final Boolean DEFAULT_USE_TCP_NO_DELAY = false;
    public static final Boolean DEFAULT_USE_TLS = false;
    public static final String DEFAULT_TLS_TRUST_CERTS_FILE_PATH = "";
    public static final Boolean DEFAULT_TLS_ALLOW_INSECURE_CONNECTION = false;
    public static final Boolean DEFAULT_TLS_HOSTNAME_VERIFICATION_ENABLE = false;
    public static final Integer DEFAULT_CONCURRENT_LOOKUP_REQUEST = 2;
    public static final Integer DEFAULT_MAX_LOOKUP_REQUEST = 1;
    public static final Integer DEFAULT_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION = 10;
    public static final Integer DEFAULT_KEEP_ALIVE_INTERVAL_SECONDS = 100;
    public static final Integer DEFAULT_CONNECTION_TIMEOUT_MS = 60000;
    public static final Integer DEFAULT_REQUEST_TIMEOUT_MS = 60000;
    public static final String DEFAULT_MAX_BACKOFF_INTERVAL_NANOS = "100000";
}
