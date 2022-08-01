package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.pulsar.supervisor.PulsarSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.util.Map;

public class PulsarIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<Integer, Long>
{
  private final Map<String, Object> consumerProperties;
  private final long pollTimeout;
  private final String serviceUrl;
  private final String authPluginClassName;
  private final String authParams;
  private final Long operationTimeoutMs;
  private final Long statsIntervalSeconds;
  private final Integer numIoThreads;
  private final Integer numListenerThreads;
  private final Boolean useTcpNoDelay;
  private final Boolean useTls;
  private final String tlsTrustCertsFilePath;
  private final Boolean tlsAllowInsecureConnection;
  private final Boolean tlsHostnameVerificationEnable;
  private final Integer concurrentLookupRequest;
  private final Integer maxLookupRequest;
  private final Integer maxNumberOfRejectedRequestPerConnection;
  private final Integer keepAliveIntervalSeconds;
  private final Integer connectionTimeoutMs;
  private final Integer requestTimeoutMs;
  private final Long maxBackoffIntervalNanos;

  @JsonCreator
  public PulsarIndexTaskIOConfig(@JsonProperty("taskGroupId") @Nullable Integer taskGroupId,
                                 // can be null for backward compabitility
                                 @JsonProperty("baseSequenceName") String baseSequenceName,
                                 // startPartitions and endPartitions exist to be able to read old ioConfigs in metadata store
                                 @JsonProperty("startPartitions") @Nullable
                                 @Deprecated SeekableStreamEndSequenceNumbers<Integer, Long> startPartitions,
                                 @JsonProperty("endPartitions") @Nullable
                                 @Deprecated SeekableStreamEndSequenceNumbers<Integer, Long> endPartitions,
                                 // startSequenceNumbers and endSequenceNumbers must be set for new versions
                                 @JsonProperty("startSequenceNumbers")
                                 @Nullable SeekableStreamStartSequenceNumbers<Integer, Long> startSequenceNumbers,
                                 @JsonProperty("endSequenceNumbers")
                                 @Nullable SeekableStreamEndSequenceNumbers<Integer, Long> endSequenceNumbers,
                                 @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
                                 @JsonProperty("pollTimeout") Long pollTimeout,
                                 @JsonProperty("useTransaction") Boolean useTransaction,
                                 @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
                                 @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
                                 @JsonProperty("inputFormat") @Nullable InputFormat inputFormat)
  {
    super(
        taskGroupId,
        baseSequenceName,
        startSequenceNumbers == null
            ? Preconditions.checkNotNull(startPartitions, "startPartitions").asStartPartitions(true)
            : startSequenceNumbers,
        endSequenceNumbers == null ? endPartitions : endSequenceNumbers,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime,
        inputFormat
    );

    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    this.pollTimeout = pollTimeout != null ? pollTimeout : PulsarSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS;

    Preconditions.checkNotNull(
        consumerProperties.get(PulsarSupervisorIOConfig.SERVICE_URL_KEY),
        StringUtils.format("consumerProperties must contain entry for [%s]", PulsarSupervisorIOConfig.SERVICE_URL_KEY)
    );

    this.serviceUrl = (String) consumerProperties.getOrDefault(PulsarSupervisorIOConfig.SERVICE_URL_KEY, "");
    this.authPluginClassName = (String) consumerProperties.getOrDefault("authPluginClassName", ConsumerConfigDefaults.DEFAULT_AUTH_PLUGIN_CLASS_NAME);
    this.authParams = (String) consumerProperties.getOrDefault("authParams", ConsumerConfigDefaults.DEFAULT_AUTH_PARAMS);
    this.operationTimeoutMs = Long.parseLong((String) consumerProperties.getOrDefault("operationTimeoutMs", ConsumerConfigDefaults.DEFAULT_OPERATION_TIMEOUT_MS));
    this.statsIntervalSeconds = Long.parseLong((String) consumerProperties.getOrDefault("statsIntervalSeconds", ConsumerConfigDefaults.DEFAULT_STATS_INTERVAL_SECONDS));
    this.numIoThreads = (Integer) consumerProperties.getOrDefault("numIoThreads", ConsumerConfigDefaults.DEFAULT_NUM_IO_THREADS);
    this.numListenerThreads = (Integer) consumerProperties.getOrDefault("numListenerThreads", ConsumerConfigDefaults.DEFAULT_NUM_LISTENER_THREADS);
    this.useTcpNoDelay = (Boolean) consumerProperties.getOrDefault("useTcpNoDelay", ConsumerConfigDefaults.DEFAULT_USE_TCP_NO_DELAY);
    this.useTls = (Boolean) consumerProperties.getOrDefault("useTls", ConsumerConfigDefaults.DEFAULT_USE_TLS);
    this.tlsTrustCertsFilePath = (String) consumerProperties.getOrDefault("tlsTrustCertsFilePath", ConsumerConfigDefaults.DEFAULT_TLS_TRUST_CERTS_FILE_PATH);
    this.tlsAllowInsecureConnection = (Boolean) consumerProperties.getOrDefault("tlsAllowInsecureConnection", ConsumerConfigDefaults.DEFAULT_TLS_ALLOW_INSECURE_CONNECTION);
    this.tlsHostnameVerificationEnable = (Boolean) consumerProperties.getOrDefault("tlsHostnameVerificationEnable", ConsumerConfigDefaults.DEFAULT_TLS_HOSTNAME_VERIFICATION_ENABLE);
    this.concurrentLookupRequest = (Integer) consumerProperties.getOrDefault("concurrentLookupRequest", ConsumerConfigDefaults.DEFAULT_CONCURRENT_LOOKUP_REQUEST);
    this.maxLookupRequest = (Integer) consumerProperties.getOrDefault("maxLookupRequest", ConsumerConfigDefaults.DEFAULT_MAX_LOOKUP_REQUEST);
    this.maxNumberOfRejectedRequestPerConnection = (Integer) consumerProperties.getOrDefault("maxNumberOfRejectedRequestPerConnection", ConsumerConfigDefaults.DEFAULT_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION);
    this.connectionTimeoutMs = (Integer) consumerProperties.getOrDefault("connectionTimeoutMs", ConsumerConfigDefaults.DEFAULT_CONNECTION_TIMEOUT_MS);
    this.requestTimeoutMs = (Integer) consumerProperties.getOrDefault("requestTimeoutMs", ConsumerConfigDefaults.DEFAULT_REQUEST_TIMEOUT_MS);
    this.keepAliveIntervalSeconds = (Integer) consumerProperties.getOrDefault("keepAliveIntervalSeconds", ConsumerConfigDefaults.DEFAULT_KEEP_ALIVE_INTERVAL_SECONDS);
    this.maxBackoffIntervalNanos = Long.parseLong((String) consumerProperties.getOrDefault("maxBackoffIntervalNanos", ConsumerConfigDefaults.DEFAULT_MAX_BACKOFF_INTERVAL_NANOS));

    final SeekableStreamEndSequenceNumbers<Integer, Long> myEndSequenceNumbers = getEndSequenceNumbers();
    for (int partition : myEndSequenceNumbers.getPartitionSequenceNumberMap().keySet()) {
      Preconditions.checkArgument(
          myEndSequenceNumbers.getPartitionSequenceNumberMap()
              .get(partition)
              .compareTo(getStartSequenceNumbers().getPartitionSequenceNumberMap().get(partition)) >= 0,
          "end offset must be >= start offset for partition[%s]",
          partition
      );
    }
  }

  public PulsarIndexTaskIOConfig(
      int taskGroupId,
      String baseSequenceName,
      SeekableStreamStartSequenceNumbers<Integer, Long> startSequenceNumbers,
      SeekableStreamEndSequenceNumbers<Integer, Long> endSequenceNumbers,
      Map<String, Object> consumerProperties,
      Long pollTimeout,
      Boolean useTransaction,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      InputFormat inputFormat
  )
  {
    this(
        taskGroupId,
        baseSequenceName,
        null,
        null,
        startSequenceNumbers,
        endSequenceNumbers,
        consumerProperties,
        pollTimeout,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime,
        inputFormat
    );
  }

  /**
   * This method is for compatibilty so that newer version of PulsarIndexTaskIOConfig can be read by
   * old version of Druid. Note that this method returns end sequence numbers instead of start. This is because
   * {@link SeekableStreamStartSequenceNumbers} didn't exist before.
   */
  @JsonProperty
  @Deprecated
  public SeekableStreamEndSequenceNumbers<Integer, Long> getStartPartitions()
  {
    // Converting to start sequence numbers. This is allowed for Pulsar because the start offset is always inclusive.
    final SeekableStreamStartSequenceNumbers<Integer, Long> startSequenceNumbers = getStartSequenceNumbers();
    return new SeekableStreamEndSequenceNumbers<>(
        startSequenceNumbers.getStream(),
        startSequenceNumbers.getPartitionSequenceNumberMap()
    );
  }

  /**
   * This method is for compatibilty so that newer version of PulsarIndexTaskIOConfig can be read by
   * old version of Druid.
   */
  @JsonProperty
  @Deprecated
  public SeekableStreamEndSequenceNumbers<Integer, Long> getEndPartitions()
  {
    return getEndSequenceNumbers();
  }

  @JsonProperty
  public Map<String, Object> getConsumerProperties()
  {
    return consumerProperties;
  }

  @JsonProperty
  public long getPollTimeout()
  {
    return pollTimeout;
  }

  @Override
  public String toString()
  {
    return "PulsarIndexTaskIOConfig{" +
           "taskGroupId=" + getTaskGroupId() +
           ", baseSequenceName='" + getBaseSequenceName() + '\'' +
           ", startSequenceNumbers=" + getStartSequenceNumbers() +
           ", endSequenceNumbers=" + getEndSequenceNumbers() +
           ", consumerProperties=" + consumerProperties +
           ", pollTimeout=" + pollTimeout +
           ", useTransaction=" + isUseTransaction() +
           ", minimumMessageTime=" + getMinimumMessageTime() +
           ", maximumMessageTime=" + getMaximumMessageTime() +
           '}';
  }

  public String getServiceUrl()
  {
    return serviceUrl;
  }

  public String getAuthPluginClassName()
  {
    return authPluginClassName;
  }

  public String getAuthParams()
  {
    return authParams;
  }

  public Long getOperationTimeoutMs()
  {
    return operationTimeoutMs;
  }

  public Long getStatsIntervalSeconds()
  {
    return statsIntervalSeconds;
  }

  public Integer getNumIoThreads()
  {
    return numIoThreads;
  }

  public Integer getNumListenerThreads()
  {
    return numListenerThreads;
  }

  public Boolean getUseTcpNoDelay()
  {
    return useTcpNoDelay;
  }

  public Boolean getUseTls()
  {
    return useTls;
  }

  public String getTlsTrustCertsFilePath()
  {
    return tlsTrustCertsFilePath;
  }

  public Boolean getTlsAllowInsecureConnection()
  {
    return tlsAllowInsecureConnection;
  }

  public Boolean getTlsHostnameVerificationEnable()
  {
    return tlsHostnameVerificationEnable;
  }

  public Integer getConcurrentLookupRequest()
  {
    return concurrentLookupRequest;
  }

  public Integer getMaxLookupRequest()
  {
    return maxLookupRequest;
  }

  public Integer getMaxNumberOfRejectedRequestPerConnection()
  {
    return maxNumberOfRejectedRequestPerConnection;
  }

  public Integer getKeepAliveIntervalSeconds()
  {
    return keepAliveIntervalSeconds;
  }

  public Integer getConnectionTimeoutMs()
  {
    return connectionTimeoutMs;
  }

  public Integer getRequestTimeoutMs()
  {
    return requestTimeoutMs;
  }

  public Long getMaxBackoffIntervalNanos()
  {
    return maxBackoffIntervalNanos;
  }
}
