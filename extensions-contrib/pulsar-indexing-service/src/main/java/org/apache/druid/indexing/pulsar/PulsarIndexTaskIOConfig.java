/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.pulsar.supervisor.PulsarSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

public class PulsarIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<Integer, String>
{
  private final Long pollTimeout;
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
  public PulsarIndexTaskIOConfig(
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId, // can be null for backward compabitility
      @JsonProperty("baseSequenceName") String baseSequenceName,
      // startPartitions and endPartitions exist to be able to read old ioConfigs in metadata store
      @JsonProperty("startPartitions") @Nullable
      @Deprecated SeekableStreamEndSequenceNumbers<Integer, String> startPartitions,
      @JsonProperty("endPartitions") @Nullable
      @Deprecated SeekableStreamEndSequenceNumbers<Integer, String> endPartitions,
      // startSequenceNumbers and endSequenceNumbers must be set for new versions
      @JsonProperty("startSequenceNumbers")
      @Nullable SeekableStreamStartSequenceNumbers<Integer, String> startSequenceNumbers,
      @JsonProperty("endSequenceNumbers")
      @Nullable SeekableStreamEndSequenceNumbers<Integer, String> endSequenceNumbers,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
      @JsonProperty("inputFormat") @Nullable InputFormat inputFormat,
      @JsonProperty("pollTimeout") Long pollTimeout,
      @JsonProperty("serviceUrl") String serviceUrl,
      @JsonProperty("authPluginClassName") String authPluginClassName,
      @JsonProperty("authParams") String authParams,
      @JsonProperty("operationTimeoutMs") Long operationTimeoutMs,
      @JsonProperty("statsIntervalSeconds") Long statsIntervalSeconds,
      @JsonProperty("numIoThreads") Integer numIoThreads,
      @JsonProperty("numListenerThreads") Integer numListenerThreads,
      @JsonProperty("useTcpNoDelay") Boolean useTcpNoDelay,
      @JsonProperty("useTls") Boolean useTls,
      @JsonProperty("tlsTrustCertsFilePath") String tlsTrustCertsFilePath,
      @JsonProperty("tlsAllowInsecureConnection") Boolean tlsAllowInsecureConnection,
      @JsonProperty("tlsHostnameVerificationEnable") Boolean tlsHostnameVerificationEnable,
      @JsonProperty("concurrentLookupRequest") Integer concurrentLookupRequest,
      @JsonProperty("maxLookupRequest") Integer maxLookupRequest,
      @JsonProperty("maxNumberOfRejectedRequestPerConnection") Integer maxNumberOfRejectedRequestPerConnection,
      @JsonProperty("keepAliveIntervalSeconds") Integer keepAliveIntervalSeconds,
      @JsonProperty("connectionTimeoutMs") Integer connectionTimeoutMs,
      @JsonProperty("requestTimeoutMs") Integer requestTimeoutMs,
      @JsonProperty("maxBackoffIntervalNanos") Long maxBackoffIntervalNanos
  )
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

    this.pollTimeout = pollTimeout != null ? pollTimeout : PulsarSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS;
    this.serviceUrl = Preconditions.checkNotNull(serviceUrl, "serviceUrl");
    this.authPluginClassName = authPluginClassName;
    this.authParams = authParams;
    this.operationTimeoutMs = operationTimeoutMs;
    this.statsIntervalSeconds = statsIntervalSeconds;
    this.numIoThreads = numIoThreads;
    this.numListenerThreads = numListenerThreads;
    this.useTcpNoDelay = useTcpNoDelay;
    this.useTls = useTls;
    this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
    this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
    this.tlsHostnameVerificationEnable = tlsHostnameVerificationEnable;
    this.concurrentLookupRequest = concurrentLookupRequest;
    this.maxLookupRequest = maxLookupRequest;
    this.maxNumberOfRejectedRequestPerConnection = maxNumberOfRejectedRequestPerConnection;
    this.keepAliveIntervalSeconds = keepAliveIntervalSeconds;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.requestTimeoutMs = requestTimeoutMs;
    this.maxBackoffIntervalNanos = maxBackoffIntervalNanos;

    final SeekableStreamEndSequenceNumbers<Integer, String> myEndSequenceNumbers = getEndSequenceNumbers();
    for (Integer partition : myEndSequenceNumbers.getPartitionSequenceNumberMap().keySet()) {
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
      Integer taskGroupId,
      String baseSequenceName,
      SeekableStreamStartSequenceNumbers<Integer, String> startSequenceNumbers,
      SeekableStreamEndSequenceNumbers<Integer, String> endSequenceNumbers,
      Boolean useTransaction,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      InputFormat inputFormat,
      Long pollTimeout,
      String serviceUrl,
      String authPluginClassName,
      String authParams,
      Long operationTimeoutMs,
      Long statsIntervalSeconds,
      Integer numIoThreads,
      Integer numListenerThreads,
      Boolean useTcpNoDelay,
      Boolean useTls,
      String tlsTrustCertsFilePath,
      Boolean tlsAllowInsecureConnection,
      Boolean tlsHostnameVerificationEnable,
      Integer concurrentLookupRequest,
      Integer maxLookupRequest,
      Integer maxNumberOfRejectedRequestPerConnection,
      Integer keepAliveIntervalSeconds,
      Integer connectionTimeoutMs,
      Integer requestTimeoutMs,
      Long maxBackoffIntervalNanos
  )
  {
    this(
        taskGroupId,
        baseSequenceName,
        null,
        null,
        startSequenceNumbers,
        endSequenceNumbers,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime,
        inputFormat,
        pollTimeout,
        serviceUrl,
        authPluginClassName,
        authParams,
        operationTimeoutMs,
        statsIntervalSeconds,
        numIoThreads,
        numListenerThreads,
        useTcpNoDelay,
        useTls,
        tlsTrustCertsFilePath,
        tlsAllowInsecureConnection,
        tlsHostnameVerificationEnable,
        concurrentLookupRequest,
        maxLookupRequest,
        maxNumberOfRejectedRequestPerConnection,
        keepAliveIntervalSeconds,
        connectionTimeoutMs,
        requestTimeoutMs,
        maxBackoffIntervalNanos
    );
  }

  /**
   * This method is for compatibilty so that newer version of PulsarIndexTaskIOConfig can be read by
   * old version of Druid. Note that this method returns end sequence numbers instead of start. This is because
   * {@link SeekableStreamStartSequenceNumbers} didn't exist before.
   */
  @JsonProperty
  @Deprecated
  public SeekableStreamEndSequenceNumbers<Integer, String> getStartPartitions()
  {
    // Converting to start sequence numbers. This is allowed for Pulsar because the start offset is always inclusive.
    final SeekableStreamStartSequenceNumbers<Integer, String> startSequenceNumbers = getStartSequenceNumbers();
    return new SeekableStreamEndSequenceNumbers<>(
        startSequenceNumbers.getStream(),
        startSequenceNumbers.getPartitionSequenceNumberMap()
    );
  }

  @JsonProperty
  public Long getPollTimeout()
  {
    return pollTimeout;
  }

  @JsonProperty
  public String getServiceUrl()
  {
    return serviceUrl;
  }

  @JsonProperty
  public String getAuthPluginClassName()
  {
    return authPluginClassName;
  }

  @JsonProperty
  public String getAuthParams()
  {
    return authParams;
  }

  @JsonProperty
  public Long getOperationTimeoutMs()
  {
    return operationTimeoutMs;
  }

  @JsonProperty
  public Long getStatsIntervalSeconds()
  {
    return statsIntervalSeconds;
  }

  @JsonProperty
  public Integer getNumIoThreads()
  {
    return numIoThreads;
  }

  @JsonProperty
  public Integer getNumListenerThreads()
  {
    return numListenerThreads;
  }

  @JsonProperty
  public Boolean isUseTcpNoDelay()
  {
    return useTcpNoDelay;
  }

  @JsonProperty
  public Boolean isUseTls()
  {
    return useTls;
  }

  @JsonProperty
  public String getTlsTrustCertsFilePath()
  {
    return tlsTrustCertsFilePath;
  }

  @JsonProperty
  public Boolean isTlsAllowInsecureConnection()
  {
    return tlsAllowInsecureConnection;
  }

  @JsonProperty
  public Boolean isTlsHostnameVerificationEnable()
  {
    return tlsHostnameVerificationEnable;
  }

  @JsonProperty
  public Integer getConcurrentLookupRequest()
  {
    return concurrentLookupRequest;
  }

  @JsonProperty
  public Integer getMaxLookupRequest()
  {
    return maxLookupRequest;
  }

  @JsonProperty
  public Integer getMaxNumberOfRejectedRequestPerConnection()
  {
    return maxNumberOfRejectedRequestPerConnection;
  }

  @JsonProperty
  public Integer getKeepAliveIntervalSeconds()
  {
    return keepAliveIntervalSeconds;
  }

  @JsonProperty
  public Integer getConnectionTimeoutMs()
  {
    return connectionTimeoutMs;
  }

  @JsonProperty
  public Integer getRequestTimeoutMs()
  {
    return requestTimeoutMs;
  }

  @JsonProperty
  public Long getMaxBackoffIntervalNanos()
  {
    return maxBackoffIntervalNanos;
  }

  @Override
  public String toString()
  {
    return "PulsarIndexTaskIOConfig{" +
           "taskGroupId=" + getTaskGroupId() +
           ", baseSequenceName='" + getBaseSequenceName() + '\'' +
           ", startSequenceNumbers=" + getStartSequenceNumbers() +
           ", endSequenceNumbers=" + getEndSequenceNumbers() +
           ", useTransaction=" + isUseTransaction() +
           ", minimumMessageTime=" + getMinimumMessageTime() +
           ", maximumMessageTime=" + getMaximumMessageTime() +
           ", pollTimeout=" + getPollTimeout() +
           ", serviceUrl=" + getServiceUrl() +
           ", authPluginClassName=" + getAuthPluginClassName() +
           ", authParams=" + getAuthParams() +
           ", operationTimeoutMs=" + getOperationTimeoutMs() +
           ", statsIntervalSeconds=" + getStatsIntervalSeconds() +
           ", numIoThreads=" + getNumIoThreads() +
           ", numListenerThreads=" + getNumListenerThreads() +
           ", useTcpNoDelay=" + isUseTcpNoDelay() +
           ", useTls=" + isUseTls() +
           ", tlsTrustCertsFilePath=" + getTlsTrustCertsFilePath() +
           ", tlsAllowInsecureConnection=" + isTlsAllowInsecureConnection() +
           ", tlsHostnameVerificationEnable=" + isTlsHostnameVerificationEnable() +
           ", concurrentLookupRequest=" + getConcurrentLookupRequest() +
           ", maxLookupRequest=" + getMaxLookupRequest() +
           ", maxNumberOfRejectedRequestPerConnection=" + getMaxNumberOfRejectedRequestPerConnection() +
           ", keepAliveIntervalSeconds=" + getKeepAliveIntervalSeconds() +
           ", connectionTimeoutMs=" + getConnectionTimeoutMs() +
           ", requestTimeoutMs=" + getRequestTimeoutMs() +
           ", maxBackoffIntervalNanos=" + getMaxBackoffIntervalNanos() +
           '}';
  }

}
