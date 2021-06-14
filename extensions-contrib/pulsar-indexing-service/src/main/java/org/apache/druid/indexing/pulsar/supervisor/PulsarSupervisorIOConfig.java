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

package org.apache.druid.indexing.pulsar.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.joda.time.DateTime;
import org.joda.time.Period;

import javax.annotation.Nullable;

public class PulsarSupervisorIOConfig extends SeekableStreamSupervisorIOConfig
{
  public static final long DEFAULT_POLL_TIMEOUT_MILLIS = 100;

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
  public PulsarSupervisorIOConfig(
      @JsonProperty("topic") String topic,
      @JsonProperty("inputFormat") InputFormat inputFormat,
      @JsonProperty("replicas") Integer replicas,
      @JsonProperty("taskCount") Integer taskCount,
      @JsonProperty("taskDuration") Period taskDuration,
      @Nullable @JsonProperty("autoScalerConfig") AutoScalerConfig autoScalerConfig,
      @JsonProperty("startDelay") Period startDelay,
      @JsonProperty("period") Period period,
      @JsonProperty("useEarliestMessageId") Boolean useEarliestMessageId,
      @JsonProperty("completionTimeout") Period completionTimeout,
      @JsonProperty("lateMessageRejectionPeriod") Period lateMessageRejectionPeriod,
      @JsonProperty("earlyMessageRejectionPeriod") Period earlyMessageRejectionPeriod,
      @JsonProperty("lateMessageRejectionStartDateTime") DateTime lateMessageRejectionStartDateTime,
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
        Preconditions.checkNotNull(topic, "topic"),
        inputFormat,
        replicas,
        taskCount,
        taskDuration,
        startDelay,
        period,
        useEarliestMessageId,
        completionTimeout,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        autoScalerConfig,
        lateMessageRejectionStartDateTime
    );

    this.pollTimeout = pollTimeout != null ? pollTimeout : DEFAULT_POLL_TIMEOUT_MILLIS;
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
  }

  @JsonProperty
  public String getTopic()
  {
    return getStream();
  }

  @JsonProperty
  public long getPollTimeout()
  {
    return pollTimeout;
  }

  @JsonProperty
  public boolean isUseEarliestMessageId()
  {
    return isUseEarliestSequenceNumber();
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
    return "PulsarSupervisorIOConfig{" +
           "topic='" + getTopic() + '\'' +
           ", replicas=" + getReplicas() +
           ", taskCount=" + getTaskCount() +
           ", taskDuration=" + getTaskDuration() +
           ", autoScalerConfig=" + getAutoscalerConfig() +
           ", startDelay=" + getStartDelay() +
           ", period=" + getPeriod() +
           ", useEarliestMessageId=" + isUseEarliestMessageId() +
           ", completionTimeout=" + getCompletionTimeout() +
           ", earlyMessageRejectionPeriod=" + getEarlyMessageRejectionPeriod() +
           ", lateMessageRejectionPeriod=" + getLateMessageRejectionPeriod() +
           ", lateMessageRejectionStartDateTime=" + getLateMessageRejectionStartDateTime() +
           ", pollTimeout=" + pollTimeout +
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
