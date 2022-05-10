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
 *
 *
 */

package org.apache.druid.indexing.pulsar.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;

import javax.annotation.Nullable;

import java.util.Map;

public class PulsarSupervisorIOConfig extends SeekableStreamSupervisorIOConfig
{
  public static final long DEFAULT_POLL_TIMEOUT_MILLIS = 100;
  public static final String SERVICE_URL_KEY = "serviceUrl";
  private final long pollTimeout;
  private final Map<String, Object> consumerProperties;
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

  public PulsarSupervisorIOConfig(@JsonProperty("topic") String topic,
                                  @JsonProperty("inputFormat") InputFormat inputFormat,
                                  @JsonProperty("replicas") Integer replicas,
                                  @JsonProperty("taskCount") Integer taskCount,
                                  @JsonProperty("taskDuration") Period taskDuration,
                                  @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
                                  @Nullable @JsonProperty("autoScalerConfig") AutoScalerConfig autoScalerConfig,
                                  @JsonProperty("pollTimeout") Long pollTimeout,
                                  @JsonProperty("startDelay") Period startDelay,
                                  @JsonProperty("period") Period period,
                                  @JsonProperty("useEarliestOffset") Boolean useEarliestOffset,
                                  @JsonProperty("completionTimeout") Period completionTimeout,
                                  @JsonProperty("lateMessageRejectionPeriod") Period lateMessageRejectionPeriod,
                                  @JsonProperty("earlyMessageRejectionPeriod") Period earlyMessageRejectionPeriod,
                                  @JsonProperty("lateMessageRejectionStartDateTime")
                                      DateTime lateMessageRejectionStartDateTime)
  {
    super(topic, inputFormat, replicas, taskCount, taskDuration, startDelay, period, useEarliestOffset,
        completionTimeout, lateMessageRejectionPeriod, earlyMessageRejectionPeriod, autoScalerConfig,
        lateMessageRejectionStartDateTime);

    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    Preconditions.checkNotNull(
        consumerProperties.get(SERVICE_URL_KEY),
        StringUtils.format("consumerProperties must contain entry for [%s]", SERVICE_URL_KEY)
    );

    this.serviceUrl = (String) consumerProperties.get(SERVICE_URL_KEY);
    this.pollTimeout = pollTimeout != null ? pollTimeout : DEFAULT_POLL_TIMEOUT_MILLIS;
    this.authPluginClassName = (String) consumerProperties.get("authPluginClassName");
    this.authParams = (String) consumerProperties.get("authParams");
    this.operationTimeoutMs = (Long) consumerProperties.get("operationTimeoutMs");
    this.statsIntervalSeconds = (Long) consumerProperties.get("statsIntervalSeconds");
    this.numIoThreads = (Integer) consumerProperties.get("numIoThreads");
    this.numListenerThreads = (Integer) consumerProperties.get("numListenerThreads");
    this.useTcpNoDelay = (Boolean) consumerProperties.get("useTcpNoDelay");
    this.useTls = (Boolean) consumerProperties.get("useTls");
    this.tlsTrustCertsFilePath = (String) consumerProperties.get("tlsTrustCertsFilePath");
    this.tlsAllowInsecureConnection = (Boolean) consumerProperties.get("tlsAllowInsecureConnection");
    this.tlsHostnameVerificationEnable = (Boolean) consumerProperties.get("tlsHostnameVerificationEnable");
    this.concurrentLookupRequest = (Integer) consumerProperties.get("concurrentLookupRequest");
    this.maxLookupRequest = (Integer) consumerProperties.get("maxLookupRequest");
    this.maxNumberOfRejectedRequestPerConnection = (Integer) consumerProperties.get("maxNumberOfRejectedRequestPerConnection");
    this.connectionTimeoutMs = (Integer) consumerProperties.get("keepAliveIntervalSeconds");
    this.requestTimeoutMs = (Integer) consumerProperties.get("requestTimeoutMs");
    this.keepAliveIntervalSeconds = (Integer) consumerProperties.get("keepAliveIntervalSeconds");
    this.maxBackoffIntervalNanos = (Long) consumerProperties.get("maxBackoffIntervalNanos");
  }

  @JsonProperty
  public String getTopic()
  {
    return getStream();
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
