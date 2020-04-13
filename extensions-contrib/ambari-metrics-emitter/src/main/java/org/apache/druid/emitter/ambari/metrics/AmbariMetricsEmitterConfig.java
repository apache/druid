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

package org.apache.druid.emitter.ambari.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AmbariMetricsEmitterConfig
{
  private static final int DEFAULT_BATCH_SIZE = 100;
  private static final long DEFAULT_FLUSH_PERIOD_MILLIS = TimeUnit.MINUTES.toMillis(1); // flush every one minute
  private static final long DEFAULT_GET_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(1); // default wait for get operations on the queue 1 sec
  private static final String DEFAULT_PROTOCOL = "http";

  @JsonProperty
  private final String hostname;

  @JsonProperty
  private final int port;

  @JsonProperty
  private final String protocol;

  @JsonProperty
  private final String trustStorePath;

  @JsonProperty
  private final String trustStoreType;

  @JsonProperty
  private final String trustStorePassword;

  @JsonProperty
  private final int batchSize;

  @JsonProperty
  private final long flushPeriod;

  @JsonProperty
  private final int maxQueueSize;

  @JsonProperty("eventConverter")
  private final DruidToTimelineMetricConverter druidToTimelineEventConverter;

  @JsonProperty
  private final List<String> alertEmitters;

  @JsonProperty
  private final long emitWaitTime;

  //waiting up to the specified wait time if necessary for an event to become available.
  @JsonProperty
  private final long waitForEventTime;

  @JsonCreator
  public AmbariMetricsEmitterConfig(
      @JsonProperty("hostname") String hostname,
      @JsonProperty("port") Integer port,
      @JsonProperty("protocol") String protocol,
      @JsonProperty("trustStorePath") String trustStorePath,
      @JsonProperty("trustStoreType") String trustStoreType,
      @JsonProperty("trustStorePassword") String trustStorePassword,
      @JsonProperty("batchSize") Integer batchSize,
      @JsonProperty("flushPeriod") Long flushPeriod,
      @JsonProperty("maxQueueSize") Integer maxQueueSize,
      @JsonProperty("eventConverter") DruidToTimelineMetricConverter druidToTimelineEventConverter,
      @JsonProperty("alertEmitters") List<String> alertEmitters,
      @JsonProperty("emitWaitTime") Long emitWaitTime,
      @JsonProperty("waitForEventTime") Long waitForEventTime
  )
  {
    this.hostname = Preconditions.checkNotNull(hostname, "hostname can not be null");
    this.port = Preconditions.checkNotNull(port, "port can not be null");
    this.protocol = protocol == null ? DEFAULT_PROTOCOL : protocol;
    this.trustStorePath = trustStorePath;
    this.trustStoreType = trustStoreType;
    this.trustStorePassword = trustStorePassword;
    this.batchSize = (batchSize == null) ? DEFAULT_BATCH_SIZE : batchSize;
    this.flushPeriod = flushPeriod == null ? DEFAULT_FLUSH_PERIOD_MILLIS : flushPeriod;
    this.maxQueueSize = maxQueueSize == null ? Integer.MAX_VALUE : maxQueueSize;
    this.druidToTimelineEventConverter = Preconditions.checkNotNull(
        druidToTimelineEventConverter,
        "Event converter can not be null"
    );
    this.alertEmitters = alertEmitters == null ? Collections.emptyList() : alertEmitters;
    this.emitWaitTime = emitWaitTime == null ? 0 : emitWaitTime;
    this.waitForEventTime = waitForEventTime == null ? DEFAULT_GET_TIMEOUT_MILLIS : waitForEventTime;
  }

  @JsonProperty
  public String getHostname()
  {
    return hostname;
  }

  @JsonProperty
  public int getPort()
  {
    return port;
  }

  @JsonProperty
  public String getProtocol()
  {
    return protocol;
  }

  @JsonProperty
  public String getTrustStorePath()
  {
    return trustStorePath;
  }

  @JsonProperty
  public String getTrustStoreType()
  {
    return trustStoreType;
  }

  @JsonProperty
  public String getTrustStorePassword()
  {
    return trustStorePassword;
  }

  @JsonProperty
  public int getBatchSize()
  {
    return batchSize;
  }

  @JsonProperty
  public int getMaxQueueSize()
  {
    return maxQueueSize;
  }

  @JsonProperty
  public long getFlushPeriod()
  {
    return flushPeriod;
  }

  @JsonProperty
  public DruidToTimelineMetricConverter getDruidToTimelineEventConverter()
  {
    return druidToTimelineEventConverter;
  }

  @JsonProperty
  public List<String> getAlertEmitters()
  {
    return alertEmitters;
  }

  @JsonProperty
  public long getEmitWaitTime()
  {
    return emitWaitTime;
  }

  @JsonProperty
  public long getWaitForEventTime()
  {
    return waitForEventTime;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AmbariMetricsEmitterConfig that = (AmbariMetricsEmitterConfig) o;

    if (port != that.port) {
      return false;
    }
    if (batchSize != that.batchSize) {
      return false;
    }
    if (flushPeriod != that.flushPeriod) {
      return false;
    }
    if (maxQueueSize != that.maxQueueSize) {
      return false;
    }
    if (emitWaitTime != that.emitWaitTime) {
      return false;
    }
    if (waitForEventTime != that.waitForEventTime) {
      return false;
    }
    if (!hostname.equals(that.hostname)) {
      return false;
    }
    if (!protocol.equals(that.protocol)) {
      return false;
    }
    if (trustStorePath != null ? !trustStorePath.equals(that.trustStorePath) : that.trustStorePath != null) {
      return false;
    }
    if (trustStoreType != null ? !trustStoreType.equals(that.trustStoreType) : that.trustStoreType != null) {
      return false;
    }
    if (trustStorePassword != null
        ? !trustStorePassword.equals(that.trustStorePassword)
        : that.trustStorePassword != null) {
      return false;
    }
    if (!druidToTimelineEventConverter.equals(that.druidToTimelineEventConverter)) {
      return false;
    }
    return alertEmitters.equals(that.alertEmitters);

  }

  @Override
  public int hashCode()
  {
    int result = hostname.hashCode();
    result = 31 * result + port;
    result = 31 * result + protocol.hashCode();
    result = 31 * result + (trustStorePath != null ? trustStorePath.hashCode() : 0);
    result = 31 * result + (trustStoreType != null ? trustStoreType.hashCode() : 0);
    result = 31 * result + (trustStorePassword != null ? trustStorePassword.hashCode() : 0);
    result = 31 * result + batchSize;
    result = 31 * result + (int) (flushPeriod ^ (flushPeriod >>> 32));
    result = 31 * result + maxQueueSize;
    result = 31 * result + druidToTimelineEventConverter.hashCode();
    result = 31 * result + alertEmitters.hashCode();
    result = 31 * result + (int) (emitWaitTime ^ (emitWaitTime >>> 32));
    result = 31 * result + (int) (waitForEventTime ^ (waitForEventTime >>> 32));
    return result;
  }
}
