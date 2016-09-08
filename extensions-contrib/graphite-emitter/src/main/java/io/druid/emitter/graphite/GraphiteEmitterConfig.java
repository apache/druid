/*
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.druid.emitter.graphite;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;


public class GraphiteEmitterConfig
{
  private final static int DEFAULT_BATCH_SIZE = 100;
  private static final Long DEFAULT_FLUSH_PERIOD = (long) (60 * 1000); // flush every one minute
  private final static long DEFAULT_GET_TIMEOUT = 1000; // default wait for get operations on the queue 1 sec

  @JsonProperty
  final private String hostname;
  @JsonProperty
  final private int port;
  @JsonProperty
  final private int batchSize;
  @JsonProperty
  final private Long flushPeriod;
  @JsonProperty
  final private Integer maxQueueSize;
  @JsonProperty("eventConverter")
  final private DruidToGraphiteEventConverter druidToGraphiteEventConverter;

  @JsonProperty
  final private List<String> alertEmitters;

  @JsonProperty
  final private Long emitWaitTime;

  //waiting up to the specified wait time if necessary for an event to become available.
  @JsonProperty
  final private Long waitForEventTime;

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GraphiteEmitterConfig)) {
      return false;
    }

    GraphiteEmitterConfig that = (GraphiteEmitterConfig) o;

    if (getPort() != that.getPort()) {
      return false;
    }
    if (getBatchSize() != that.getBatchSize()) {
      return false;
    }
    if (!getHostname().equals(that.getHostname())) {
      return false;
    }
    if (!getFlushPeriod().equals(that.getFlushPeriod())) {
      return false;
    }
    if (!getMaxQueueSize().equals(that.getMaxQueueSize())) {
      return false;
    }
    if (!getDruidToGraphiteEventConverter().equals(that.getDruidToGraphiteEventConverter())) {
      return false;
    }
    if (getAlertEmitters() != null
        ? !getAlertEmitters().equals(that.getAlertEmitters())
        : that.getAlertEmitters() != null) {
      return false;
    }
    if (!getEmitWaitTime().equals(that.getEmitWaitTime())) {
      return false;
    }
    return getWaitForEventTime().equals(that.getWaitForEventTime());

  }

  @Override
  public int hashCode()
  {
    int result = getHostname().hashCode();
    result = 31 * result + getPort();
    result = 31 * result + getBatchSize();
    result = 31 * result + getFlushPeriod().hashCode();
    result = 31 * result + getMaxQueueSize().hashCode();
    result = 31 * result + getDruidToGraphiteEventConverter().hashCode();
    result = 31 * result + (getAlertEmitters() != null ? getAlertEmitters().hashCode() : 0);
    result = 31 * result + getEmitWaitTime().hashCode();
    result = 31 * result + getWaitForEventTime().hashCode();
    return result;
  }

  @JsonCreator
  public GraphiteEmitterConfig(
      @JsonProperty("hostname") String hostname,
      @JsonProperty("port") Integer port,
      @JsonProperty("batchSize") Integer batchSize,
      @JsonProperty("flushPeriod") Long flushPeriod,
      @JsonProperty("maxQueueSize") Integer maxQueueSize,
      @JsonProperty("eventConverter") DruidToGraphiteEventConverter druidToGraphiteEventConverter,
      @JsonProperty("alertEmitters") List<String> alertEmitters,
      @JsonProperty("emitWaitTime") Long emitWaitTime,
      @JsonProperty("waitForEventTime") Long waitForEventTime
  )
  {
    this.waitForEventTime = waitForEventTime == null ? DEFAULT_GET_TIMEOUT : waitForEventTime;
    this.emitWaitTime = emitWaitTime == null ? 0 : emitWaitTime;
    this.alertEmitters = alertEmitters == null ? Collections.<String>emptyList() : alertEmitters;
    this.druidToGraphiteEventConverter = Preconditions.checkNotNull(
        druidToGraphiteEventConverter,
        "Event converter can not ne null dude"
    );
    this.flushPeriod = flushPeriod == null ? DEFAULT_FLUSH_PERIOD : flushPeriod;
    this.maxQueueSize = maxQueueSize == null ? Integer.MAX_VALUE : maxQueueSize;
    this.hostname = Preconditions.checkNotNull(hostname, "hostname can not be null");
    this.port = Preconditions.checkNotNull(port, "port can not be null");
    this.batchSize = (batchSize == null) ? DEFAULT_BATCH_SIZE : batchSize;
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
  public int getBatchSize()
  {
    return batchSize;
  }

  @JsonProperty
  public Integer getMaxQueueSize()
  {
    return maxQueueSize;
  }

  @JsonProperty
  public Long getFlushPeriod()
  {
    return flushPeriod;
  }

  @JsonProperty
  public DruidToGraphiteEventConverter getDruidToGraphiteEventConverter()
  {
    return druidToGraphiteEventConverter;
  }

  @JsonProperty
  public List<String> getAlertEmitters()
  {
    return alertEmitters;
  }

  @JsonProperty
  public Long getEmitWaitTime()
  {
    return emitWaitTime;
  }

  @JsonProperty
  public Long getWaitForEventTime()
  {
    return waitForEventTime;
  }
}
