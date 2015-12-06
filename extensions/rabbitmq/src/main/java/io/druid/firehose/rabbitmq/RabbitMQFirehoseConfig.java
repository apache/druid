/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.firehose.rabbitmq;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A configuration object for a RabbitMQ connection.
 */
public class RabbitMQFirehoseConfig
{
  // Lyra (auto reconnect) properties
  private static final int defaultMaxRetries = 100;
  private static final int defaultRetryIntervalSeconds = 2;
  private static final long defaultMaxDurationSeconds = 5 * 60;

  public static RabbitMQFirehoseConfig makeDefaultConfig()
  {
    return new RabbitMQFirehoseConfig(null, null, null, false, false, false, 0, 0, 0);
  }

  private final String queue;
  private final String exchange;
  private final String routingKey;
  private final boolean durable;
  private final boolean exclusive;
  private final boolean autoDelete;
  private final int maxRetries;
  private final int retryIntervalSeconds;
  private final long maxDurationSeconds;

  @JsonCreator
  public RabbitMQFirehoseConfig(
      @JsonProperty("queue") String queue,
      @JsonProperty("exchange") String exchange,
      @JsonProperty("routingKey") String routingKey,
      @JsonProperty("durable") boolean durable,
      @JsonProperty("exclusive") boolean exclusive,
      @JsonProperty("autoDelete") boolean autoDelete,
      @JsonProperty("maxRetries") int maxRetries,
      @JsonProperty("retryIntervalSeconds") int retryIntervalSeconds,
      @JsonProperty("maxDurationSeconds") long maxDurationSeconds
  )
  {
    this.queue = queue;
    this.exchange = exchange;
    this.routingKey = routingKey;
    this.durable = durable;
    this.exclusive = exclusive;
    this.autoDelete = autoDelete;

    this.maxRetries = maxRetries == 0 ? defaultMaxRetries : maxRetries;
    this.retryIntervalSeconds = retryIntervalSeconds == 0 ? defaultRetryIntervalSeconds : retryIntervalSeconds;
    this.maxDurationSeconds = maxDurationSeconds == 0 ? defaultMaxDurationSeconds : maxDurationSeconds;
  }

  @JsonProperty
  public String getQueue()
  {
    return queue;
  }

  @JsonProperty
  public String getExchange()
  {
    return exchange;
  }

  @JsonProperty
  public String getRoutingKey()
  {
    return routingKey;
  }

  @JsonProperty
  public boolean isDurable()
  {
    return durable;
  }

  @JsonProperty
  public boolean isExclusive()
  {
    return exclusive;
  }

  @JsonProperty
  public boolean isAutoDelete()
  {
    return autoDelete;
  }

  @JsonProperty
  public int getMaxRetries()
  {
    return maxRetries;
  }

  @JsonProperty
  public int getRetryIntervalSeconds()
  {
    return retryIntervalSeconds;
  }

  @JsonProperty
  public long getMaxDurationSeconds()
  {
    return maxDurationSeconds;
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

    RabbitMQFirehoseConfig that = (RabbitMQFirehoseConfig) o;

    if (autoDelete != that.autoDelete) {
      return false;
    }
    if (durable != that.durable) {
      return false;
    }
    if (exclusive != that.exclusive) {
      return false;
    }
    if (maxDurationSeconds != that.maxDurationSeconds) {
      return false;
    }
    if (maxRetries != that.maxRetries) {
      return false;
    }
    if (retryIntervalSeconds != that.retryIntervalSeconds) {
      return false;
    }
    if (exchange != null ? !exchange.equals(that.exchange) : that.exchange != null) {
      return false;
    }
    if (queue != null ? !queue.equals(that.queue) : that.queue != null) {
      return false;
    }
    if (routingKey != null ? !routingKey.equals(that.routingKey) : that.routingKey != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = queue != null ? queue.hashCode() : 0;
    result = 31 * result + (exchange != null ? exchange.hashCode() : 0);
    result = 31 * result + (routingKey != null ? routingKey.hashCode() : 0);
    result = 31 * result + (durable ? 1 : 0);
    result = 31 * result + (exclusive ? 1 : 0);
    result = 31 * result + (autoDelete ? 1 : 0);
    result = 31 * result + maxRetries;
    result = 31 * result + retryIntervalSeconds;
    result = 31 * result + (int) (maxDurationSeconds ^ (maxDurationSeconds >>> 32));
    return result;
  }
}
