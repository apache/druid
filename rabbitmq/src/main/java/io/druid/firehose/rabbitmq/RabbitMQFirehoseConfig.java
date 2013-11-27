/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.firehose.rabbitmq;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A configuration object for a RabbitMQ connection.
 */
public class RabbitMQFirehoseConfig
{
  private String queue = null;
  private String exchange = null;
  private String routingKey = null;
  private boolean durable = false;
  private boolean exclusive = false;
  private boolean autoDelete = false;

  // Lyra (auto reconnect) properties
  private int maxRetries = 100;
  private int retryIntervalSeconds = 2;
  private long maxDurationSeconds = 5 * 60;

  @JsonProperty
  public String getQueue()
  {
    return queue;
  }

  public void setQueue(String queue)
  {
    this.queue = queue;
  }

  @JsonProperty
  public String getExchange()
  {
    return exchange;
  }

  public void setExchange(String exchange)
  {
    this.exchange = exchange;
  }

  @JsonProperty
  public String getRoutingKey()
  {
    return routingKey;
  }

  public void setRoutingKey(String routingKey)
  {
    this.routingKey = routingKey;
  }

  @JsonProperty
  public boolean isDurable()
  {
    return durable;
  }

  public void setDurable(boolean durable)
  {
    this.durable = durable;
  }

  @JsonProperty
  public boolean isExclusive()
  {
    return exclusive;
  }

  public void setExclusive(boolean exclusive)
  {
    this.exclusive = exclusive;
  }

  @JsonProperty
  public boolean isAutoDelete()
  {
    return autoDelete;
  }

  public void setAutoDelete(boolean autoDelete)
  {
    this.autoDelete = autoDelete;
  }

  @JsonProperty
  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  @JsonProperty
  public int getRetryIntervalSeconds() {
    return retryIntervalSeconds;
  }

  public void setRetryIntervalSeconds(int retryIntervalSeconds) {
    this.retryIntervalSeconds = retryIntervalSeconds;
  }

  @JsonProperty
  public long getMaxDurationSeconds() {
    return maxDurationSeconds;
  }

  public void setMaxDurationSeconds(int maxDurationSeconds) {
    this.maxDurationSeconds = maxDurationSeconds;
  }
}
