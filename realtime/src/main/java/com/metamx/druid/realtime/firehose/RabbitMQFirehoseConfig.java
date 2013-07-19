package com.metamx.druid.realtime.firehose;

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
}
