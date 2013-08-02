package com.metamx.druid.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rabbitmq.client.ConnectionFactory;

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * A Jacksonified version of the RabbitMQ ConnectionFactory for better integration
 * into the realtime.spec configuration file format.
 */
public class JacksonifiedConnectionFactory extends ConnectionFactory
{
  @Override
  @JsonProperty
  public String getHost()
  {
    return super.getHost();
  }

  @Override
  public void setHost(String host)
  {
    super.setHost(host);
  }

  @Override
  @JsonProperty
  public int getPort()
  {
    return super.getPort();
  }

  @Override
  public void setPort(int port)
  {
    super.setPort(port);
  }

  @Override
  @JsonProperty
  public String getUsername()
  {
    return super.getUsername();
  }

  @Override
  public void setUsername(String username)
  {
    super.setUsername(username);
  }

  @Override
  @JsonProperty
  public String getPassword()
  {
    return super.getPassword();
  }

  @Override
  public void setPassword(String password)
  {
    super.setPassword(password);
  }

  @Override
  @JsonProperty
  public String getVirtualHost()
  {
    return super.getVirtualHost();
  }

  @Override
  public void setVirtualHost(String virtualHost)
  {
    super.setVirtualHost(virtualHost);
  }

  @Override
  @JsonProperty
  public void setUri(String uriString) throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException
  {
    super.setUri(uriString);
  }

  @Override
  @JsonProperty
  public int getRequestedChannelMax()
  {
    return super.getRequestedChannelMax();
  }

  @Override
  public void setRequestedChannelMax(int requestedChannelMax)
  {
    super.setRequestedChannelMax(requestedChannelMax);
  }

  @Override
  @JsonProperty
  public int getRequestedFrameMax()
  {
    return super.getRequestedFrameMax();
  }

  @Override
  public void setRequestedFrameMax(int requestedFrameMax)
  {
    super.setRequestedFrameMax(requestedFrameMax);
  }

  @Override
  @JsonProperty
  public int getRequestedHeartbeat()
  {
    return super.getRequestedHeartbeat();
  }

  @Override
  public void setConnectionTimeout(int connectionTimeout)
  {
    super.setConnectionTimeout(connectionTimeout);
  }

  @Override
  @JsonProperty
  public int getConnectionTimeout()
  {
    return super.getConnectionTimeout();
  }

  @Override
  public void setRequestedHeartbeat(int requestedHeartbeat)
  {
    super.setRequestedHeartbeat(requestedHeartbeat);
  }

  @Override
  @JsonProperty
  public Map<String, Object> getClientProperties()
  {
    return super.getClientProperties();
  }

  @Override
  public void setClientProperties(Map<String, Object> clientProperties)
  {
    super.setClientProperties(clientProperties);
  }
}
