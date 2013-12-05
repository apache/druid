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
