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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.LongString;

import java.net.URI;
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
  public static JacksonifiedConnectionFactory makeDefaultConnectionFactory() throws Exception
  {
    return new JacksonifiedConnectionFactory(null, 0, null, null, null, null, 0, 0, 0, 0, null);
  }

  private static Map<String, Object> getSerializableClientProperties(final Map<String, Object> clientProperties)
  {
    return Maps.transformEntries(
        clientProperties,
        new Maps.EntryTransformer<String, Object, Object>()
        {
          @Override
          public Object transformEntry(String key, Object value)
          {
            if (value instanceof LongString) {
              return value.toString();
            }
            return value;
          }
        }
    );
  }

  private final String host;
  private final int port;
  private final String username;
  private final String password;
  private final String virtualHost;
  private final String uri;
  private final int requestedChannelMax;
  private final int requestedFrameMax;
  private final int requestedHeartbeat;
  private final int connectionTimeout;
  private final Map<String, Object> clientProperties;

  @JsonCreator
  public JacksonifiedConnectionFactory(
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("username") String username,
      @JsonProperty("password") String password,
      @JsonProperty("virtualHost") String virtualHost,
      @JsonProperty("uri") String uri,
      @JsonProperty("requestedChannelMax") int requestedChannelMax,
      @JsonProperty("requestedFrameMax") int requestedFrameMax,
      @JsonProperty("requestedHeartbeat") int requestedHeartbeat,
      @JsonProperty("connectionTimeout") int connectionTimeout,
      @JsonProperty("clientProperties") Map<String, Object> clientProperties
  ) throws Exception
  {
    super();

    this.host = host == null ? super.getHost() : host;
    this.port = port == 0 ? super.getPort() : port;
    this.username = username == null ? super.getUsername() : username;
    this.password = password == null ? super.getPassword() : password;
    this.virtualHost = virtualHost == null ? super.getVirtualHost() : virtualHost;
    this.uri = uri;
    this.requestedChannelMax = requestedChannelMax == 0 ? super.getRequestedChannelMax() : requestedChannelMax;
    this.requestedFrameMax = requestedFrameMax == 0 ? super.getRequestedFrameMax() : requestedFrameMax;
    this.requestedHeartbeat = requestedHeartbeat == 0 ? super.getRequestedHeartbeat() : requestedHeartbeat;
    this.connectionTimeout = connectionTimeout == 0 ? super.getConnectionTimeout() : connectionTimeout;
    this.clientProperties = clientProperties == null ? super.getClientProperties() : clientProperties;

    super.setHost(this.host);
    super.setPort(this.port);
    super.setUsername(this.username);
    super.setPassword(this.password);
    super.setVirtualHost(this.virtualHost);
    if (this.uri != null) {
      super.setUri(this.uri);
    }
    super.setRequestedChannelMax(this.requestedChannelMax);
    super.setRequestedFrameMax(this.requestedFrameMax);
    super.setRequestedHeartbeat(this.requestedHeartbeat);
    super.setConnectionTimeout(this.connectionTimeout);
    super.setClientProperties(this.clientProperties);
  }

  @Override
  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @Override
  @JsonProperty
  public int getPort()
  {
    return port;
  }


  @Override
  @JsonProperty
  public String getUsername()
  {
    return username;
  }

  @Override
  @JsonProperty
  public String getPassword()
  {
    return password;
  }

  @Override
  @JsonProperty
  public String getVirtualHost()
  {
    return virtualHost;
  }

  @JsonProperty
  public String getUri()
  {
    return uri;
  }

  // we are only overriding this to help Jackson not be confused about the two setURI methods
  @JsonIgnore
  @Override
  public void setUri(URI uri) throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException
  {
    super.setUri(uri);
  }

  @Override
  public void setUri(String uriString) throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException
  {
    super.setUri(uriString);
  }

  @Override
  @JsonProperty
  public int getRequestedChannelMax()
  {
    return requestedChannelMax;
  }

  @Override
  @JsonProperty
  public int getRequestedFrameMax()
  {
    return requestedFrameMax;
  }

  @Override
  @JsonProperty
  public int getRequestedHeartbeat()
  {
    return requestedHeartbeat;
  }

  @Override
  @JsonProperty
  public int getConnectionTimeout()
  {
    return connectionTimeout;
  }

  @JsonProperty("clientProperties")
  public Map<String, Object> getSerializableClientProperties()
  {
    return getSerializableClientProperties(clientProperties);
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

    JacksonifiedConnectionFactory that = (JacksonifiedConnectionFactory) o;

    if (connectionTimeout != that.connectionTimeout) {
      return false;
    }
    if (port != that.port) {
      return false;
    }
    if (requestedChannelMax != that.requestedChannelMax) {
      return false;
    }
    if (requestedFrameMax != that.requestedFrameMax) {
      return false;
    }
    if (requestedHeartbeat != that.requestedHeartbeat) {
      return false;
    }
    if (clientProperties != null
        ? !Maps.difference(
        getSerializableClientProperties(clientProperties),
        getSerializableClientProperties(that.clientProperties)
    ).areEqual()
        : that.clientProperties != null) {
      return false;
    }
    if (host != null ? !host.equals(that.host) : that.host != null) {
      return false;
    }
    if (password != null ? !password.equals(that.password) : that.password != null) {
      return false;
    }
    if (uri != null ? !uri.equals(that.uri) : that.uri != null) {
      return false;
    }
    if (username != null ? !username.equals(that.username) : that.username != null) {
      return false;
    }
    if (virtualHost != null ? !virtualHost.equals(that.virtualHost) : that.virtualHost != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = host != null ? host.hashCode() : 0;
    result = 31 * result + port;
    result = 31 * result + (username != null ? username.hashCode() : 0);
    result = 31 * result + (password != null ? password.hashCode() : 0);
    result = 31 * result + (virtualHost != null ? virtualHost.hashCode() : 0);
    result = 31 * result + (uri != null ? uri.hashCode() : 0);
    result = 31 * result + requestedChannelMax;
    result = 31 * result + requestedFrameMax;
    result = 31 * result + requestedHeartbeat;
    result = 31 * result + connectionTimeout;
    result = 31 * result + (clientProperties != null ? clientProperties.hashCode() : 0);
    return result;
  }
}
