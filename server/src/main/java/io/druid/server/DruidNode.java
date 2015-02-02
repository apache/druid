/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.common.utils.SocketUtil;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 */
public class DruidNode
{
  private String hostNoPort;

  @JsonProperty("service")
  @NotNull
  private String serviceName = null;

  @JsonProperty
  @NotNull
  private String host = null;

  @JsonProperty
  @Min(0) @Max(0xffff)
  private int port = -1;

  @JsonCreator
  public DruidNode(
      @JsonProperty("service") String serviceName,
      @JsonProperty("host") String host,
      @JsonProperty("port") Integer port
  )
  {
    this.serviceName = serviceName;

    if (port == null) {
      if (host == null) {
        setHostAndPort(null, -1, null);
      }
      else if (host.contains(":")) {
        final String[] hostParts = host.split(":");
        try {
          setHostAndPort(host, Integer.parseInt(hostParts[1]), hostParts[0]);
        }
        catch (NumberFormatException e) {
          setHostAndPort(host, -1, hostParts[0]);
        }
      }
      else {
        final int openPort = SocketUtil.findOpenPort(8080);
        setHostAndPort(String.format("%s:%d", host, openPort), openPort, host);
      }
    }
    else {
      if (host == null || host.contains(":")) {
        setHostAndPort(host, port, host == null ? null : host.split(":")[0]);
      }
      else {
        setHostAndPort(String.format("%s:%d", host, port), port, host);
      }
    }
  }

  private void setHostAndPort(String host, int port, String hostNoPort)
  {
    this.host = host;
    this.port = port;
    this.hostNoPort = hostNoPort;
  }

  public String getServiceName()
  {
    return serviceName;
  }

  public String getHost()
  {
    return host;
  }

  public int getPort()
  {
    return port;
  }

  public String getHostNoPort()
  {
    return hostNoPort;
  }

  @Override
  public String toString()
  {
    return "DruidNode{" +
           "serviceName='" + serviceName + '\'' +
           ", host='" + host + '\'' +
           ", port=" + port +
           '}';
  }
}
