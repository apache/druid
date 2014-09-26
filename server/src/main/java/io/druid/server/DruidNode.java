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

package io.druid.server;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.name.Named;
import io.druid.common.utils.SocketUtil;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 */
public class DruidNode
{
  public static final String DEFAULT_HOST = "localhost";

  private String hostNoPort;

  @JsonProperty("service")
  @NotNull
  private String serviceName;

  @JsonProperty
  @NotNull
  private String host;

  @JsonProperty
  @Min(0) @Max(0xffff)
  private int port = -1;

  @JsonCreator
  public DruidNode(
      @JacksonInject @Named("serviceName") @JsonProperty("service") String serviceName,
      @JsonProperty("host") String host,
      @JacksonInject @Named("servicePort") @JsonProperty("port") Integer port
  )
  {
    init(serviceName, host, port);
  }

  private void init(String serviceName, String host, Integer port)
  {
    this.serviceName = serviceName;

    if (port == null) {
      if (host == null) {
        setHostAndPort(DEFAULT_HOST, -1, DEFAULT_HOST);
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
        setHostAndPort(host == null ? DEFAULT_HOST : host, port, host == null ? DEFAULT_HOST : host.split(":")[0]);
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
