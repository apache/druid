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
import com.metamx.common.IAE;
import io.druid.common.utils.SocketUtil;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 */
public class DruidNode
{
  public static final String DEFAULT_HOST = "localhost";

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

  /**
   * host = "abc:123", port = null -> host = abc, port = 123
   * host = "abc:fff", port = null -> host = abc, port = -1
   * host = "abc"    , port = null -> host = abc, port = _auto_
   * host = null     , port = null -> host = _default_, port = -1
   * host = "abc:123 , port = 456  -> throw IAE
   * host = "abc:fff , port = 456  -> throw IAE
   * host = "abc:123 , port = 123  -> host = abc, port = 123
   * host = "abc"    , port = 123  -> host = abc, port = 123
   * host = null     , port = 123  -> host = _default_, port = 123
   */
  private void init(String serviceName, String host, Integer port)
  {
    this.serviceName = serviceName;

    if (host != null && host.contains(":")) {
      final String[] hostParts = host.split(":");
      int parsedPort = -1;
      try {
        parsedPort = Integer.parseInt(hostParts[1]);
      }
      catch (NumberFormatException e) {
        // leave -1
      }
      if (port != null && port != parsedPort) {
        throw new IAE("Conflicting host:port [%s] and port [%d] settings", host, port);
      }
      host = hostParts[0];
      port = parsedPort;
    }

    if (port == null && host != null) {
      port = SocketUtil.findOpenPort(8080);
    }

    this.port = port != null ? port : -1;
    this.host = host != null ? host : DEFAULT_HOST;
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

  public String getHostAndPort() {
    return String.format("%s:%d", host, port);
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
