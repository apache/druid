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
import java.net.URI;
import java.net.URISyntaxException;

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

  /**
   * host = null     , port = null -> host = _default_, port = -1
   * host = "abc:123", port = null -> host = abc, port = 123
   * host = "abc:fff", port = null -> throw IAE (invalid ipv6 host)
   * host = "2001:db8:85a3::8a2e:370:7334", port = null -> host = [2001:db8:85a3::8a2e:370:7334], port = _auto_
   * host = "[2001:db8:85a3::8a2e:370:7334]", port = null -> host = [2001:db8:85a3::8a2e:370:7334], port = _auto_
   * host = "abc"    , port = null -> host = abc, port = _auto_
   * host = "abc"    , port = 123  -> host = abc, port = 123
   * host = "abc:123 , port = 123  -> host = abc, port = 123
   * host = "abc:123 , port = 456  -> throw IAE (conflicting port)
   * host = "abc:fff , port = 456  -> throw IAE (invalid ipv6 host)
   * host = "[2001:db8:85a3::8a2e:370:7334]:123", port = null -> host = [2001:db8:85a3::8a2e:370:7334], port = 123
   * host = "[2001:db8:85a3::8a2e:370:7334]", port = 123 -> host = [2001:db8:85a3::8a2e:370:7334], port = 123
   * host = "2001:db8:85a3::8a2e:370:7334", port = 123 -> host = [2001:db8:85a3::8a2e:370:7334], port = 123
   * host = null     , port = 123  -> host = _default_, port = 123
   */
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

    int parsedPort = -1;
    if (host != null) {
      try {
        // try host:port parsing (necessary for IPv6)
        final URI uri = new URI(null, host, null, null, null);
        // host is null if authority cannot be parsed into host and port
        if(uri.getHost() != null) {
          parsedPort = uri.getPort();
          host = uri.getHost();
        } else {
          throw new IllegalArgumentException();
        }
      }
      catch (IllegalArgumentException | URISyntaxException ee) {
        // try host alone
        try {
          final URI uri = new URI(null, host, null, null);
          host = uri.getHost();
        } catch(URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
      }
      if (port != null && parsedPort != -1 && port != parsedPort) {
        throw new IAE("Conflicting host:port [%s] and port [%d] settings", host, port);
      }
    }

    if (port == null) {
      if (parsedPort == -1 && host != null) {
        port = SocketUtil.findOpenPort(8080);
      } else {
        port = parsedPort;
      }
    }

    this.port = port != null ? port : -1;
    this.host = host != null ? host : DEFAULT_HOST;
    try {
      new URI(null, null, this.host, this.port, null, null, null).getAuthority();
    } catch(URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
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

  /**
   * Returns host and port together as something that can be used as part of a URI.
   */
  public String getHostAndPort() {
    try {
      return new URI(null, null, host, port, null, null, null).getAuthority();
    } catch(URISyntaxException e) {
      // should never happen, since we tried it in init already
      throw new RuntimeException(e);
    }
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
