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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
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

  /**
   * host = null     , port = null -> host = _default_, port = -1
   * host = "abc:123", port = null -> host = abc, port = 123
   * host = "abc:fff", port = null -> throw IAE (invalid ipv6 host)
   * host = "2001:db8:85a3::8a2e:370:7334", port = null -> host = 2001:db8:85a3::8a2e:370:7334, port = _auto_
   * host = "[2001:db8:85a3::8a2e:370:7334]", port = null -> host = 2001:db8:85a3::8a2e:370:7334, port = _auto_
   * host = "abc"    , port = null -> host = abc, port = _auto_
   * host = "abc"    , port = 123  -> host = abc, port = 123
   * host = "abc:123 , port = 123  -> host = abc, port = 123
   * host = "abc:123 , port = 456  -> throw IAE (conflicting port)
   * host = "abc:fff , port = 456  -> throw IAE (invalid ipv6 host)
   * host = "[2001:db8:85a3::8a2e:370:7334]:123", port = null -> host = 2001:db8:85a3::8a2e:370:7334, port = 123
   * host = "[2001:db8:85a3::8a2e:370:7334]", port = 123 -> host = 2001:db8:85a3::8a2e:370:7334, port = 123
   * host = "2001:db8:85a3::8a2e:370:7334", port = 123 -> host = 2001:db8:85a3::8a2e:370:7334, port = 123
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
    Preconditions.checkNotNull(serviceName);
    this.serviceName = serviceName;

    if(host == null && port == null) {
      host = DEFAULT_HOST;
      port = -1;
    }
    else {
      final HostAndPort hostAndPort;
      if (host != null) {
        hostAndPort = HostAndPort.fromString(host);
        if (port != null && hostAndPort.hasPort() && port != hostAndPort.getPort()) {
          throw new IAE("Conflicting host:port [%s] and port [%d] settings", host, port);
        }
      } else {
        hostAndPort = HostAndPort.fromParts(DEFAULT_HOST, port);
      }

      host = hostAndPort.getHostText();

      if (hostAndPort.hasPort()) {
        port = hostAndPort.getPort();
      }

      if (port == null) {
        port = SocketUtil.findOpenPort(8080);
      }
    }

    this.port = port;
    this.host = host;
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
    if(port < 0) {
      return HostAndPort.fromString(host).toString();
    } else {
      return HostAndPort.fromParts(host, port).toString();
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
