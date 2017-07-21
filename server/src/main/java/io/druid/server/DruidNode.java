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

package io.druid.server;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.inject.name.Named;
import io.druid.common.utils.SocketUtil;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.server.initialization.ServerConfig;

import javax.validation.constraints.Max;
import javax.validation.constraints.NotNull;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 */
public class DruidNode
{
  @JsonProperty("service")
  @NotNull
  private String serviceName;

  @JsonProperty
  @NotNull
  private String host;

  /**
   * This property is now deprecated, this is present just so that JsonConfigurator does not fail if this is set.
   * Please use {@link DruidNode#plaintextPort} instead, which if set will be used and hence this has -1 as default value.
   * */
  @Deprecated
  @JsonProperty
  @Max(0xffff)
  private int port = -1;

  @JsonProperty
  @Max(0xffff)
  private int plaintextPort = -1;

  @JsonProperty
  @Max(0xffff)
  private int tlsPort = -1;

  @JacksonInject
  @NotNull
  private ServerConfig serverConfig;

  public DruidNode(String serviceName, String host, Integer plaintextPort, Integer tlsPort, ServerConfig serverConfig)
  {
    this(serviceName, host, plaintextPort, null, tlsPort, serverConfig);
    this.serverConfig = serverConfig;
  }

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
      @JsonProperty("plaintextPort") Integer plaintextPort,
      @JacksonInject @Named("servicePort") @JsonProperty("port") Integer port,
      @JacksonInject @Named("tlsServicePort") @JsonProperty("tlsPort") Integer tlsPort,
      @JacksonInject ServerConfig serverConfig
  )
  {
    init(serviceName, host, plaintextPort != null ? plaintextPort : port, tlsPort, serverConfig);
  }

  private void init(String serviceName, String host, Integer plainTextPort, Integer tlsPort, ServerConfig serverConfig)
  {
    Preconditions.checkNotNull(serviceName);

    if (!serverConfig.isTls() && !serverConfig.isPlaintext()) {
      throw new IAE("At least one of the druid.server.http.plainText or druid.server.http.tls needs to be enabled");
    }

    final boolean nullHost = host == null;
    HostAndPort hostAndPort;
    Integer portFromHostConfig;
    if (host != null) {
      hostAndPort = HostAndPort.fromString(host);
      host = hostAndPort.getHostText();
      portFromHostConfig = hostAndPort.hasPort() ? hostAndPort.getPort() : null;
      if (plainTextPort != null && portFromHostConfig != null && !plainTextPort.equals(portFromHostConfig)) {
        throw new IAE("Conflicting host:port [%s] and port [%d] settings", host, plainTextPort);
      }
      if (portFromHostConfig != null) {
        plainTextPort = portFromHostConfig;
      }
    } else {
      host = getDefaultHost();
    }

    if (serverConfig.isPlaintext() && serverConfig.isTls() && ((plainTextPort == null || tlsPort == null)
                                                               || plainTextPort.equals(tlsPort))) {
      // If both plainTExt and tls are enabled then do not allow plaintextPort to be null or
      throw new IAE("plaintextPort and tlsPort cannot be null or same if both http and https connectors are enabled");
    }
    if (serverConfig.isTls() && (tlsPort == null || tlsPort < 0)) {
      throw new IAE("A valid tlsPort needs to specified when druid.server.http.tls is set");
    }

    if (serverConfig.isPlaintext()) {
      // to preserve backwards compatible behaviour
      if (nullHost && plainTextPort == null) {
        plainTextPort = -1;
      } else {
        if (plainTextPort == null) {
          plainTextPort = SocketUtil.findOpenPort(8080);
        }
      }
      this.plaintextPort = plainTextPort;
    } else {
      this.plaintextPort = -1;
    }
    if (serverConfig.isTls()) {
      this.tlsPort = tlsPort;
    } else {
      this.tlsPort = -1;
    }

    this.serviceName = serviceName;
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

  public int getPlaintextPort()
  {
    return plaintextPort;
  }

  public int getTlsPort()
  {
    return tlsPort;
  }

  public DruidNode withService(String service)
  {
    return new DruidNode(service, host, plaintextPort, tlsPort, serverConfig);
  }

  public String getServiceScheme()
  {
    return tlsPort >= 0 ? "https" : "http";
  }

  /**
   * Returns host and port together as something that can be used as part of a URI.
   */
  public String getHostAndPort()
  {
    if (serverConfig.isPlaintext()) {
      if (plaintextPort < 0) {
        return HostAndPort.fromString(host).toString();
      } else {
        return HostAndPort.fromParts(host, plaintextPort).toString();
      }
    }
    return null;
  }

  public String getHostAndTlsPort()
  {
    if (serverConfig.isTls()) {
      return HostAndPort.fromParts(host, tlsPort).toString();
    }
    return null;
  }

  public String getHostAndPortToUse()
  {
    return getHostAndTlsPort() != null ? getHostAndTlsPort() : getHostAndPort();
  }

  public static String getDefaultHost()
  {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    }
    catch (UnknownHostException e) {
      throw new ISE(e, "Unable to determine host name");
    }
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

    DruidNode druidNode = (DruidNode) o;

    if (plaintextPort != druidNode.plaintextPort) {
      return false;
    }
    if (tlsPort != druidNode.tlsPort) {
      return false;
    }
    if (serviceName != null ? !serviceName.equals(druidNode.serviceName) : druidNode.serviceName != null) {
      return false;
    }
    if (host != null ? !host.equals(druidNode.host) : druidNode.host != null) {
      return false;
    }
    return serverConfig != null ? serverConfig.equals(druidNode.serverConfig) : druidNode.serverConfig == null;
  }

  @Override
  public int hashCode()
  {
    int result = serviceName != null ? serviceName.hashCode() : 0;
    result = 31 * result + (host != null ? host.hashCode() : 0);
    result = 31 * result + plaintextPort;
    result = 31 * result + tlsPort;
    result = 31 * result + (serverConfig != null ? serverConfig.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "DruidNode{" +
           "serviceName='" + serviceName + '\'' +
           ", host='" + host + '\'' +
           ", plaintextPort=" + plaintextPort +
           ", tlsPort=" + tlsPort +
           ", serverConfig=" + serverConfig +
           '}';
  }
}
