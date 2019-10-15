/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.inject.name.Named;
import org.apache.druid.common.utils.SocketUtil;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.validation.constraints.Max;
import javax.validation.constraints.NotNull;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

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
   * This property indicates whether the druid node's internal jetty server bind on {@link DruidNode#host}.
   * Default is false, which means binding to all interfaces.
   */
  @JsonProperty
  private boolean bindOnHost = false;

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
  private boolean enablePlaintextPort = true;

  @JsonProperty
  @Max(0xffff)
  private int tlsPort = -1;

  @JsonProperty
  private boolean enableTlsPort = false;

  public DruidNode(
      String serviceName,
      String host,
      boolean bindOnHost,
      Integer plaintextPort,
      Integer tlsPort,
      boolean enablePlaintextPort,
      boolean enableTlsPort
  )
  {
    this(serviceName, host, bindOnHost, plaintextPort, null, tlsPort, enablePlaintextPort, enableTlsPort);
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
   *
   */
  @JsonCreator
  public DruidNode(
      @JacksonInject @Named("serviceName") @JsonProperty("service") String serviceName,
      @JsonProperty("host") String host,
      @JsonProperty("bindOnHost") boolean bindOnHost,
      @JsonProperty("plaintextPort") Integer plaintextPort,
      @JacksonInject @Named("servicePort") @JsonProperty("port") Integer port,
      @JacksonInject @Named("tlsServicePort") @JsonProperty("tlsPort") Integer tlsPort,
      @JsonProperty("enablePlaintextPort") Boolean enablePlaintextPort,
      @JsonProperty("enableTlsPort") boolean enableTlsPort
  )
  {
    init(
        serviceName,
        host,
        bindOnHost,
        plaintextPort != null ? plaintextPort : port,
        tlsPort,
        enablePlaintextPort == null ? true : enablePlaintextPort.booleanValue(),
        enableTlsPort
    );
  }

  private void init(String serviceName, String host, boolean bindOnHost, Integer plainTextPort, Integer tlsPort, boolean enablePlaintextPort, boolean enableTlsPort)
  {
    Preconditions.checkNotNull(serviceName);

    if (!enableTlsPort && !enablePlaintextPort) {
      throw new IAE("At least one of the druid.enablePlaintextPort or druid.enableTlsPort needs to be true.");
    }

    this.enablePlaintextPort = enablePlaintextPort;
    this.enableTlsPort = enableTlsPort;

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

    if (enablePlaintextPort && enableTlsPort && ((plainTextPort == null || tlsPort == null)
                                                               || plainTextPort.equals(tlsPort))) {
      // If both plainTExt and tls are enabled then do not allow plaintextPort to be null or
      throw new IAE("plaintextPort and tlsPort cannot be null or same if both http and https connectors are enabled");
    }
    if (enableTlsPort && (tlsPort == null || tlsPort < 0)) {
      throw new IAE("A valid tlsPort needs to specified when druid.enableTlsPort is set");
    }

    if (enablePlaintextPort) {
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
    if (enableTlsPort) {
      this.tlsPort = tlsPort;
    } else {
      this.tlsPort = -1;
    }

    this.serviceName = serviceName;
    this.host = host;
    this.bindOnHost = bindOnHost;
  }

  public String getServiceName()
  {
    return serviceName;
  }

  public String getHost()
  {
    return host;
  }

  public boolean isBindOnHost()
  {
    return bindOnHost;
  }

  public int getPlaintextPort()
  {
    return plaintextPort;
  }

  public boolean isEnablePlaintextPort()
  {
    return enablePlaintextPort;
  }

  public boolean isEnableTlsPort()
  {
    return enableTlsPort;
  }

  public int getTlsPort()
  {
    return tlsPort;
  }

  public DruidNode withService(String service)
  {
    return new DruidNode(service, host, bindOnHost, plaintextPort, tlsPort, enablePlaintextPort, enableTlsPort);
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
    if (enablePlaintextPort) {
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
    if (enableTlsPort) {
      return HostAndPort.fromParts(host, tlsPort).toString();
    }
    return null;
  }

  public int getPortToUse()
  {
    if (enableTlsPort) {
      return getTlsPort();
    } else {
      return getPlaintextPort();
    }
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
    return port == druidNode.port &&
           bindOnHost == druidNode.bindOnHost &&
           plaintextPort == druidNode.plaintextPort &&
           enablePlaintextPort == druidNode.enablePlaintextPort &&
           tlsPort == druidNode.tlsPort &&
           enableTlsPort == druidNode.enableTlsPort &&
           Objects.equals(serviceName, druidNode.serviceName) &&
           Objects.equals(host, druidNode.host);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(serviceName, host, port, plaintextPort, enablePlaintextPort, tlsPort, enableTlsPort);
  }

  @Override
  public String toString()
  {
    return "DruidNode{" +
           "serviceName='" + serviceName + '\'' +
           ", host='" + host + '\'' +
           ", bindOnHost=" + bindOnHost +
           ", port=" + port +
           ", plaintextPort=" + plaintextPort +
           ", enablePlaintextPort=" + enablePlaintextPort +
           ", tlsPort=" + tlsPort +
           ", enableTlsPort=" + enableTlsPort +
           '}';
  }
}
