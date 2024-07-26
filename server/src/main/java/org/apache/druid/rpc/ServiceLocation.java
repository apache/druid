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

package org.apache.druid.rpc;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.Objects;

/**
 * Represents a service location at a particular point in time.
 */
public class ServiceLocation
{
  private static final String HTTP_SCHEME = "http";
  private static final String HTTPS_SCHEME = "https";
  private static final Splitter HOST_SPLITTER = Splitter.on(":").limit(2);

  private final String host;
  private final int plaintextPort;
  private final int tlsPort;
  private final String basePath;

  /**
   * Create a service location.
   *
   * @param host          hostname or address
   * @param plaintextPort plaintext port
   * @param tlsPort       TLS port
   * @param basePath      base path; must be encoded and must not include trailing "/". In particular, to use root as
   *                      the base path, pass "" for this parameter.
   */
  public ServiceLocation(final String host, final int plaintextPort, final int tlsPort, final String basePath)
  {
    this.host = Preconditions.checkNotNull(host, "host");
    this.plaintextPort = plaintextPort;
    this.tlsPort = tlsPort;
    this.basePath = Preconditions.checkNotNull(basePath, "basePath");
  }

  /**
   * Create a service location based on a {@link DruidNode}, without a base path.
   */
  public static ServiceLocation fromDruidNode(final DruidNode druidNode)
  {
    return new ServiceLocation(druidNode.getHost(), druidNode.getPlaintextPort(), druidNode.getTlsPort(), "");
  }

  /**
   * Create a service location based on a {@link DruidServerMetadata}.
   *
   * @throws IllegalArgumentException if the server metadata cannot be mapped to a service location.
   */
  public static ServiceLocation fromDruidServerMetadata(final DruidServerMetadata druidServerMetadata)
  {
    final String host = getHostFromString(
        Preconditions.checkNotNull(
            druidServerMetadata.getHost(),
            "Host was null for druid server metadata[%s]",
            druidServerMetadata
        )
    );
    int plaintextPort = getPortFromString(druidServerMetadata.getHostAndPort());
    int tlsPort = getPortFromString(druidServerMetadata.getHostAndTlsPort());
    return new ServiceLocation(host, plaintextPort, tlsPort, "");
  }

  private static String getHostFromString(@NotNull String s)
  {
    Iterator<String> iterator = HOST_SPLITTER.split(s).iterator();
    ImmutableList<String> strings = ImmutableList.copyOf(iterator);
    return strings.get(0);
  }

  private static int getPortFromString(String s)
  {
    if (s == null) {
      return -1;
    }
    Iterator<String> iterator = HOST_SPLITTER.split(s).iterator();
    ImmutableList<String> strings = ImmutableList.copyOf(iterator);
    try {
      return Integer.parseInt(strings.get(1));
    }
    catch (NumberFormatException e) {
      throw new ISE(e, "Unable to parse port out of %s", strings.get(1));
    }
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

  public String getBasePath()
  {
    return basePath;
  }

  public URL toURL(@Nullable final String encodedPathAndQueryString)
  {
    final String scheme;
    final int portToUse;

    if (tlsPort > 0) {
      // Prefer HTTPS if available.
      scheme = HTTPS_SCHEME;
      portToUse = tlsPort;
    } else {
      scheme = HTTP_SCHEME;
      portToUse = plaintextPort;
    }

    try {
      return new URL(
          scheme,
          host,
          portToUse,
          basePath + (encodedPathAndQueryString == null ? "" : encodedPathAndQueryString)
      );
    }
    catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
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
    ServiceLocation that = (ServiceLocation) o;
    return plaintextPort == that.plaintextPort
           && tlsPort == that.tlsPort
           && Objects.equals(host, that.host)
           && Objects.equals(basePath, that.basePath);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(host, plaintextPort, tlsPort, basePath);
  }

  @Override
  public String toString()
  {
    return "ServiceLocation{" +
           "host='" + host + '\'' +
           ", plaintextPort=" + plaintextPort +
           ", tlsPort=" + tlsPort +
           ", basePath='" + basePath + '\'' +
           '}';
  }

}
