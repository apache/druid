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

package org.apache.druid.server.http;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

public class HostAndPortWithScheme
{
  private String scheme;
  private HostAndPort hostAndPort;

  public HostAndPortWithScheme(String scheme, HostAndPort hostAndPort)
  {
    this.scheme = scheme;
    this.hostAndPort = hostAndPort;
  }

  public static HostAndPortWithScheme fromParts(String scheme, String host, int port)
  {
    return new HostAndPortWithScheme(scheme, HostAndPort.fromParts(host, port));
  }

  public static HostAndPortWithScheme fromString(String hostPortMaybeSchemeString)
  {
    if (hostPortMaybeSchemeString.startsWith("http")) {
      int colonIndex = hostPortMaybeSchemeString.indexOf(':');
      if (colonIndex == -1) {
        throw new IAE("Invalid host with scheme string: [%s]", hostPortMaybeSchemeString);
      }
      return HostAndPortWithScheme.fromString(
          hostPortMaybeSchemeString.substring(0, colonIndex),
          hostPortMaybeSchemeString.substring(colonIndex + 1)
      );
    }
    return HostAndPortWithScheme.fromString("http", hostPortMaybeSchemeString);
  }

  public static HostAndPortWithScheme fromString(String scheme, String hostPortString)
  {
    return new HostAndPortWithScheme(checkAndGetScheme(scheme), HostAndPort.fromString(hostPortString));
  }

  private static String checkAndGetScheme(String scheme)
  {
    String schemeLowerCase = StringUtils.toLowerCase(scheme);
    Preconditions.checkState("http".equals(schemeLowerCase) || "https".equals(schemeLowerCase));
    return schemeLowerCase;
  }

  public String getScheme()
  {
    return scheme;
  }

  public String getHostText()
  {
    return hostAndPort.getHostText();
  }

  public int getPort()
  {
    return hostAndPort.getPort();
  }

  public int getPortOrDefault(int defaultPort)
  {
    return hostAndPort.getPortOrDefault(defaultPort);
  }

  public HostAndPort getHostAndPort()
  {
    return hostAndPort;
  }

  @Override
  public String toString()
  {
    return StringUtils.format("%s:%s", scheme, hostAndPort.toString());
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

    HostAndPortWithScheme that = (HostAndPortWithScheme) o;

    if (!scheme.equals(that.scheme)) {
      return false;
    }
    return hostAndPort.equals(that.hostAndPort);
  }

  @Override
  public int hashCode()
  {
    int result = scheme.hashCode();
    result = 31 * result + hostAndPort.hashCode();
    return result;
  }
}
