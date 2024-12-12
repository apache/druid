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

package org.apache.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.net.HostAndPort;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;

public class TaskLocation
{
  private static final TaskLocation UNKNOWN = new TaskLocation(null, -1, -1, null);

  @Nullable
  private final String host;
  private final int port;
  private final int tlsPort;

  @Nullable
  private final String k8sPodName;

  public static TaskLocation create(String host, int port, int tlsPort)
  {
    return new TaskLocation(host, port, tlsPort, null);
  }

  public static TaskLocation create(String host, int port, int tlsPort, boolean isTls)
  {
    return create(host, port, tlsPort, isTls, null);
  }

  public static TaskLocation create(String host, int port, int tlsPort, boolean isTls, @Nullable String k8sPodName)
  {
    return isTls ? new TaskLocation(host, -1, tlsPort, k8sPodName) : new TaskLocation(host, port, -1, k8sPodName);
  }

  public static TaskLocation unknown()
  {
    return TaskLocation.UNKNOWN;
  }

  @JsonCreator
  public TaskLocation(
      @JsonProperty("host") @Nullable String host,
      @JsonProperty("port") int port,
      @JsonProperty("tlsPort") int tlsPort,
      @JsonProperty("k8sPodName") @Nullable String k8sPodName
  )
  {
    this.host = host;
    this.port = port;
    this.tlsPort = tlsPort;
    this.k8sPodName = k8sPodName;
  }

  @Nullable
  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  public int getPort()
  {
    return port;
  }

  @JsonProperty
  public int getTlsPort()
  {
    return tlsPort;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty
  public String getK8sPodName()
  {
    return k8sPodName;
  }

  @JsonIgnore
  @Nullable
  public String getLocation()
  {
    if (k8sPodName != null) {
      return k8sPodName;
    } else if (host == null) {
      return null;
    } else {
      final int thePort;
      if (tlsPort >= 0) {
        thePort = tlsPort;
      } else {
        thePort = port;
      }
      return HostAndPort.fromParts(host, thePort).toString();
    }
  }

  public URL makeURL(final String encodedPathAndQueryString) throws MalformedURLException
  {
    final String scheme;
    final int portToUse;

    if (tlsPort > 0) {
      scheme = "https";
      portToUse = tlsPort;
    } else {
      scheme = "http";
      portToUse = port;
    }

    if (!encodedPathAndQueryString.startsWith("/")) {
      throw new IAE("Path must start with '/'");
    }

    // Use URL constructor, not URI, since the path is already encoded.
    return new URL(scheme, host, portToUse, encodedPathAndQueryString);
  }

  @Override
  public String toString()
  {
    return "TaskLocation{" +
           "host='" + host + '\'' +
           ", port=" + port +
           ", tlsPort=" + tlsPort +
           ", k8sPodName=" + k8sPodName +
           '}';
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
    TaskLocation that = (TaskLocation) o;
    return port == that.port && tlsPort == that.tlsPort && Objects.equals(host, that.host) && Objects.equals(k8sPodName, that.k8sPodName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(host, port, tlsPort, k8sPodName);
  }
}
