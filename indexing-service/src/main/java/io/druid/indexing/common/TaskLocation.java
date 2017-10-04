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

package io.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class TaskLocation
{
  private static final TaskLocation UNKNOWN = new TaskLocation(null, -1, -1);

  private final String host;
  private final int port;
  private final int tlsPort;

  public static TaskLocation create(String host, int port, int tlsPort)
  {
    return new TaskLocation(host, port, tlsPort);
  }

  public static TaskLocation unknown()
  {
    return TaskLocation.UNKNOWN;
  }

  @JsonCreator
  public TaskLocation(
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("tlsPort") int tlsPort
  )
  {
    this.host = host;
    this.port = port;
    this.tlsPort = tlsPort;
  }

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

    return port == that.port && tlsPort == that.tlsPort &&
           Objects.equals(host, that.host);
  }

  @Override
  public int hashCode()
  {
    int result = host.hashCode();
    result = 31 * result + port;
    result = 31 * result + tlsPort;
    return result;
  }

  @Override
  public String toString()
  {
    return "TaskLocation{" +
           "host='" + host + '\'' +
           ", port=" + port +
           ", tlsPort=" + tlsPort +
           '}';
  }
}
