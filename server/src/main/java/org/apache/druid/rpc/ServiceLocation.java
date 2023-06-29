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
import org.apache.druid.server.DruidNode;

import java.util.Objects;

/**
 * Represents a service location at a particular point in time.
 */
public class ServiceLocation
{
  private final String host;
  private final int plaintextPort;
  private final int tlsPort;
  private final String basePath;

  public ServiceLocation(final String host, final int plaintextPort, final int tlsPort, final String basePath)
  {
    this.host = Preconditions.checkNotNull(host, "host");
    this.plaintextPort = plaintextPort;
    this.tlsPort = tlsPort;
    this.basePath = Preconditions.checkNotNull(basePath, "basePath");
  }

  public static ServiceLocation fromDruidNode(final DruidNode druidNode)
  {
    return new ServiceLocation(druidNode.getHost(), druidNode.getPlaintextPort(), druidNode.getTlsPort(), "");
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
