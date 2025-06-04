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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.rpc.ServiceLocation;

import java.util.Objects;

/**
 * Immutable class which represents the status of a dynamic configuration sync with a specific broker.
 */
public class BrokerSyncStatus
{
  private final String host;
  private final int port;
  private final long lastSyncTimestampMillis;

  @JsonCreator
  public BrokerSyncStatus(
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("lastSyncTimestampMillis") long lastSyncTimestampMillis
  )
  {
    this.host = host;
    this.port = port;
    this.lastSyncTimestampMillis = lastSyncTimestampMillis;
  }

  public BrokerSyncStatus(ServiceLocation broker, long lastSyncTimestampMillis)
  {
    this.host = broker.getHost();
    this.port = broker.getTlsPort() > 0 ? broker.getTlsPort() : broker.getPlaintextPort();
    this.lastSyncTimestampMillis = lastSyncTimestampMillis;
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
  public long getLastSyncTimestampMillis()
  {
    return lastSyncTimestampMillis;
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
    BrokerSyncStatus that = (BrokerSyncStatus) o;
    return port == that.port && Objects.equals(host, that.host);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(host, port);
  }

  @Override
  public String toString()
  {
    return "BrokerSyncStatus{" +
           "host='" + host + '\'' +
           ", port=" + port +
           ", lastSyncTimestampMillis=" + lastSyncTimestampMillis +
           '}';
  }
}
