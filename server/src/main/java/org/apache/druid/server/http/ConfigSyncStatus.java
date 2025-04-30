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
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;

import java.util.Objects;
import java.util.Set;

/**
 * Immutable class which contains the current set of Brokers which have been synced with the latest
 * {@link CoordinatorDynamicConfig}.
 */
public class ConfigSyncStatus
{
  private final Set<BrokerSyncStatus> syncedBrokers;

  @JsonCreator
  public ConfigSyncStatus(@JsonProperty("syncedBrokers") Set<BrokerSyncStatus> syncedBrokers)
  {
    this.syncedBrokers = syncedBrokers;
  }

  @JsonProperty
  public Set<BrokerSyncStatus> getSyncedBrokers()
  {
    return syncedBrokers;
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
    ConfigSyncStatus that = (ConfigSyncStatus) o;
    return Objects.equals(syncedBrokers, that.syncedBrokers);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(syncedBrokers);
  }

  @Override
  public String toString()
  {
    return "ConfigSyncStatus{" +
           "syncedBrokers=" + syncedBrokers +
           '}';
  }
}
