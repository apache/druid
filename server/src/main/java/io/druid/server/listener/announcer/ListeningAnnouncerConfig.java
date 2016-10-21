/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.listener.announcer;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.utils.ZKPaths;

/**
 * Even though we provide the mechanism to get zk paths here, we do NOT handle announcing and unannouncing in this module.
 * The reason is that it is not appropriate to force a global announce/unannounce since individual listeners may have
 * different lifecycles.
 */
public class ListeningAnnouncerConfig
{
  @JacksonInject
  private final ZkPathsConfig zkPathsConfig;
  @JsonProperty("listenersPath")
  private String listenersPath = null;

  @Inject
  public ListeningAnnouncerConfig(
      ZkPathsConfig zkPathsConfig
  )
  {
    this.zkPathsConfig = zkPathsConfig;
  }

  @JsonProperty("listenersPath")
  public String getListenersPath()
  {
    return listenersPath == null ? zkPathsConfig.defaultPath("listeners") : listenersPath;
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

    ListeningAnnouncerConfig that = (ListeningAnnouncerConfig) o;

    return !(listenersPath != null ? !listenersPath.equals(that.listenersPath) : that.listenersPath != null);

  }

  @Override
  public int hashCode()
  {
    return listenersPath != null ? listenersPath.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return "ListeningAnnouncerConfig{" +
           "listenersPath='" + getListenersPath() + '\'' +
           '}';
  }

  /**
   * Build a path for the particular named listener. The first implementation of this is used with zookeeper, but
   * there is nothing restricting its use in a more general pathing (example: http endpoint proxy for raft)
   * @param listenerName The key for the listener.
   * @return A path appropriate for use in zookeeper to discover the listeners with the particular listener name
   */
  public String getAnnouncementPath(String listenerName)
  {
    return ZKPaths.makePath(
        getListenersPath(), Preconditions.checkNotNull(
            Strings.emptyToNull(listenerName), "Listener name cannot be null"
        )
    );
  }
}
