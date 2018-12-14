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

package org.apache.druid.discovery;

import java.util.Collection;

/**
 * Interface for discovering Druid Nodes announced by DruidNodeAnnouncer.
 */
public interface DruidNodeDiscovery
{
  Collection<DiscoveryDruidNode> getAllNodes();
  void registerListener(Listener listener);

  /**
   * Listener for watching nodes in a DruidNodeDiscovery instance obtained via DruidNodeDiscoveryProvider.getXXX().
   * DruidNodeDiscovery implementation should assume that Listener is not threadsafe and never call methods in
   * Listener concurrently.
   *
   * Implementation of Listener must ensure to not do any time consuming work or block in any of the methods.
   */
  interface Listener
  {
    void nodesAdded(Collection<DiscoveryDruidNode> nodes);

    void nodesRemoved(Collection<DiscoveryDruidNode> nodes);

    /**
     * Called once when the underlying cache in the DruidNodeDiscovery implementation has been initialized.
     */
    default void nodeViewInitialized()
    {
      // do nothing
    }
  }
}
