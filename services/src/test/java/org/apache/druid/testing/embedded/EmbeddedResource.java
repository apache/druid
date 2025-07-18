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

package org.apache.druid.testing.embedded;

/**
 * Represents any resource outside Druid used by an {@link EmbeddedDruidCluster}.
 * Cluster start and stop triggers {@link #start()} and {@link #stop()} on the
 * resource respectively. Resources are started in the same order in which they
 * are added to a cluster and stopped in the reverse order.
 */
public interface EmbeddedResource
{

  /**
   * Starts this resource. Implementations of this method should clean up any
   * previous state as it may be called multiple times on a single instance of
   * {@link EmbeddedResource}.
   */
  void start() throws Exception;

  /**
   * Cleans up this resource.
   */
  void stop() throws Exception;


  /**
   * Called before {@link #start()} with a pointer to the current cluster. This is primarily useful for any
   * final initialization of {@link EmbeddedDruidServer} that need to configure themselves based on some
   * shared resources which have already been started, such as {@link TestFolder}.
   */
  default void beforeStart(EmbeddedDruidCluster cluster)
  {
    // do nothing by default
  }

  /**
   * Called after {@link #start()} with a pointer to the current cluster. This is intended for use by resources
   * that are started before any Druid services, and that need to configure the Druid services in some way.
   */
  default void onStarted(EmbeddedDruidCluster cluster)
  {
    // Do nothing by default.
  }
}
