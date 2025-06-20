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

package org.apache.druid.testing.simulate;

/**
 * Represents a resource with a lifecycle used in an {@link EmbeddedDruidCluster}.
 * {@link #start()} and {@link #stop()} are called on cluster start and stop
 * respectively. Resources are started in the same order in which they are added
 * to a cluster and stopped in the reverse order.
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
}
