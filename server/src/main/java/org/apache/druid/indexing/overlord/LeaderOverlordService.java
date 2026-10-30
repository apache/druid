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

package org.apache.druid.indexing.overlord;

/**
 * Represents a stateful service running on the leader Overlord. A single service
 * is responsible for a particular aspect of Overlord operations such as
 * managing segment allocation, launching compaction jobs, etc.
 * <p>
 * This is distinct from an {@code OverlordDuty} which contains relatively simpler
 * operations that run on the {@code OverlordDutyExecutor} itself. On the other
 * hand, a {@link LeaderOverlordService} may have its own dedicated thread pool.
 */
public interface LeaderOverlordService
{
  /**
   * Called when this Overlord becomes leader so that the service can initialize
   * state and start its scheduled management.
   * <p>
   * The order in which this method is called for different instances of
   * {@link LeaderOverlordService} is non-deterministic. However, it is always
   * called after initializing all other Overlord dependencies.
   * <p>
   * This method blocks the Overlord lifecycle thread and thus must not be used
   * to perform long computations.
   */
  void becomeLeader();

  /**
   * Called when this Overlord is not leader anymore so that the service can
   * interrupt any ongoing tasks and clean up state.
   * <p>
   * The order in which this method is called for different instances of
   * {@link LeaderOverlordService} is non-deterministic. However, it is always
   * called after initializing all other Overlord dependencies.
   * <p>
   * This method blocks the Overlord lifecycle thread and thus must not be used
   * to perform long computations.
   */
  void stopBeingLeader();
}
