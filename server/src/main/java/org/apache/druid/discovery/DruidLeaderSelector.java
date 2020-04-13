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

import javax.annotation.Nullable;

/**
 * Interface for supporting Overlord and Coordinator Leader Elections in TaskMaster and DruidCoordinator
 * which expect appropriate implementation available in guice annotated with @IndexingService and @Coordinator
 * respectively.
 *
 * Usage is as follows:
 * On lifecycle start:
 *  druidLeaderSelector.registerListener(myListener);
 *
 * On lifecycle stop:
 *  druidLeaderSelector.unregisterListener();
 */
public interface DruidLeaderSelector
{
  /**
   * Get ID of current Leader. Returns NULL if it can't find the leader.
   * Note that it is possible for leadership to change right after this call returns, so caller would get wrong
   * leader.
   */
  @Nullable
  String getCurrentLeader();

  /**
   * Returns true if this node is elected leader from underlying system's point of view. For example if curator
   * is used to implement this then true would be returned when curator believes this node to be the leader.
   * Note that it is possible for leadership to change right after this call returns, so caller would get wrong
   * status.
   */
  boolean isLeader();

  /**
   * Implementation would increment it everytime it becomes leader. This allows users to start a long running
   * task when they become leader and be able to intermittently check that they are still leader from same
   * term when they started. DruidCoordinator class uses it to do intermittent checks and stop the activity
   * as needed.
   */
  int localTerm();

  /**
   * Register the listener for watching leadership notifications. It should only be called once.
   */
  void registerListener(Listener listener);

  /**
   * Unregisters the listener.
   */
  void unregisterListener();

  interface Listener
  {
    /**
     * Notification that this node should start activities to be done by the leader. If this method throws an
     * exception, the implementation should resign leadership in the underlying system such as curator.
     */
    void becomeLeader();

    /**
     * Notification that this node should stop activities which are done by the leader. If this method throws
     * an exception, an alert should be created.
     */
    void stopBeingLeader();
  }
}
