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

package org.apache.druid.server.coordinator;

/**
 */
public interface LoadPeonCallback
{
  /**
   * Ideally, this method is called after the load/drop opertion is successfully done, i.e., the historical node
   * removes the zookeeper node from loadQueue and announces/unannouces the segment. However, this method will
   * also be called in failure scenarios so for implementations of LoadPeonCallback that care about success it
   * is important to take extra measures to ensure that whatever side effects they expect to happen upon success
   * have happened. Coordinator will have a complete and correct view of the cluster in the next run period.
   */
  void execute();
}
