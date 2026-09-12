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

package org.apache.druid.query;

/**
 * Exposes queue-depth and busy-thread counters for a processing pool so the metrics monitor can emit
 * {@code segment/scan/pending} and {@code segment/scan/active} without depending on a specific pool implementation.
 *
 * <p>Implemented by both {@link PrioritizedExecutorService} (single pool) and
 * {@link ShardedPrioritizedExecutorService} (a composite of pools whose counters are summed across shards). The
 * metrics monitor checks {@code instanceof ProcessingPoolStats} rather than a concrete class, so any future pool type
 * that reports these two numbers is picked up automatically.
 */
public interface ProcessingPoolStats
{
  /**
   * Number of queued (not-yet-running) tasks. For a composite pool this is summed across all shards.
   */
  int getQueueSize();

  /**
   * Approximate number of tasks currently being run by worker threads. For a composite pool this is summed across
   * all shards.
   */
  int getActiveTasks();
}
