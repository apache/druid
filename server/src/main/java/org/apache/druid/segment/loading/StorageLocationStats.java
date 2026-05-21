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

package org.apache.druid.segment.loading;

public interface StorageLocationStats
{
  /**
   * Current number of bytes stored at the time this measurement collection was created.
   */
  long getUsedBytes();

  /**
   * Number of load operations that were started (space reserved) during the measurement period. This is incremented
   * when the load begins, regardless of whether it ultimately completes.
   */
  long getLoadBeginCount();

  /**
   * Number of bytes for which a load was started (space reserved) during the measurement period. This is incremented
   * when the load begins, regardless of whether it ultimately completes.
   */
  long getLoadBeginBytes();

  /**
   * Number of load operations that finished (segment downloaded, deserialized, and made queryable) during the
   * measurement period. This may be lower than {@link #getLoadBeginCount()} when mounts fail or are aborted.
   */
  long getLoadCount();

  /**
   * Number of bytes for load operations that finished during the measurement period.
   */
  long getLoadBytes();

  /**
   * Number of drop operations during the measurement period
   */
  long getDropCount();

  /**
   * Number of bytes dropped during the measurement period
   */
  long getDropBytes();
}
