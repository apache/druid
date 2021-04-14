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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.google.common.util.concurrent.Striped;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * Utility for locking positions in a buffer.
 */
public class StripedBufferLock
{
  /**
   * for locking per buffer position (power of 2 to make index computation faster)
   */
  private static final int NUM_STRIPES = 64;

  private final Striped<ReadWriteLock> lock = Striped.readWriteLock(NUM_STRIPES);

  /**
   * Get the lock corresponding to a particular position.
   */
  public ReadWriteLock getLock(final int position)
  {
    // Compute lock index to avoid boxing in Striped.get() call
    final int lockIndex = smear(position) % NUM_STRIPES;
    return lock.getAt(lockIndex);
  }

  /**
   * see https://github.com/google/guava/blob/master/guava/src/com/google/common/util/concurrent/Striped.java#L536-L548
   *
   * @return smeared value
   */
  private static int smear(int x)
  {
    x ^= (x >>> 20) ^ (x >>> 12);
    return x ^ (x >>> 7) ^ (x >>> 4);
  }
}
