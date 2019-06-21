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

package org.apache.druid.query.aggregation.datasketches;

import com.google.common.util.concurrent.Striped;
import java.util.concurrent.locks.ReadWriteLock;

public class StripedLockHelper
{

  /** for locking per buffer position (power of 2 to make index computation faster) */
  private static final int NUM_STRIPES = 64;

  public static Striped<ReadWriteLock> getReadWriteLock()
  {
    return Striped.readWriteLock(NUM_STRIPES);
  }

  /**
   * compute lock index to avoid boxing in Striped.get() call
   * @param position
   * @return index
   */
  public static int lockIndex(final int position)
  {
    return smear(position) % NUM_STRIPES;
  }

  /**
   * see https://github.com/google/guava/blob/master/guava/src/com/google/common/util/concurrent/Striped.java#L536-L548
   * @param hashCode
   * @return smeared hashCode
   */
  private static int smear(int hashCode)
  {
    hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
    return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
  }

}
