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

package org.apache.druid.msq.kernel;

import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;

import java.util.Collections;
import java.util.List;

public class ShuffleSpecs
{
  /**
   * Shuffle spec for a single unsorted partition.
   */
  public static ShuffleSpec singlePartition()
  {
    return sortedSinglePartition(Collections.emptyList());
  }

  /**
   * Shuffle spec for a single sorted partition.
   */
  public static ShuffleSpec sortedSinglePartition(final List<KeyColumn> sortKey)
  {
    if (sortKey.isEmpty()) {
      return MuxShuffleSpec.instance();
    } else {
      return new GlobalSortMaxCountShuffleSpec(new ClusterBy(sortKey, 0), 1, false);
    }
  }

  /**
   * Shuffle spec for hash partitioning.
   */
  public static ShuffleSpec hashPartition(
      final List<KeyColumn> partitionKey,
      final int numPartitions
  )
  {
    return new HashShuffleSpec(new ClusterBy(partitionKey, 0), numPartitions);
  }
}
