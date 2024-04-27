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

package org.apache.druid.msq.querykit;

import org.apache.druid.msq.kernel.GlobalSortMaxCountShuffleSpec;
import org.apache.druid.msq.kernel.GlobalSortTargetSizeShuffleSpec;
import org.apache.druid.msq.kernel.MixShuffleSpec;

/**
 * Static factory methods for common implementations of {@link ShuffleSpecFactory}.
 */
public class ShuffleSpecFactories
{
  private ShuffleSpecFactories()
  {
    // No instantiation.
  }

  /**
   * Factory that produces a single output partition, which may or may not be sorted.
   */
  public static ShuffleSpecFactory singlePartition()
  {
    return (clusterBy, aggregate) -> {
      if (clusterBy.sortable() && !clusterBy.isEmpty()) {
        return new GlobalSortMaxCountShuffleSpec(clusterBy, 1, aggregate);
      } else {
        return MixShuffleSpec.instance();
      }
    };
  }

  /**
   * Factory that produces a particular number of output partitions.
   */
  public static ShuffleSpecFactory globalSortWithMaxPartitionCount(final int partitions)
  {
    return (clusterBy, aggregate) -> new GlobalSortMaxCountShuffleSpec(clusterBy, partitions, aggregate);
  }

  /**
   * Factory that produces globally sorted partitions of a target size.
   */
  public static ShuffleSpecFactory getGlobalSortWithTargetSize(int targetSize)
  {
    return (clusterBy, aggregate) ->
        new GlobalSortTargetSizeShuffleSpec(
            clusterBy,
            targetSize,
            aggregate
        );
  }
}
