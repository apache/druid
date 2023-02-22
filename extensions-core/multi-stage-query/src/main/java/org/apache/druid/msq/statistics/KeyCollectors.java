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

package org.apache.druid.msq.statistics;

import org.apache.druid.frame.key.ClusterBy;

public class KeyCollectors
{
  private KeyCollectors()
  {
    // No instantiation.
  }

  /**
   * Used by {@link ClusterByStatisticsCollectorImpl#create} and anything else that seeks to have the same behavior.
   */
  public static KeyCollectorFactory<?, ?> makeStandardFactory(
      final ClusterBy clusterBy,
      final boolean aggregate
  )
  {
    final KeyCollectorFactory<?, ?> baseFactory;

    if (aggregate) {
      baseFactory = DistinctKeyCollectorFactory.create(clusterBy);
    } else {
      baseFactory = QuantilesSketchKeyCollectorFactory.create(clusterBy);
    }

    // Wrap in DelegateOrMinKeyCollectorFactory, so we are guaranteed to be able to downsample to a single key. This
    // is important because it allows us to better handle large numbers of small buckets.
    return new DelegateOrMinKeyCollectorFactory<>(clusterBy.keyComparator(), baseFactory);
  }
}
