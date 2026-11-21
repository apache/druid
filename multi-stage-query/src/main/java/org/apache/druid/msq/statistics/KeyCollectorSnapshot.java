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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.msq.statistics.serde.KeyCollectorSnapshotSerializer;

/**
 * Marker interface for deserialization.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "collectorType")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = DelegateOrMinKeyCollectorSnapshot.TYPE, value = DelegateOrMinKeyCollectorSnapshot.class),
    @JsonSubTypes.Type(name = QuantilesSketchKeyCollectorSnapshot.TYPE, value = QuantilesSketchKeyCollectorSnapshot.class),
    @JsonSubTypes.Type(name = DistinctKeySnapshot.TYPE, value = DistinctKeySnapshot.class),
})
public interface KeyCollectorSnapshot
{
  KeyCollectorSnapshotSerializer getSerializer();

  /**
   * Returns how many times the collector this snapshot was taken from had halved its set of retained keys, or 0 if
   * this kind of snapshot is not downsampled by halving. See {@link DistinctKeyCollector}.
   */
  default int spaceReductionFactorOrZero()
  {
    return 0;
  }

  /**
   * Returns a snapshot holding only the keys that a collector at the given factor would retain. A collector that
   * merges the result ends up with the same retained keys as one that merged this snapshot, provided its own factor
   * reaches at least "spaceReductionFactor". Returns this snapshot if it doesn't hold more keys than that.
   *
   * @param spaceReductionFactor factor to downsample to
   */
  default KeyCollectorSnapshot downsampleTo(final int spaceReductionFactor)
  {
    return this;
  }
}
