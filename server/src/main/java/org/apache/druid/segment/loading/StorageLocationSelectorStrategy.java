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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import org.apache.druid.timeline.DataSegment;

/**
 * Interface which represents a strategy to select a {@link StorageLocation} from available segment cache locations
 * for segment distribution.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "tier", defaultImpl = LeastBytesUsedStorageLocationSelectorStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "leastBytesUsed", value = LeastBytesUsedStorageLocationSelectorStrategy.class),
    @JsonSubTypes.Type(name = "roundRobin", value = RoundRobinStorageLocationSelectorStrategy.class)
})
public interface StorageLocationSelectorStrategy
{
  /**
   *  Find the best {@link StorageLocation} to load the given {@link DataSegment} into according to the location selector strategy.
   *
   * @return The storage location to load the given segment into or null if no location has the capacity to store the given segment.
   */
  StorageLocation select(DataSegment dataSegment, String storageDirStr);

  /**
   * Sets the storage locations list  with the supplied storage locations.
   * @param storageLocations storage locations list to be used.
   */
  void setStorageLocations(ImmutableList<StorageLocation> storageLocations);
}
