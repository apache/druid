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

import java.util.Iterator;

/**
 * This interface describes the storage location selection strategy which is responsible for ordering the
 * available multiple {@link StorageLocation}s for optimal segment distribution.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "tier", defaultImpl = LeastBytesUsedStorageLocationSelectorStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "leastBytesUsed", value = LeastBytesUsedStorageLocationSelectorStrategy.class),
    @JsonSubTypes.Type(name = "roundRobin", value = RoundRobinStorageLocationSelectorStrategy.class)
})
public interface StorageLocationSelectorStrategy
{
  /**
   *  Finds the best ordering of the {@link StorageLocation}s to load the given {@link DataSegment} into according to
   *  the location selector strategy. This method returns an iterator instead of a single best location. The
   *  caller is responsible for iterating over the locations and calling {@link StorageLocation}'s reserve() method.
   *  This is because a single location may be problematic like failed disk or might become unwritable for whatever
   *  reasons.
   *
   * @return An iterator of {@link StorageLocation}s from which the callers can iterate and pick a location.
   */
  Iterator<StorageLocation> getLocations(DataSegment dataSegment, String storageDirStr);

  /**
   * Sets the storage locations list  with the supplied storage locations.
   * @param storageLocations storage locations list to be used.
   */
  void setStorageLocations(ImmutableList<StorageLocation> storageLocations);
}
