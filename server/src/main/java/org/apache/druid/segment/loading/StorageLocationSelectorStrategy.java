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
import org.apache.druid.timeline.DataSegment;

import java.util.Iterator;

/**
 * This interface describes the storage location selection strategy which is responsible for ordering the
 * available multiple {@link StorageLocation}s for segment distribution.
 *
 * Only a snapshot of the locations is returned here. The implemntations currently do not handle all kinds of
 * concurrency issues and accesses to the underlying storage. Please see
 * https://github.com/apache/druid/pull/8038#discussion_r325520829 of PR https://github
 * .com/apache/druid/pull/8038 for more details.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl =
    LeastBytesUsedStorageLocationSelectorStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "leastBytesUsed", value = LeastBytesUsedStorageLocationSelectorStrategy.class),
    @JsonSubTypes.Type(name = "roundRobin", value = RoundRobinStorageLocationSelectorStrategy.class),
    @JsonSubTypes.Type(name = "random", value = RandomStorageLocationSelectorStrategy.class),
    @JsonSubTypes.Type(name = "mostAvailableSize", value = MostAvailableSizeStorageLocationSelectorStrategy.class)
})
public interface StorageLocationSelectorStrategy
{
  /**
   * Finds the best ordering of the {@link StorageLocation}s to load a {@link DataSegment} according to
   * the location selector strategy. This method returns an iterator instead of a single best location. The
   * caller is responsible for iterating over the locations and calling {@link StorageLocation#reserve}
   * method. This is because a single location may be problematic like failed disk or might become unwritable for
   * whatever reasons.
   *
   * This method can be called by different threads and so should be thread-safe.
   *
   * @return An iterator of {@link StorageLocation}s from which the callers can iterate and pick a location.
   */
  Iterator<StorageLocation> getLocations();
}
