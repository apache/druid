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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.druid.timeline.DataSegment;

import java.util.Iterator;

/**
 * A {@link StorageLocation} selector strategy that selects a segment cache location in a round-robin fashion each time
 * among the available storage locations.
 */
public class RoundRobinStorageLocationSelectorStrategy implements StorageLocationSelectorStrategy
{

  private ImmutableList<StorageLocation> storageLocations;
  private Iterator<StorageLocation> cyclicIterator;

  @Override
  public void setStorageLocations(ImmutableList<StorageLocation> storageLocations) {
    this.storageLocations = storageLocations;
    // cyclicIterator remembers the marker internally
    cyclicIterator = Iterators.cycle(storageLocations);
  }

  @Override
  public StorageLocation select(DataSegment dataSegment, String storageDirStr) {

    StorageLocation bestLocation = null;

    int numLocationsToTry = storageLocations.size();

    while (cyclicIterator.hasNext() && numLocationsToTry > 0) {

      StorageLocation loc = cyclicIterator.next();

      numLocationsToTry--;

      if (null != loc.reserve(storageDirStr, dataSegment)) {
        bestLocation = loc;
        break;
      }
    }

    return bestLocation;
  }
}
