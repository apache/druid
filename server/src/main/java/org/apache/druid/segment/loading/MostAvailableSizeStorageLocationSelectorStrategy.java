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

import com.google.common.collect.Ordering;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link StorageLocation} selector strategy that selects a segment cache location that has most free space
 * among the available storage locations.
 */
public class MostAvailableSizeStorageLocationSelectorStrategy implements StorageLocationSelectorStrategy
{
  private static final Ordering<StorageLocation> ORDERING = Ordering.from(Comparator
      .comparingLong(StorageLocation::availableSizeBytes)
      .reversed());

  private List<StorageLocation> storageLocations;

  public MostAvailableSizeStorageLocationSelectorStrategy(List<StorageLocation> storageLocations)
  {
    this.storageLocations = storageLocations;
  }

  @Override
  public Iterator<StorageLocation> getLocations()
  {
    return ORDERING.sortedCopy(this.storageLocations).iterator();
  }

  @Override
  public String toString()
  {
    return "MostAvailableSizeStorageLocationSelectorStrategy{" +
           "storageLocations=" + storageLocations +
           '}';
  }
}
