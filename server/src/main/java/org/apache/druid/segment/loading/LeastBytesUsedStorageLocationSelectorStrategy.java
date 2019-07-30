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
import com.google.common.collect.Ordering;
import org.apache.druid.timeline.DataSegment;

import java.util.Comparator;
import java.util.Iterator;

/**
 * A {@link StorageLocation} selector strategy that selects a segment cache location that is least filled each time
 * among the available storage locations.
 */
public class LeastBytesUsedStorageLocationSelectorStrategy implements StorageLocationSelectorStrategy
{
  private static final Comparator<StorageLocation> COMPARATOR = Comparator
      .comparingLong(StorageLocation::currSizeBytes);

  private ImmutableList<StorageLocation> storageLocations;

  @Override
  public Iterator<StorageLocation> getLocations(DataSegment dataSegment, String storageDirStr)
  {
    return Ordering.from(COMPARATOR).sortedCopy(this.storageLocations).iterator();
  }

  @Override
  public void setStorageLocations(ImmutableList<StorageLocation> storageLocations)
  {
    this.storageLocations = storageLocations;
  }
}
