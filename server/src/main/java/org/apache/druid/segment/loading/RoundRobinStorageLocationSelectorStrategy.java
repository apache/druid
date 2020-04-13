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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link StorageLocation} selector strategy that selects a segment cache location in a round-robin fashion each time
 * among the available storage locations. When {@link Iterator#next()} on iterator retuned by
 * {@link RoundRobinStorageLocationSelectorStrategy#getLocations()} is called the locations are returned in a round
 * robin fashion even when multiple threads are in use.
 */
public class RoundRobinStorageLocationSelectorStrategy implements StorageLocationSelectorStrategy
{

  private final List<StorageLocation> storageLocations;
  private final AtomicInteger startIndex = new AtomicInteger(0);

  public RoundRobinStorageLocationSelectorStrategy(List<StorageLocation> storageLocations)
  {
    this.storageLocations = storageLocations;
  }

  @Override
  public Iterator<StorageLocation> getLocations()
  {
    return new Iterator<StorageLocation>() {

      private final int numStorageLocations = storageLocations.size();
      private int remainingIterations = numStorageLocations;
      // Each call to this methods starts with a different startIndex to avoid the same location being picked up over
      // again. See https://github.com/apache/druid/issues/8614.
      private int i = startIndex.getAndUpdate(n -> (n + 1) % numStorageLocations);

      @Override
      public boolean hasNext()
      {
        return remainingIterations > 0;
      }

      @Override
      public StorageLocation next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        remainingIterations--;
        final StorageLocation nextLocation = storageLocations.get(i++);
        if (i == numStorageLocations) {
          i = 0;
        }
        return nextLocation;
      }
    };
  }

}
