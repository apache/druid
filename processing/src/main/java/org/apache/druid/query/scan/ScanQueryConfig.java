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

package org.apache.druid.query.scan;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class ScanQueryConfig
{
  public static final String CTX_KEY_MAX_ROWS_QUEUED_FOR_ORDERING = "maxRowsQueuedForOrdering";
  public static final String CTX_KEY_MAX_SEGMENT_PARTITIONS_FOR_ORDERING = "maxSegmentPartitionsOrderedInMemory";

  @JsonProperty
  private int maxRowsQueuedForOrdering = 100000;

  @JsonProperty
  private int maxSegmentPartitionsOrderedInMemory = 50;

  public int getMaxRowsQueuedForOrdering()
  {
    return maxRowsQueuedForOrdering;
  }

  public int getMaxSegmentPartitionsOrderedInMemory()
  {
    return maxSegmentPartitionsOrderedInMemory;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScanQueryConfig that = (ScanQueryConfig) o;
    return maxRowsQueuedForOrdering == that.maxRowsQueuedForOrdering
           && maxSegmentPartitionsOrderedInMemory == that.maxSegmentPartitionsOrderedInMemory;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(maxRowsQueuedForOrdering, maxSegmentPartitionsOrderedInMemory);
  }

  @Override
  public String toString()
  {
    return "ScanQueryConfig{" +
           "maxRowsQueuedForOrdering=" + maxRowsQueuedForOrdering +
           ", maxSegmentPartitionsOrderedInMemory=" + maxSegmentPartitionsOrderedInMemory +
           '}';
  }
}
