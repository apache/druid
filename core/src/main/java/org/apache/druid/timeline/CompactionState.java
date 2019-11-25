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

package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.partitions.PartitionsSpec;

import java.util.Map;
import java.util.Objects;

/**
 * This class describes what compaction task spec was used to create a given segment.
 * The compaction task is a task that reads Druid segments and overwrites them with new ones. Since this task always
 * reads segments in the same order, the same task spec will always create the same set of segments
 * (not same segment ID, but same content).
 *
 * Note that this class doesn't include all fields in the compaction task spec. Only the configurations that can
 * affect the content of segment should be included.
 *
 * @see DataSegment#lastCompactionState
 */
public class CompactionState
{
  private final PartitionsSpec partitionsSpec;
  // org.apache.druid.segment.IndexSpec cannot be used here because it's in the 'processing' module which
  // has a dependency on the 'core' module where this class is.
  private final Map<String, Object> indexSpec;

  @JsonCreator
  public CompactionState(
      @JsonProperty("partitionsSpec") PartitionsSpec partitionsSpec,
      @JsonProperty("indexSpec") Map<String, Object> indexSpec
  )
  {
    this.partitionsSpec = partitionsSpec;
    this.indexSpec = indexSpec;
  }

  @JsonProperty
  public PartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @JsonProperty
  public Map<String, Object> getIndexSpec()
  {
    return indexSpec;
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
    CompactionState that = (CompactionState) o;
    return Objects.equals(partitionsSpec, that.partitionsSpec) &&
           Objects.equals(indexSpec, that.indexSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionsSpec, indexSpec);
  }

  @Override
  public String toString()
  {
    return "CompactionState{" +
           "partitionsSpec=" + partitionsSpec +
           ", indexSpec=" + indexSpec +
           '}';
  }
}
