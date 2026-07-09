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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.DruidException;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Streaming analog of the batch/compaction {@code partitionsSpec}. Configured in the streaming tuning config, it
 * declares the dimensions whose observed values each published segment should record in a
 * {@link org.apache.druid.timeline.partition.DimensionValueSetShardSpec} so the broker can prune segments at query time
 * without waiting for compaction. Unlike batch partitioning this does not route rows into shards; it only annotates
 * segments with the values they happened to ingest.
 *
 * <p>Use low-to-medium cardinality dimensions; the {@code partitionDimensions} here should be kept in sync with the
 * {@code partitionDimensions} of the compaction config for the same datasource.
 */
public class StreamingPartitionsSpec
{
  private final List<String> partitionDimensions;
  @Nullable
  private final Integer maxValuesPerDimension;

  @JsonCreator
  public StreamingPartitionsSpec(
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,
      @JsonProperty("maxValuesPerDimension") @Nullable Integer maxValuesPerDimension
  )
  {
    this.partitionDimensions = partitionDimensions == null ? Collections.emptyList() : partitionDimensions;
    if (maxValuesPerDimension != null && maxValuesPerDimension <= 0) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build("maxValuesPerDimension must be > 0, got [%d]", maxValuesPerDimension);
    }
    this.maxValuesPerDimension = maxValuesPerDimension;
  }

  public StreamingPartitionsSpec(@Nullable List<String> partitionDimensions)
  {
    this(partitionDimensions, null);
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getMaxValuesPerDimension()
  {
    return maxValuesPerDimension;
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
    StreamingPartitionsSpec that = (StreamingPartitionsSpec) o;
    return Objects.equals(partitionDimensions, that.partitionDimensions)
           && Objects.equals(maxValuesPerDimension, that.maxValuesPerDimension);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionDimensions, maxValuesPerDimension);
  }

  @Override
  public String toString()
  {
    return "StreamingPartitionsSpec{"
           + "partitionDimensions=" + partitionDimensions
           + ", maxValuesPerDimension=" + maxValuesPerDimension
           + '}';
  }

  public static List<String> getPartitionDimensionsOrEmpty(@Nullable StreamingPartitionsSpec spec)
  {
    return spec == null ? List.of() : spec.getPartitionDimensions();
  }

  @Nullable
  public static Integer getMaxValuesPerDimensionOrNull(@Nullable StreamingPartitionsSpec spec)
  {
    return spec == null ? null : spec.getMaxValuesPerDimension();
  }
}
