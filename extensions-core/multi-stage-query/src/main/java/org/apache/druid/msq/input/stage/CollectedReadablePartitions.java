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

package org.apache.druid.msq.input.stage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterators;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import org.apache.druid.msq.input.SlicerUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Set of partitions that have been collected onto specific workers: each partition is on exactly one worker.
 */
public class CollectedReadablePartitions implements ReadablePartitions
{
  private final int stageNumber;
  private final Int2IntSortedMap partitionToWorkerMap;

  CollectedReadablePartitions(final int stageNumber, final Int2IntSortedMap partitionToWorkerMap)
  {
    this.stageNumber = stageNumber;
    this.partitionToWorkerMap = partitionToWorkerMap;
  }

  @JsonCreator
  private CollectedReadablePartitions(
      @JsonProperty("stageNumber") final int stageNumber,
      @JsonProperty("partitionToWorkerMap") final Map<Integer, Integer> partitionToWorkerMap
  )
  {
    this(stageNumber, new Int2IntAVLTreeMap(partitionToWorkerMap));
  }

  @Override
  public Iterator<ReadablePartition> iterator()
  {
    return Iterators.transform(
        partitionToWorkerMap.int2IntEntrySet().iterator(),
        entry -> ReadablePartition.collected(stageNumber, entry.getIntValue(), entry.getIntKey())
    );
  }

  @Override
  public List<ReadablePartitions> split(int maxNumSplits)
  {
    return SlicerUtils.makeSlices(partitionToWorkerMap.int2IntEntrySet().iterator(), maxNumSplits)
                      .stream()
                      .map(
                          entries -> {
                            final Int2IntSortedMap map = new Int2IntAVLTreeMap();

                            for (final Int2IntMap.Entry entry : entries) {
                              map.put(entry.getIntKey(), entry.getIntValue());
                            }

                            return new CollectedReadablePartitions(stageNumber, map);
                          }
                      )
                      .collect(Collectors.toList());
  }

  @JsonProperty
  int getStageNumber()
  {
    return stageNumber;
  }

  @JsonProperty
  Int2IntSortedMap getPartitionToWorkerMap()
  {
    return partitionToWorkerMap;
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
    CollectedReadablePartitions that = (CollectedReadablePartitions) o;
    return stageNumber == that.stageNumber && Objects.equals(partitionToWorkerMap, that.partitionToWorkerMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageNumber, partitionToWorkerMap);
  }

  @Override
  public String toString()
  {
    return "CollectedReadablePartitions{" +
           "stageNumber=" + stageNumber +
           ", partitionToWorkerMap=" + partitionToWorkerMap +
           '}';
  }
}
