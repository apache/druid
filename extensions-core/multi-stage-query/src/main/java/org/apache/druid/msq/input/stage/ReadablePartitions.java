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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A set of {@link ReadablePartition} representing outputs in terms of stage-partitions. Used by {@link StageInputSlice}
 * to represent inputs to a particular worker in a particular stage.
 *
 * Each implementation of this interface represents a different way that partitions can be distributed.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "collected", value = CollectedReadablePartitions.class),
    @JsonSubTypes.Type(name = "striped", value = StripedReadablePartitions.class),
    @JsonSubTypes.Type(name = "combined", value = CombinedReadablePartitions.class)
})
public interface ReadablePartitions extends Iterable<ReadablePartition>
{
  /**
   * Splits the current instance into a list of disjoint subsets, up to {@code maxNumSplits}.
   */
  List<ReadablePartitions> split(int maxNumSplits);

  /**
   * Empty set of partitions.
   */
  static ReadablePartitions empty()
  {
    return new CombinedReadablePartitions(Collections.emptyList());
  }

  /**
   * Combines various sets of partitions into a single set.
   */
  static CombinedReadablePartitions combine(List<ReadablePartitions> readablePartitions)
  {
    return new CombinedReadablePartitions(readablePartitions);
  }

  /**
   * Returns a set of {@code numPartitions} partitions striped across {@code numWorkers} workers: each worker contains
   * a "stripe" of each partition.
   */
  static StripedReadablePartitions striped(
      final int stageNumber,
      final int numWorkers,
      final int numPartitions
  )
  {
    final IntAVLTreeSet partitionNumbers = new IntAVLTreeSet();
    for (int i = 0; i < numPartitions; i++) {
      partitionNumbers.add(i);
    }

    return new StripedReadablePartitions(stageNumber, numWorkers, partitionNumbers);
  }

  /**
   * Returns a set of partitions that have been collected onto specific workers: each partition is on exactly
   * one worker.
   */
  static CollectedReadablePartitions collected(
      final int stageNumber,
      final Map<Integer, Integer> partitionToWorkerMap
  )
  {
    if (partitionToWorkerMap instanceof Int2IntSortedMap) {
      return new CollectedReadablePartitions(stageNumber, (Int2IntSortedMap) partitionToWorkerMap);
    } else {
      return new CollectedReadablePartitions(stageNumber, new Int2IntAVLTreeMap(partitionToWorkerMap));
    }
  }
}
