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
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.druid.msq.input.SlicerUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Set of partitions striped across a sparse set of {@code workers}. Each worker contains a "stripe" of each partition.
 *
 * @see StripedReadablePartitions dense version, where workers from [0..N) are all used.
 */
public class SparseStripedReadablePartitions implements ReadablePartitions
{
  private final int stageNumber;
  private final IntSortedSet workers;
  private final IntSortedSet partitionNumbers;

  /**
   * Constructor. Most callers should use {@link ReadablePartitions#striped(int, int, int)} instead, which takes
   * a partition count rather than a set of partition numbers.
   */
  public SparseStripedReadablePartitions(
      final int stageNumber,
      final IntSortedSet workers,
      final IntSortedSet partitionNumbers
  )
  {
    this.stageNumber = stageNumber;
    this.workers = workers;
    this.partitionNumbers = partitionNumbers;
  }

  @JsonCreator
  private SparseStripedReadablePartitions(
      @JsonProperty("stageNumber") final int stageNumber,
      @JsonProperty("workers") final Set<Integer> workers,
      @JsonProperty("partitionNumbers") final Set<Integer> partitionNumbers
  )
  {
    this(stageNumber, new IntAVLTreeSet(workers), new IntAVLTreeSet(partitionNumbers));
  }

  @Override
  public Iterator<ReadablePartition> iterator()
  {
    return Iterators.transform(
        partitionNumbers.iterator(),
        partitionNumber -> ReadablePartition.striped(stageNumber, workers, partitionNumber)
    );
  }

  @Override
  public List<ReadablePartitions> split(final int maxNumSplits)
  {
    final List<ReadablePartitions> retVal = new ArrayList<>();

    for (List<Integer> entries : SlicerUtils.makeSlicesStatic(partitionNumbers.iterator(), maxNumSplits)) {
      if (!entries.isEmpty()) {
        retVal.add(new SparseStripedReadablePartitions(stageNumber, workers, new IntAVLTreeSet(entries)));
      }
    }

    return retVal;
  }

  @JsonProperty
  int getStageNumber()
  {
    return stageNumber;
  }

  @JsonProperty
  IntSortedSet getWorkers()
  {
    return workers;
  }

  @JsonProperty
  IntSortedSet getPartitionNumbers()
  {
    return partitionNumbers;
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
    SparseStripedReadablePartitions that = (SparseStripedReadablePartitions) o;
    return stageNumber == that.stageNumber
           && Objects.equals(workers, that.workers)
           && Objects.equals(partitionNumbers, that.partitionNumbers);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageNumber, workers, partitionNumbers);
  }

  @Override
  public String toString()
  {
    return "StripedReadablePartitions{" +
           "stageNumber=" + stageNumber +
           ", workers=" + workers +
           ", partitionNumbers=" + partitionNumbers +
           '}';
  }
}
