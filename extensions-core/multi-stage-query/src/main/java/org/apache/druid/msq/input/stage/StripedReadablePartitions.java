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
 * Set of partitions striped across {@code numWorkers} workers: each worker contains a "stripe" of each partition.
 */
public class StripedReadablePartitions implements ReadablePartitions
{
  private final int stageNumber;
  private final int numWorkers;
  private final IntSortedSet partitionNumbers;

  StripedReadablePartitions(final int stageNumber, final int numWorkers, final IntSortedSet partitionNumbers)
  {
    this.stageNumber = stageNumber;
    this.numWorkers = numWorkers;
    this.partitionNumbers = partitionNumbers;
  }

  @JsonCreator
  private StripedReadablePartitions(
      @JsonProperty("stageNumber") final int stageNumber,
      @JsonProperty("numWorkers") final int numWorkers,
      @JsonProperty("partitionNumbers") final Set<Integer> partitionNumbers
  )
  {
    this(stageNumber, numWorkers, new IntAVLTreeSet(partitionNumbers));
  }

  @Override
  public Iterator<ReadablePartition> iterator()
  {
    return Iterators.transform(
        partitionNumbers.iterator(),
        partitionNumber -> ReadablePartition.striped(stageNumber, numWorkers, partitionNumber)
    );
  }

  @Override
  public List<ReadablePartitions> split(final int maxNumSplits)
  {
    final List<ReadablePartitions> retVal = new ArrayList<>();

    for (List<Integer> entries : SlicerUtils.makeSlices(partitionNumbers.iterator(), maxNumSplits)) {
      if (!entries.isEmpty()) {
        retVal.add(new StripedReadablePartitions(stageNumber, numWorkers, new IntAVLTreeSet(entries)));
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
  int getNumWorkers()
  {
    return numWorkers;
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
    StripedReadablePartitions that = (StripedReadablePartitions) o;
    return stageNumber == that.stageNumber
           && numWorkers == that.numWorkers
           && Objects.equals(partitionNumbers, that.partitionNumbers);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageNumber, numWorkers, partitionNumbers);
  }

  @Override
  public String toString()
  {
    return "StripedReadablePartitions{" +
           "stageNumber=" + stageNumber +
           ", numWorkers=" + numWorkers +
           ", partitionNumbers=" + partitionNumbers +
           '}';
  }
}
