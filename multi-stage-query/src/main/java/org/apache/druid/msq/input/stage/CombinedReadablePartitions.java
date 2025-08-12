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
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Combines a set of {@link ReadablePartitions} into a single instance.
 */
public class CombinedReadablePartitions implements ReadablePartitions
{
  private final List<ReadablePartitions> readablePartitions;

  @JsonCreator
  public CombinedReadablePartitions(@JsonProperty("children") final List<ReadablePartitions> readablePartitions)
  {
    this.readablePartitions = readablePartitions;
  }

  @Override
  public List<ReadablePartitions> split(final int maxNumSplits)
  {
    // Split each item of "readablePartitions", then combine all the 0s, all the 1s, all the 2s, etc.
    final List<List<ReadablePartitions>> splits =
        readablePartitions.stream().map(rp -> rp.split(maxNumSplits)).collect(Collectors.toList());

    final List<ReadablePartitions> retVal = new ArrayList<>();

    for (int i = 0; i < maxNumSplits; i++) {
      final List<ReadablePartitions> combo = new ArrayList<>();

      for (int j = 0; j < readablePartitions.size(); j++) {
        if (splits.get(j).size() > i) {
          combo.add(splits.get(j).get(i));
        }
      }

      if (combo.size() == 1) {
        retVal.add(Iterables.getOnlyElement(combo));
      } else {
        retVal.add(new CombinedReadablePartitions(combo));
      }
    }

    return retVal;
  }

  @Override
  public Iterator<ReadablePartition> iterator()
  {
    return Iterables.concat(readablePartitions).iterator();
  }

  @JsonProperty("children")
  List<ReadablePartitions> getReadablePartitions()
  {
    return readablePartitions;
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
    CombinedReadablePartitions that = (CombinedReadablePartitions) o;
    return Objects.equals(readablePartitions, that.readablePartitions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(readablePartitions);
  }

  @Override
  public String toString()
  {
    return "CombinedReadablePartitions{" +
           "readablePartitions=" + readablePartitions +
           '}';
  }
}
