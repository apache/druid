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

package org.apache.druid.frame.key;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.IAE;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Holder object for a set of {@link ClusterByPartition}. There are no preconditions put upon the partitions, except
 * that there is at least one of them.
 *
 * In particular, they are not required to abut each other or to be non-overlapping. Use {@link #allAbutting()} to
 * check if this particular list of partitions is in fact all abutting (and, therefore, also non-overlapping).
 */
public class ClusterByPartitions implements Iterable<ClusterByPartition>
{
  private static final ClusterByPartitions ONE_UNIVERSAL_PARTITION =
      new ClusterByPartitions(Collections.singletonList(new ClusterByPartition(null, null)));

  private final List<ClusterByPartition> ranges;

  @JsonCreator
  public ClusterByPartitions(final List<ClusterByPartition> ranges)
  {
    if (ranges.isEmpty()) {
      throw new IAE("Must provide at least one range");
    }

    this.ranges = ranges;
  }

  public static ClusterByPartitions oneUniversalPartition()
  {
    return ONE_UNIVERSAL_PARTITION;
  }

  /**
   * Whether this list of partitions is all abutting, meaning: each partition's start is equal to the previous
   * partition's end.
   *
   * Note that the start of the first partition, and the end of the last partition, may or may not be unbounded.
   * So this list of partitions may not cover the entire space even if they are all abutting.
   */
  public boolean allAbutting()
  {
    if (ranges.isEmpty()) {
      return true;
    }

    // Walk through all ranges and make sure they're adjacent.
    RowKey current = ranges.get(0).getEnd();

    for (int i = 1; i < ranges.size(); i++) {
      if (current == null || !current.equals(ranges.get(i).getStart())) {
        return false;
      }

      current = ranges.get(i).getEnd();
    }

    return true;
  }

  public ClusterByPartition get(final int i)
  {
    return ranges.get(i);
  }

  public int size()
  {
    return ranges.size();
  }

  @JsonValue
  public List<ClusterByPartition> ranges()
  {
    return ranges;
  }

  @Override
  public Iterator<ClusterByPartition> iterator()
  {
    return ranges.iterator();
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
    ClusterByPartitions that = (ClusterByPartitions) o;
    return Objects.equals(ranges, that.ranges);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(ranges);
  }

  @Override
  public String toString()
  {
    return "ClusterByPartitions{" +
           "ranges=" + ranges +
           '}';
  }
}
