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

package org.apache.druid.msq.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.IAE;

public enum ShuffleKind
{
  /**
   * Put all data in a single partition, with no sorting and no statistics gathering.
   */
  MIX("mix", false, false),

  /**
   * Partition using hash codes, with no sorting.
   *
   * This kind of shuffle supports pipelining: producer and consumer stages can run at the same time.
   */
  HASH("hash", true, false),

  /**
   * Partition using hash codes, with each partition internally sorted.
   *
   * Each worker partitions its outputs according to hash code of the cluster key, and does a local sort of its
   * own outputs.
   *
   * Due to the need to sort outputs, this shuffle mechanism cannot be pipelined. Producer stages must finish before
   * consumer stages can run.
   */
  HASH_LOCAL_SORT("hashLocalSort", true, true),

  /**
   * Partition using a distributed global sort.
   *
   * First, each worker reads its input fully and feeds statistics into a
   * {@link org.apache.druid.msq.statistics.ClusterByStatisticsCollector}. The controller merges those statistics,
   * generating final {@link org.apache.druid.frame.key.ClusterByPartitions}. Then, workers fully sort and partition
   * their outputs along those lines.
   *
   * Consumers (workers in the next stage downstream) do an N-way merge of the already-sorted and already-partitioned
   * output files from each worker.
   *
   * Due to the need to sort outputs, this shuffle mechanism cannot be pipelined. Producer stages must finish before
   * consumer stages can run.
   */
  GLOBAL_SORT("globalSort", false, true);

  private final String name;
  private final boolean hash;
  private final boolean sort;

  ShuffleKind(String name, boolean hash, boolean sort)
  {
    this.name = name;
    this.hash = hash;
    this.sort = sort;
  }

  @JsonCreator
  public static ShuffleKind fromString(final String s)
  {
    for (final ShuffleKind kind : values()) {
      if (kind.toString().equals(s)) {
        return kind;
      }
    }

    throw new IAE("No such shuffleKind[%s]", s);
  }

  /**
   * Whether this shuffle does hash-partitioning.
   */
  public boolean isHash()
  {
    return hash;
  }

  /**
   * Whether this shuffle sorts within partitions. (If true, it may, or may not, also sort globally.)
   */
  public boolean isSort()
  {
    return sort;
  }

  @Override
  @JsonValue
  public String toString()
  {
    return name;
  }
}
