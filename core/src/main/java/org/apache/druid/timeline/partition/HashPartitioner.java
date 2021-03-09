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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.com.google.common.annotations.VisibleForTesting;
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Rows;

import java.util.List;

/**
 * This class is used for hash partitioning during ingestion. The {@link ShardSpecLookup} returned from
 * {@link #createHashLookup} is used to determine what hash bucket the given input row will belong to.
 */
public class HashPartitioner
{
  private final ObjectMapper jsonMapper;
  private final HashPartitionFunction hashPartitionFunction;
  private final List<String> partitionDimensions;
  private final int numBuckets;

  HashPartitioner(
      final ObjectMapper jsonMapper,
      final HashPartitionFunction hashPartitionFunction,
      final List<String> partitionDimensions,
      final int numBuckets
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.hashPartitionFunction = Preconditions.checkNotNull(hashPartitionFunction, "hashPartitionFunction");
    this.partitionDimensions = Preconditions.checkNotNull(partitionDimensions, "partitionDimensions");
    this.numBuckets = numBuckets;
  }

  ShardSpecLookup createHashLookup(final List<? extends ShardSpec> shardSpecs)
  {
    Preconditions.checkNotNull(hashPartitionFunction, "hashPartitionFunction");
    return (long timestamp, InputRow row) -> {
      int index = hash(timestamp, row);
      return shardSpecs.get(index);
    };
  }

  @VisibleForTesting
  int hash(final long timestamp, final InputRow inputRow)
  {
    return hashPartitionFunction.hash(
        HashBasedNumberedShardSpec.serializeGroupKey(jsonMapper, extractKeys(timestamp, inputRow)),
        numBuckets
    );
  }

  /**
   * This method extracts keys for hash partitioning based on whether {@param partitionDimensions} is empty or not.
   * If yes, then both {@param timestamp} and dimension values in {@param inputRow} are returned.
   * Otherwise, values of {@param partitionDimensions} are returned.
   *
   * @param timestamp should be bucketed with query granularity
   * @param inputRow  row from input data
   *
   * @return a list of values of grouping keys
   */
  @VisibleForTesting
  List<Object> extractKeys(final long timestamp, final InputRow inputRow)
  {
    return extractKeys(partitionDimensions, timestamp, inputRow);
  }

  public static List<Object> extractKeys(
      final List<String> partitionDimensions,
      final long timestamp,
      final InputRow inputRow
  )
  {
    if (partitionDimensions.isEmpty()) {
      return Rows.toGroupKey(timestamp, inputRow);
    } else {
      return Lists.transform(partitionDimensions, inputRow::getDimension);
    }
  }
}
