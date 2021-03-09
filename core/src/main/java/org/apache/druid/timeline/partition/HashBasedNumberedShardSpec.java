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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.com.google.common.collect.BoundType;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.Lists;
import org.apache.druid.com.google.common.collect.Range;
import org.apache.druid.com.google.common.collect.RangeSet;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class HashBasedNumberedShardSpec extends NumberedShardSpec
{
  public static final List<String> DEFAULT_PARTITION_DIMENSIONS = ImmutableList.of();

  private final int bucketId;

  /**
   * Number of hash buckets
   */
  private final int numBuckets;
  private final ObjectMapper jsonMapper;
  private final List<String> partitionDimensions;

  /**
   * A hash function to use for both hash partitioning at ingestion time and pruning segments at query time.
   *
   * During ingestion, the partition function is defaulted to {@link HashPartitionFunction#MURMUR3_32_ABS} if this
   * variable is null. See {@link HashPartitioner} for details.
   *
   * During query, this function will be null unless it is explicitly specified in
   * {@link org.apache.druid.indexer.partitions.HashedPartitionsSpec} at ingestion time. This is because the default
   * hash function used to create segments at ingestion time can change over time, but we don't guarantee the changed
   * hash function is backwards-compatible. The query will process all segments if this function is null.
   */
  @Nullable
  private final HashPartitionFunction partitionFunction;

  @JsonCreator
  public HashBasedNumberedShardSpec(
      @JsonProperty("partitionNum") int partitionNum,    // partitionId
      @JsonProperty("partitions") int partitions,        // core partition set size
      @JsonProperty("bucketId") @Nullable Integer bucketId, // nullable for backward compatibility
      @JsonProperty("numBuckets") @Nullable Integer numBuckets, // nullable for backward compatibility
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,
      @JsonProperty("partitionFunction") @Nullable HashPartitionFunction partitionFunction, // nullable for backward compatibility
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(partitionNum, partitions);
    // Use partitionId as bucketId if it's missing.
    this.bucketId = bucketId == null ? partitionNum : bucketId;
    // If numBuckets is missing, assume that any hash bucket is not empty.
    // Use the core partition set size as the number of buckets.
    this.numBuckets = numBuckets == null ? partitions : numBuckets;
    this.partitionDimensions = partitionDimensions == null ? DEFAULT_PARTITION_DIMENSIONS : partitionDimensions;
    this.partitionFunction = partitionFunction;
    this.jsonMapper = jsonMapper;
  }

  @JsonProperty
  public int getBucketId()
  {
    return bucketId;
  }

  @JsonProperty
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @JsonProperty("partitionDimensions")
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @JsonProperty
  public @Nullable HashPartitionFunction getPartitionFunction()
  {
    return partitionFunction;
  }

  @Override
  public List<String> getDomainDimensions()
  {
    return partitionDimensions;
  }

  @Override
  public ShardSpecLookup getLookup(final List<? extends ShardSpec> shardSpecs)
  {
    // partitionFunction can be null when you read a shardSpec of a segment created in an old version of Druid.
    // The current version of Druid will always specify a partitionFunction on newly created segments.
    if (partitionFunction == null) {
      throw new ISE("Cannot create a hashPartitioner since partitionFunction is null");
    }
    return new HashPartitioner(
        jsonMapper,
        partitionFunction,
        partitionDimensions,
        numBuckets
    ).createHashLookup(shardSpecs);
  }

  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    // partitionFunction should be used instead of HashPartitioner at query time.
    // We should process all segments if partitionFunction is null because we don't know what hash function
    // was used to create segments at ingestion time.
    if (partitionFunction == null) {
      return true;
    }

    // If no partitionDimensions are specified during ingestion, hash is based on all dimensions plus the truncated
    // input timestamp according to QueryGranularity instead of just partitionDimensions. Since we don't store in shard
    // specs the truncated timestamps of the events that fall into the shard after ingestion, there's no way to recover
    // the hash during ingestion, bypass this case
    if (partitionDimensions.isEmpty()) {
      return true;
    }

    // One possible optimization is to move the conversion from range set to point set to the function signature and
    // cache it in the caller of this function if there are repetitive calls of the same domain
    Map<String, Set<String>> domainSet = new HashMap<>();
    for (String p : partitionDimensions) {
      RangeSet<String> domainRangeSet = domain.get(p);
      if (domainRangeSet == null || domainRangeSet.isEmpty()) {
        return true;
      }

      for (Range<String> v : domainRangeSet.asRanges()) {
        // If there are range values, simply bypass, because we can't hash range values
        if (v.isEmpty() || !v.hasLowerBound() || !v.hasUpperBound() ||
            v.lowerBoundType() != BoundType.CLOSED || v.upperBoundType() != BoundType.CLOSED ||
            !v.lowerEndpoint().equals(v.upperEndpoint())) {
          return true;
        }
        domainSet.computeIfAbsent(p, k -> new HashSet<>()).add(v.lowerEndpoint());
      }
    }

    return !domainSet.isEmpty() && chunkPossibleInDomain(partitionFunction, domainSet, new HashMap<>());
  }

  /**
   * Recursively enumerate all possible combinations of values for dimensions in {@link #partitionDimensions} based on
   * {@code domainSet}, test if any combination matches the current segment
   *
   * @param hashPartitionFunction     hash function used to create segments at ingestion time
   * @param domainSet                 The set where values of dimensions in {@link #partitionDimensions} are
   *                                  drawn from
   * @param partitionDimensionsValues A map from dimensions in {@link #partitionDimensions} to their values drawn from
   *                                  {@code domainSet}
   * @return Whether the current segment possibly holds records for the provided domain. Return false if and only if
   * none of the combinations matches this segment
   */
  private boolean chunkPossibleInDomain(
      HashPartitionFunction hashPartitionFunction,
      Map<String, Set<String>> domainSet,
      Map<String, String> partitionDimensionsValues
  )
  {
    int curIndex = partitionDimensionsValues.size();
    if (curIndex == partitionDimensions.size()) {
      return isInChunk(hashPartitionFunction, partitionDimensionsValues);
    }

    String dimension = partitionDimensions.get(curIndex);
    for (String e : domainSet.get(dimension)) {
      partitionDimensionsValues.put(dimension, e);
      if (chunkPossibleInDomain(hashPartitionFunction, domainSet, partitionDimensionsValues)) {
        return true;
      }
      partitionDimensionsValues.remove(dimension);
    }

    return false;
  }

  /**
   * Check if the current segment possibly holds records if the values of dimensions in {@link #partitionDimensions}
   * are of {@code partitionDimensionsValues}
   *
   * @param hashPartitionFunction     hash function used to create segments at ingestion time
   * @param partitionDimensionsValues An instance of values of dimensions in {@link #partitionDimensions}
   *
   * @return Whether the current segment possibly holds records for the given values of partition dimensions
   */
  private boolean isInChunk(HashPartitionFunction hashPartitionFunction, Map<String, String> partitionDimensionsValues)
  {
    assert !partitionDimensions.isEmpty();
    List<Object> groupKey = Lists.transform(
        partitionDimensions,
        o -> Collections.singletonList(partitionDimensionsValues.get(o))
    );
    return hashPartitionFunction.hash(serializeGroupKey(jsonMapper, groupKey), numBuckets) == bucketId;
  }

  /**
   * Serializes a group key into a byte array. The serialization algorithm can affect hash values of partition keys
   * since {@link HashPartitionFunction#hash} takes the result of this method as its input. This means, the returned
   * byte array should be backwards-compatible in cases where we need to modify this method.
   */
  public static byte[] serializeGroupKey(ObjectMapper jsonMapper, List<Object> partitionKeys)
  {
    try {
      return jsonMapper.writeValueAsBytes(partitionKeys);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
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
    if (!super.equals(o)) {
      return false;
    }
    HashBasedNumberedShardSpec that = (HashBasedNumberedShardSpec) o;
    return bucketId == that.bucketId &&
           numBuckets == that.numBuckets &&
           Objects.equals(partitionDimensions, that.partitionDimensions) &&
           partitionFunction == that.partitionFunction;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), bucketId, numBuckets, partitionDimensions, partitionFunction);
  }

  @Override
  public String toString()
  {
    return "HashBasedNumberedShardSpec{" +
           "bucketId=" + bucketId +
           ", numBuckets=" + numBuckets +
           ", partitionDimensions=" + partitionDimensions +
           ", partitionFunction=" + partitionFunction +
           '}';
  }
}
