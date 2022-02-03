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

package org.apache.druid.timeline.partition.bloomfilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.partition.NamedNumberedShardSpec;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BloomFilterNamedNumberedShardSpec extends NamedNumberedShardSpec
{
  @JsonIgnore
  private final String dimension;

  @JsonIgnore
  private final BloomFilter<Long> bloomFilter;

  @JsonIgnore
  private final List<String> domainDimensions;

  @JsonCreator
  public BloomFilterNamedNumberedShardSpec(
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("partitions") int partitions,
      @JsonProperty("partitionName") @Nullable String partitionName,
      @JsonProperty("dimension") String dimension,
      @JsonProperty("bloomFilter") String bloomFilter
  )
  {
    super(partitionNum, partitions, partitionName);
    this.dimension = dimension;
    this.domainDimensions = com.google.common.collect.ImmutableList.of(dimension);
    try {
      this.bloomFilter = BloomFilter.readFrom(
          new ByteArrayInputStream(StringUtils.decodeBase64String(bloomFilter)),
          Funnels.longFunnel()
      );
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public BloomFilterNamedNumberedShardSpec(
      int partitionNum,
      int partitions,
      @Nullable String partitionName,
      String dimension,
      List<Long> values
  )
  {
    super(partitionNum, partitions, partitionName);
    this.dimension = dimension;
    this.domainDimensions = com.google.common.collect.ImmutableList.of(dimension);
    this.bloomFilter = BloomFilter.create(Funnels.longFunnel(), values.size());
    values.forEach(v -> this.bloomFilter.put(v));
  }

  @JsonProperty("bloomFilter")
  public String getBloomFilter()
  {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      bloomFilter.writeTo(byteArrayOutputStream);
      return StringUtils.encodeBase64String(byteArrayOutputStream.toByteArray());
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @JsonProperty("dimension")
  public String getDimension()
  {
    return this.dimension;
  }

  @Override
  public List<String> getDomainDimensions()
  {
    return domainDimensions;
  }

  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    if (!domain.containsKey(dimension)) {
      return true;
    }
    // Only support the range set that contains just 1 long element
    Set<Range<String>> ranges = domain.get(dimension).asRanges();
    if (ranges.size() != 1) {
      return true;
    }
    Range<String> r = ranges.iterator().next();
    try {
      if (r.lowerEndpoint() != r.upperEndpoint()) {
        return true;
      }
      return bloomFilter.mightContain(Long.valueOf(r.lowerEndpoint()));
    }
    catch (Exception unused) {
      return true;
    }
  }

  @Override
  public String toString()
  {
    return "BloomFilterNamedNumberedShardSpec{" +
           "partitionNum=" + getPartitionNum() +
           ", partitions=" + getPartitions() +
           ", partitionName=" + getPartitionName() +
           ", dimension=" + dimension +
           ", bloomFilter=" + getBloomFilter() +
           '}';
  }

}
