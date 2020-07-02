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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.InputRow;

import java.util.List;
import java.util.Map;

/**
 */
@Deprecated
public class NoneShardSpec implements ShardSpec
{
  private static final NoneShardSpec INSTANCE = new NoneShardSpec();

  @JsonCreator
  public static NoneShardSpec instance()
  {
    return INSTANCE;
  }

  private NoneShardSpec()
  {
    // empty
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new SingleElementPartitionChunk<>(obj);
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return true;
  }

  @Override
  @JsonIgnore
  public int getPartitionNum()
  {
    return 0;
  }

  @Override
  public int getNumCorePartitions()
  {
    return 0;
  }

  @Override
  public ShardSpecLookup getLookup(final List<? extends ShardSpec> shardSpecs)
  {
    return (long timestamp, InputRow row) -> shardSpecs.get(0);
  }

  @Override
  public List<String> getDomainDimensions()
  {
    return ImmutableList.of();
  }

  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    return true;
  }

  @Override
  public boolean equals(Object obj)
  {
    return obj instanceof NoneShardSpec;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "NoneShardSpec";
  }
}
