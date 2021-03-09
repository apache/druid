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
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.com.google.common.hash.Hashing;
import org.apache.druid.java.util.common.StringUtils;

/**
 * An enum of supported hash partition functions. This enum should be updated when we want to use a new function
 * for hash partitioning. All partition functions listed in this enum must be backwards-compatible as the hash
 * function should apply to all segments in the same way no matter what Druid version was used to create those segments.
 * This function is a part of {@link HashBasedNumberedShardSpec} which is stored in the metadata store.
 */
public enum HashPartitionFunction
{
  MURMUR3_32_ABS {
    @Override
    public int hash(byte[] serializedRow, int numBuckets)
    {
      return Math.abs(
          Hashing.murmur3_32().hashBytes(serializedRow).asInt() % numBuckets
      );
    }
  };

  /**
   * Returns an ID of a hash bucket for the given {@code serializedRow}.
   */
  public abstract int hash(byte[] serializedRow, int numBuckets);

  @JsonCreator
  public static HashPartitionFunction fromString(String type)
  {
    return HashPartitionFunction.valueOf(StringUtils.toUpperCase(type));
  }

  @Override
  @JsonValue
  public String toString()
  {
    return StringUtils.toLowerCase(name());
  }
}
