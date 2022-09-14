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
import com.google.common.hash.Hashing;

import java.util.Arrays;

/**
 * Represents a specific sorting or hashing key. Instances of this class wrap a byte array in row-based frame format.
 */
public class RowKey
{
  private static final RowKey EMPTY_KEY = new RowKey(new byte[0]);

  private final byte[] key;

  // Cached hashcode. Computed on demand, not in the constructor, to avoid unnecessary computation.
  private volatile long hashCode;
  private volatile boolean hashCodeComputed;

  private RowKey(byte[] key)
  {
    this.key = key;
  }

  /**
   * Create a key from a byte array. The array will be owned by the resulting key object.
   */
  @JsonCreator
  public static RowKey wrap(final byte[] row)
  {
    if (row.length == 0) {
      return EMPTY_KEY;
    } else {
      return new RowKey(row);
    }
  }

  public static RowKey empty()
  {
    return EMPTY_KEY;
  }

  /**
   * Get the backing array for this key (not a copy).
   */
  @JsonValue
  public byte[] array()
  {
    return key;
  }

  public long longHashCode()
  {
    // May compute hashCode multiple times if called from different threads, but that's fine. (And unlikely, given
    // how we use these objects.)
    if (!hashCodeComputed) {
      // Use murmur3_128 for dispersion properties needed by DistinctKeyCollector#isKeySelected.
      hashCode = Hashing.murmur3_128().hashBytes(key).asLong();
      hashCodeComputed = true;
    }

    return hashCode;
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
    RowKey that = (RowKey) o;
    return Arrays.equals(key, that.key);
  }

  @Override
  public int hashCode()
  {
    // Truncation is OK with murmur3_128.
    return (int) longHashCode();
  }

  @Override
  public String toString()
  {
    return Arrays.toString(key);
  }

  public int getNumberOfBytes()
  {
    return array().length;
  }
}
