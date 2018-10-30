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

package org.apache.druid.query.filter;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.hive.common.util.BloomFilter;

import java.io.IOException;
import java.util.Objects;

public class BloomFilterHolder
{
  private final BloomFilter filter;
  private final HashCode hash;

  public BloomFilterHolder(BloomFilter filter, HashCode hash)
  {
    this.filter = filter;
    this.hash = hash;
  }

  BloomFilter getFilter()
  {
    return filter;
  }

  HashCode getFilterHash()
  {
    return hash;
  }

  public static BloomFilterHolder fromBloomFilter(BloomFilter filter) throws IOException
  {
    byte[] bytes = BloomFilterSerializersModule.bloomFilterToBytes(filter);

    return new BloomFilterHolder(filter, Hashing.sha512().hashBytes(bytes));
  }

  public static BloomFilterHolder fromBytes(byte[] bytes) throws IOException
  {
    return new BloomFilterHolder(
        BloomFilterSerializersModule.bloomFilterFromBytes(bytes),
        Hashing.sha512().hashBytes(bytes)
    );
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

    BloomFilterHolder that = (BloomFilterHolder) o;
    return Objects.equals(this.hash, that.hash);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(filter, hash);
  }
}
