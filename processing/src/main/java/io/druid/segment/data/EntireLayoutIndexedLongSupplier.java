/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.base.Supplier;
import io.druid.java.util.common.StringUtils;

public class EntireLayoutIndexedLongSupplier implements Supplier<IndexedLongs>
{

  private final int totalSize;
  private final CompressionFactory.LongEncodingReader reader;

  public EntireLayoutIndexedLongSupplier(int totalSize, CompressionFactory.LongEncodingReader reader)
  {
    this.totalSize = totalSize;
    this.reader = reader;
  }

  @Override
  public IndexedLongs get()
  {
    return new EntireLayoutIndexedLongSupplier.EntireLayoutIndexedLongs();
  }

  private class EntireLayoutIndexedLongs implements IndexedLongs
  {

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public long get(int index)
    {
      return reader.read(index);
    }

    @Override
    public void fill(int index, long[] toFill)
    {
      if (totalSize - index < toFill.length) {
        throw new IndexOutOfBoundsException(
            StringUtils.format(
                "Cannot fill array of size[%,d] at index[%,d].  Max size[%,d]", toFill.length, index, totalSize
            )
        );
      }
      for (int i = 0; i < toFill.length; i++) {
        toFill[i] = get(index + i);
      }
    }

    @Override
    public String toString()
    {
      return "EntireCompressedIndexedLongs_Anonymous{" +
             ", totalSize=" + totalSize +
             '}';
    }

    @Override
    public void close()
    {
    }
  }
}
