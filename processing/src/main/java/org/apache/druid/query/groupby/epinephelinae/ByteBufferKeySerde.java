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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.aggregation.AggregatorFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class ByteBufferKeySerde implements Grouper.KeySerde<ByteBuffer>
{
  private final int keySize;

  public ByteBufferKeySerde(final int keySize)
  {
    this.keySize = keySize;
  }

  @Override
  public int keySize()
  {
    return keySize;
  }

  @Override
  public Class<ByteBuffer> keyClazz()
  {
    return ByteBuffer.class;
  }

  @Override
  public List<String> getDictionary()
  {
    return ImmutableList.of();
  }

  @Override
  public ByteBuffer toByteBuffer(ByteBuffer key)
  {
    return key;
  }

  @Override
  public ByteBuffer fromByteBuffer(ByteBuffer buffer, int position)
  {
    final ByteBuffer dup = buffer.duplicate();
    dup.position(position).limit(position + keySize);
    return dup.slice();
  }

  @Override
  public Grouper.BufferComparator bufferComparator()
  {
    // This class is used by segment processing engines, where bufferComparator will not be called.
    throw new UnsupportedOperationException();
  }

  @Override
  public Grouper.BufferComparator bufferComparatorWithAggregators(
      AggregatorFactory[] aggregatorFactories,
      int[] aggregatorOffsets
  )
  {
    // This class is used by segment processing engines, where bufferComparatorWithAggregators will not be called.
    throw new UnsupportedOperationException();
  }

  @Override
  public void reset()
  {
    // No state, nothing to reset
  }
}
