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
import com.google.common.primitives.Ints;
import org.apache.druid.query.aggregation.AggregatorFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class IntKeySerde implements Grouper.KeySerde<IntKey>
{
  public static final Grouper.KeySerde<IntKey> INSTANCE = new IntKeySerde();

  public static final Grouper.BufferComparator KEY_COMPARATOR = new Grouper.BufferComparator()
  {
    @Override
    public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
    {
      return Ints.compare(lhsBuffer.getInt(lhsPosition), rhsBuffer.getInt(rhsPosition));
    }
  };

  private final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);

  @Override
  public int keySize()
  {
    return Integer.BYTES;
  }

  @Override
  public Class<IntKey> keyClazz()
  {
    return IntKey.class;
  }

  @Override
  public List<String> getDictionary()
  {
    return ImmutableList.of();
  }

  @Override
  public ByteBuffer toByteBuffer(IntKey key)
  {
    buf.putInt(0, key.intValue());
    buf.position(0);
    return buf;
  }

  @Override
  public IntKey createKey()
  {
    return new IntKey(0);
  }

  @Override
  public void readFromByteBuffer(IntKey key, ByteBuffer buffer, int position)
  {
    key.setValue(buffer.getInt(position));
  }

  @Override
  public Grouper.BufferComparator bufferComparator()
  {
    return KEY_COMPARATOR;
  }

  @Override
  public Grouper.BufferComparator bufferComparatorWithAggregators(
      AggregatorFactory[] aggregatorFactories,
      int[] aggregatorOffsets
  )
  {
    return KEY_COMPARATOR;
  }

  @Override
  public void reset()
  {
    // Nothing to do
  }
}
