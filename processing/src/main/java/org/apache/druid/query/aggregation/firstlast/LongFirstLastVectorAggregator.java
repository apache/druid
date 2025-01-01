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

package org.apache.druid.query.aggregation.firstlast;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.SerializablePairLongLong;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Vectorized version of on heap 'last' aggregator for column selectors with type LONG..
 */
public class LongFirstLastVectorAggregator extends FirstLastVectorAggregator<Long, SerializablePairLongLong>
{
  private final SelectionPredicate selectionPredicate;

  protected LongFirstLastVectorAggregator(
      VectorValueSelector timeSelector,
      VectorObjectSelector objectSelector,
      SelectionPredicate selectionPredicate
  )
  {
    super(timeSelector, null, objectSelector, selectionPredicate);
    this.selectionPredicate = selectionPredicate;
  }

  protected LongFirstLastVectorAggregator(
      VectorValueSelector timeSelector,
      VectorValueSelector valueSelector,
      SelectionPredicate selectionPredicate
  )
  {
    super(timeSelector, valueSelector, null, selectionPredicate);
    this.selectionPredicate = selectionPredicate;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, selectionPredicate.initValue());
    buf.put(
        position + NULLITY_OFFSET,
        NullHandling.replaceWithDefault() ? NullHandling.IS_NOT_NULL_BYTE : NullHandling.IS_NULL_BYTE
    );
    buf.putLong(position + VALUE_OFFSET, 0L);
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    long time = buf.getLong(position);
    if (buf.get(position + NULLITY_OFFSET) == NullHandling.IS_NULL_BYTE) {
      return new SerializablePairLongLong(time, null);
    }
    return new SerializablePairLongLong(time, buf.getLong(position + VALUE_OFFSET));
  }

  @Override
  protected void putValue(ByteBuffer buf, int position, long time, Long value)
  {
    buf.putLong(position, time);
    buf.put(position + NULLITY_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    buf.putLong(position + VALUE_OFFSET, value);
  }

  @Override
  protected void putValue(ByteBuffer buf, int position, long time, VectorValueSelector valueSelector, int index)
  {
    buf.putLong(position, time);
    buf.put(position + NULLITY_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    buf.putLong(position + VALUE_OFFSET, valueSelector.getLongVector()[index]);
  }

  @Override
  protected void putNull(ByteBuffer buf, int position, long time)
  {
    buf.putLong(position, time);
    buf.put(position + NULLITY_OFFSET, NullHandling.IS_NULL_BYTE);
    buf.putLong(position + VALUE_OFFSET, 0L);
  }

  @Override
  protected void putDefaultValue(ByteBuffer buf, int position, long time)
  {
    buf.putLong(position, time);
    buf.put(position + NULLITY_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    buf.putLong(position + VALUE_OFFSET, 0L);
  }

  @Override
  protected SerializablePairLongLong readPairFromVectorSelectors(
      boolean[] timeNullityVector,
      long[] timeVector,
      Object[] maybeFoldedObjects,
      int index
  )
  {
    return FirstLastUtils.readLongPairFromVectorSelectors(timeNullityVector, timeVector, maybeFoldedObjects, index);
  }
}
