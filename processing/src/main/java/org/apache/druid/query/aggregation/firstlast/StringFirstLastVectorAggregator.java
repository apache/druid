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
import org.apache.druid.error.DruidException;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StringFirstLastVectorAggregator extends FirstLastVectorAggregator<String, SerializablePairLongString>
{
  private final int maxStringBytes;
  private final SelectionPredicate selectionPredicate;

  protected StringFirstLastVectorAggregator(
      @Nullable final VectorValueSelector timeSelector,
      final VectorObjectSelector objectSelector,
      final int maxStringBytes,
      final SelectionPredicate selectionPredicate
  )
  {
    super(timeSelector, null, objectSelector, selectionPredicate);
    this.maxStringBytes = maxStringBytes;
    this.selectionPredicate = selectionPredicate;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    StringFirstLastUtils.writePair(
        buf,
        position,
        new SerializablePairLongString(selectionPredicate.initValue(), null),
        maxStringBytes
    );
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return StringFirstLastUtils.readPair(buf, position);
  }

  @Override
  protected void putValue(ByteBuffer buf, int position, long time, String value)
  {
    StringFirstLastUtils.writePair(buf, position, new SerializablePairLongString(time, value), maxStringBytes);
  }

  @Override
  protected void putValue(ByteBuffer buf, int position, long time, VectorValueSelector valueSelector, int index)
  {
    throw DruidException.defensive("This variant is not applicable to the StringFirstLastVectorAggregator");
  }

  @Override
  protected void putDefaultValue(ByteBuffer buf, int position, long time)
  {
    StringFirstLastUtils.writePair(
        buf,
        position,
        new SerializablePairLongString(time, NullHandling.defaultStringValue()),
        maxStringBytes
    );
  }

  @Override
  protected void putNull(ByteBuffer buf, int position, long time)
  {
    StringFirstLastUtils.writePair(buf, position, new SerializablePairLongString(time, null), maxStringBytes);
  }

  @Override
  protected SerializablePairLongString readPairFromVectorSelectors(
      boolean[] timeNullityVector,
      long[] timeVector,
      Object[] maybeFoldedObjects,
      int index
  )
  {
    return StringFirstLastUtils.readPairFromVectorSelectorsAtIndex(
        timeNullityVector,
        timeVector,
        maybeFoldedObjects,
        index
    );
  }
}
