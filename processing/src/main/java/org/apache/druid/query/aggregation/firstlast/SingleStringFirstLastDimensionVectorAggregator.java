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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SingleStringFirstLastDimensionVectorAggregator
    extends FirstLastVectorAggregator<String, SerializablePairLongString>
{
  private final SingleValueDimensionVectorSelector singleValueDimensionVectorSelector;
  private final int maxStringBytes;
  private final SelectionPredicate selectionPredicate;

  protected SingleStringFirstLastDimensionVectorAggregator(
      VectorValueSelector timeSelector,
      SingleValueDimensionVectorSelector singleValueDimensionVectorSelector,
      int maxStringBytes,
      SelectionPredicate selectionPredicate
  )
  {
    super(
        timeSelector,
        new SingleValueDimensionVectorSelectorAdapter(singleValueDimensionVectorSelector),
        null,
        selectionPredicate
    );
    this.singleValueDimensionVectorSelector = singleValueDimensionVectorSelector;
    this.maxStringBytes = maxStringBytes;
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
    buf.putInt(position + VALUE_OFFSET, 0);
  }

  /**
   * Doesnt need to handle the null handling because that is taken care of while inserting the value, therefore in
   * replaceWithDefault mode, it is always going to be non-null
   */
  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    long time = buf.getLong(position);
    if (buf.get(position + NULLITY_OFFSET) == NullHandling.IS_NULL_BYTE) {
      return new SerializablePairLongString(time, null);
    }
    int index = buf.getInt(position + VALUE_OFFSET);
    String value = singleValueDimensionVectorSelector.lookupName(index);
    return new SerializablePairLongString(time, StringUtils.chop(value, maxStringBytes));
  }

  @Override
  protected void putValue(ByteBuffer buf, int position, long time, String value)
  {
    throw DruidException.defensive("This method is not applicable to the SingleStringFirstLastDimensionVectorAggregator");
  }

  @Override
  protected void putValue(ByteBuffer buf, int position, long time, VectorValueSelector valueSelector, int index)
  {
    buf.putLong(position, time);
    buf.put(position + NULLITY_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    buf.putInt(
        position + VALUE_OFFSET,
        ((SingleValueDimensionVectorSelectorAdapter) valueSelector).singleValueDimensionVectorSelector.getRowVector()[index]
    );
  }

  @Override
  protected void putDefaultValue(ByteBuffer buf, int position, long time)
  {
    buf.putLong(position, time);
    buf.put(position + NULLITY_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    buf.putInt(position + VALUE_OFFSET, 0);
  }

  @Override
  protected void putNull(ByteBuffer buf, int position, long time)
  {
    buf.putLong(position, time);
    buf.put(position + NULLITY_OFFSET, NullHandling.IS_NULL_BYTE);
    buf.putInt(position + VALUE_OFFSET, 0);
  }

  @Override
  protected SerializablePairLongString readPairFromVectorSelectors(
      boolean[] timeNullityVector,
      long[] timeVector,
      Object[] maybeFoldedObjects,
      int index
  )
  {
    throw DruidException.defensive("This method is not applicable to the SingleStringFirstLastDimensionVectorAggregator");
  }

  /**
   * Adapter class to from {@link SingleValueDimensionVectorSelector} to {@link VectorValueSelector}, to pass to the parent
   * class. The parent class only uses the passed in selector for the null check, therefore {@link #getNullVector()} is
   * the only relevant method implemented by the adapter. Each string value (even null) is assigned an id that will get stored,
   * therefore fetching the nullVector returns null (i.e. no null values)
   */
  private static class SingleValueDimensionVectorSelectorAdapter implements VectorValueSelector
  {

    private final SingleValueDimensionVectorSelector singleValueDimensionVectorSelector;

    public SingleValueDimensionVectorSelectorAdapter(SingleValueDimensionVectorSelector singleValueDimensionVectorSelector)
    {
      this.singleValueDimensionVectorSelector = singleValueDimensionVectorSelector;
    }

    @Override
    public int getMaxVectorSize()
    {
      throw DruidException.defensive("Unexpected call");
    }

    @Override
    public int getCurrentVectorSize()
    {
      throw DruidException.defensive("Unexpected call");
    }

    @Override
    public long[] getLongVector()
    {
      throw DruidException.defensive("Unexpected call");
    }

    @Override
    public float[] getFloatVector()
    {
      throw DruidException.defensive("Unexpected call");
    }

    @Override
    public double[] getDoubleVector()
    {
      throw DruidException.defensive("Unexpected call");
    }

    /**
     * This is the only useful method, that will get used by the parent class.
     */
    @Nullable
    @Override
    public boolean[] getNullVector()
    {
      return null;
    }
  }
}
