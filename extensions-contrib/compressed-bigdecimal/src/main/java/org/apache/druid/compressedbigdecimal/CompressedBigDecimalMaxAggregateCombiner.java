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

package org.apache.druid.compressedbigdecimal;


import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

/**
 * AggregateCombiner for CompressedBigDecimals computing a max
 */
public class CompressedBigDecimalMaxAggregateCombiner implements AggregateCombiner<CompressedBigDecimal>
{
  private CompressedBigDecimal max;

  @Override
  public void reset(@SuppressWarnings("rawtypes") ColumnValueSelector columnValueSelector)
  {
    @SuppressWarnings("unchecked")
    ColumnValueSelector<CompressedBigDecimal> selector =
        (ColumnValueSelector<CompressedBigDecimal>) columnValueSelector;

    CompressedBigDecimal cbd = selector.getObject();
    if (max == null) {
      max = new ArrayCompressedBigDecimal(cbd);
    } else {
      max.setValue(cbd);
    }
  }

  @Override
  public void fold(@SuppressWarnings("rawtypes") ColumnValueSelector columnValueSelector)
  {
    @SuppressWarnings("unchecked")
    ColumnValueSelector<CompressedBigDecimal> selector =
        (ColumnValueSelector<CompressedBigDecimal>) columnValueSelector;
    CompressedBigDecimal cbd = selector.getObject();

    if (max == null) {
      max = new ArrayCompressedBigDecimal(cbd);
    } else {
      max.accumulateMax(cbd);
    }
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support getDouble()");
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support getLong()");
  }

  @Nullable
  @Override
  public CompressedBigDecimal getObject()
  {
    return max;
  }

  @Override
  public Class<CompressedBigDecimal> classOfObject()
  {
    return CompressedBigDecimal.class;
  }
}
