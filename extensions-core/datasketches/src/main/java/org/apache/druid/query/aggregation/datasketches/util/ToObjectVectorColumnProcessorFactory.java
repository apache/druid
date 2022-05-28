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

package org.apache.druid.query.aggregation.datasketches.util;

import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VectorColumnProcessorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import java.util.function.Supplier;

/**
 * Builds vector processors that return Object arrays. Not a terribly efficient way to write aggregators, since this
 * is fighting against the strongly-typed design of the vector processing system. However, it simplifies the aggregator
 * code quite a bit, and most of the sketches that use this don't have special handling for primitive types anyway, so
 * we hopefully shouldn't lose much performance.
 */
public class ToObjectVectorColumnProcessorFactory implements VectorColumnProcessorFactory<Supplier<Object[]>>
{
  public static final ToObjectVectorColumnProcessorFactory INSTANCE = new ToObjectVectorColumnProcessorFactory();

  private ToObjectVectorColumnProcessorFactory()
  {
  }

  @Override
  public Supplier<Object[]> makeSingleValueDimensionProcessor(
      ColumnCapabilities capabilities,
      SingleValueDimensionVectorSelector selector
  )
  {
    final Object[] retVal = new Object[selector.getMaxVectorSize()];

    return () -> {
      final int[] values = selector.getRowVector();

      for (int i = 0; i < selector.getCurrentVectorSize(); i++) {
        retVal[i] = selector.lookupName(values[i]);
      }

      return retVal;
    };
  }

  @Override
  public Supplier<Object[]> makeMultiValueDimensionProcessor(
      ColumnCapabilities capabilities,
      MultiValueDimensionVectorSelector selector
  )
  {
    final Object[] retVal = new Object[selector.getMaxVectorSize()];

    return () -> {
      final IndexedInts[] values = selector.getRowVector();

      for (int i = 0; i < selector.getCurrentVectorSize(); i++) {
        retVal[i] = DimensionSelector.rowToObject(values[i], selector);
      }

      return retVal;
    };
  }

  @Override
  public Supplier<Object[]> makeFloatProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
  {
    final Object[] retVal = new Object[selector.getMaxVectorSize()];

    return () -> {
      final float[] values = selector.getFloatVector();
      final boolean[] nulls = selector.getNullVector();

      for (int i = 0; i < selector.getCurrentVectorSize(); i++) {
        retVal[i] = nulls == null || !nulls[i] ? values[i] : null;
      }

      return retVal;
    };
  }

  @Override
  public Supplier<Object[]> makeDoubleProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
  {
    final Object[] retVal = new Object[selector.getMaxVectorSize()];

    return () -> {
      final double[] values = selector.getDoubleVector();
      final boolean[] nulls = selector.getNullVector();

      for (int i = 0; i < selector.getCurrentVectorSize(); i++) {
        retVal[i] = nulls == null || !nulls[i] ? values[i] : null;
      }

      return retVal;
    };
  }

  @Override
  public Supplier<Object[]> makeLongProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
  {
    final Object[] retVal = new Object[selector.getMaxVectorSize()];

    return () -> {
      final long[] values = selector.getLongVector();
      final boolean[] nulls = selector.getNullVector();

      for (int i = 0; i < selector.getCurrentVectorSize(); i++) {
        retVal[i] = nulls == null || !nulls[i] ? values[i] : null;
      }

      return retVal;
    };
  }

  @Override
  public Supplier<Object[]> makeObjectProcessor(ColumnCapabilities capabilities, VectorObjectSelector selector)
  {
    return selector::getObjectVector;
  }
}
