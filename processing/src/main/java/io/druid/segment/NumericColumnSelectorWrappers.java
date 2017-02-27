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

package io.druid.segment;

import com.google.common.collect.ImmutableList;
import io.druid.segment.column.ValueType;

import java.util.List;

public class NumericColumnSelectorWrappers
{
  public static final List<ValueType> WRAPPABLE_TYPES = ImmutableList.of(
      ValueType.LONG,
      ValueType.FLOAT
  );

  public static LongColumnSelector wrapFloatAsLong(final FloatColumnSelector selector) {
    class FloatWrappingLongColumnSelector implements LongColumnSelector
    {
      @Override
      public long get()
      {
        return (long) selector.get();
      }
    }
    return new FloatWrappingLongColumnSelector();
  };

  public static FloatColumnSelector wrapLongAsFloat(final LongColumnSelector selector) {
    class LongWrappingFloatColumnSelector implements FloatColumnSelector
    {
      @Override
      public float get()
      {
        return (float) selector.get();
      }
    }
    return new LongWrappingFloatColumnSelector();
  };
}
