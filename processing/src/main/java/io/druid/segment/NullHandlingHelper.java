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

import com.google.common.base.Strings;
import com.google.inject.Inject;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.DoubleAggregateCombiner;
import io.druid.query.aggregation.LongAggregateCombiner;
import io.druid.query.aggregation.NullableAggregator;
import io.druid.query.aggregation.NullableBufferAggregator;
import io.druid.query.aggregation.NullableDoubleAggregateCombiner;
import io.druid.query.aggregation.NullableLongAggregateCombiner;
import io.druid.query.aggregation.NullableObjectAggregateCombiner;
import io.druid.query.aggregation.ObjectAggregateCombiner;

public class NullHandlingHelper
{
  // use these values to ensure that convertObjectToLong(), convertObjectToDouble() and convertObjectToFloat()
  // return the same boxed object when returning a constant zero.
  public static final Double ZERO_DOUBLE = 0.0d;
  public static final Float ZERO_FLOAT = 0.0f;
  public static final Long ZERO_LONG = 0L;

  // Using static injection to avoid adding JacksonInject annotations all over the code.
  @Inject
  private static NullValueHandlingConfig INSTANCE = new NullValueHandlingConfig(false);

  public static boolean useDefaultValuesForNull()
  {
    return INSTANCE.isUseDefaultValuesForNull();
  }

  public static String nullToDefault(String value)
  {
    return INSTANCE.isUseDefaultValuesForNull() ? Strings.nullToEmpty(value) : value;
  }

  public static String defaultToNull(String value)
  {
    return INSTANCE.isUseDefaultValuesForNull() ? Strings.emptyToNull(value) : value;
  }

  public static boolean isNullOrDefault(String value)
  {
    return INSTANCE.isUseDefaultValuesForNull() ? Strings.isNullOrEmpty(value) : value == null;
  }

  public static Long nullToDefault(Long value)
  {
    return INSTANCE.isUseDefaultValuesForNull() && value == null ? ZERO_LONG : value;
  }

  public static Double nullToDefault(Double value)
  {
    return INSTANCE.isUseDefaultValuesForNull() && value == null ? ZERO_DOUBLE : value;
  }

  public static Float nullToDefault(Float value)
  {
    return INSTANCE.isUseDefaultValuesForNull() && value == null ? ZERO_FLOAT : value;
  }

  public static Aggregator getNullableAggregator(Aggregator aggregator, ColumnValueSelector selector)
  {
    return INSTANCE.isUseDefaultValuesForNull() ? aggregator : new NullableAggregator(aggregator, selector);
  }

  public static BufferAggregator getNullableAggregator(BufferAggregator aggregator, ColumnValueSelector selector)
  {
    return INSTANCE.isUseDefaultValuesForNull() ? aggregator : new NullableBufferAggregator(aggregator, selector);
  }

  public static DoubleAggregateCombiner getNullableCombiner(DoubleAggregateCombiner combiner)
  {
    return INSTANCE.isUseDefaultValuesForNull() ? combiner : new NullableDoubleAggregateCombiner(combiner);
  }

  public static LongAggregateCombiner getNullableCombiner(LongAggregateCombiner combiner)
  {
    return INSTANCE.isUseDefaultValuesForNull() ? combiner : new NullableLongAggregateCombiner(combiner);
  }

  public static ObjectAggregateCombiner getNullableCombiner(ObjectAggregateCombiner combiner)
  {
    return INSTANCE.isUseDefaultValuesForNull() ? combiner : new NullableObjectAggregateCombiner(combiner);
  }
}
