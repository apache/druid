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
import io.druid.query.aggregation.AggregateCombiner;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.NullableAggregateCombiner;
import io.druid.query.aggregation.NullableAggregator;
import io.druid.query.aggregation.NullableBufferAggregator;

public class NullHandlingHelper
{
  // use these values to ensure that convertObjectToLong(), convertObjectToDouble() and convertObjectToFloat()
  // return the same boxed object when returning a constant zero.
  public static final Double ZERO_DOUBLE = 0.0d;
  public static final Float ZERO_FLOAT = 0.0f;
  public static final Long ZERO_LONG = 0L;

  // Using static injection to avoid adding JacksonInject annotations all over the code.
  @Inject
  private static boolean useDefaultValuesForNull = Boolean.valueOf(System.getProperty(
      "druid.null.handling.useDefaultValueForNull",
      "true"
  ));

  public static boolean useDefaultValuesForNull()
  {
    return useDefaultValuesForNull;
  }

  public static String nullToDefault(String value)
  {
    return useDefaultValuesForNull ? Strings.nullToEmpty(value) : value;
  }

  public static String defaultToNull(String value)
  {
    return useDefaultValuesForNull ? Strings.emptyToNull(value) : value;
  }

  public static boolean isNullOrDefault(String value)
  {
    return useDefaultValuesForNull ? Strings.isNullOrEmpty(value) : value == null;
  }

  public static Long nullToDefault(Long value)
  {
    return useDefaultValuesForNull && value == null ? ZERO_LONG : value;
  }

  public static Double nullToDefault(Double value)
  {
    return useDefaultValuesForNull && value == null ? ZERO_DOUBLE : value;
  }

  public static Float nullToDefault(Float value)
  {
    return useDefaultValuesForNull && value == null ? ZERO_FLOAT : value;
  }

  public static Aggregator getNullableAggregator(Aggregator aggregator, ColumnValueSelector selector)
  {
    return useDefaultValuesForNull ? aggregator : new NullableAggregator(aggregator, selector);
  }

  public static BufferAggregator getNullableAggregator(BufferAggregator aggregator, ColumnValueSelector selector)
  {
    return useDefaultValuesForNull ? aggregator : new NullableBufferAggregator(aggregator, selector);
  }

  public static AggregateCombiner getNullableCombiner(AggregateCombiner combiner)
  {
    return useDefaultValuesForNull ? combiner : new NullableAggregateCombiner(combiner);
  }

  public static int extraAggregatorBytes()
  {
    return NullHandlingHelper.useDefaultValuesForNull() ? 0 : Byte.BYTES;
  }
}
