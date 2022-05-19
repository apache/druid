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

package org.apache.druid.query.groupby.epinephelinae.vector;

import com.google.common.base.Preconditions;
import org.apache.druid.segment.VectorColumnProcessorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

public class GroupByVectorColumnProcessorFactory implements VectorColumnProcessorFactory<GroupByVectorColumnSelector>
{
  private static final GroupByVectorColumnProcessorFactory INSTANCE = new GroupByVectorColumnProcessorFactory();

  private GroupByVectorColumnProcessorFactory()
  {
    // Singleton.
  }

  public static GroupByVectorColumnProcessorFactory instance()
  {
    return INSTANCE;
  }

  @Override
  public GroupByVectorColumnSelector makeSingleValueDimensionProcessor(
      final ColumnCapabilities capabilities,
      final SingleValueDimensionVectorSelector selector
  )
  {
    Preconditions.checkArgument(
        capabilities.is(ValueType.STRING),
        "groupBy dimension processors must be STRING typed"
    );
    return new SingleValueStringGroupByVectorColumnSelector(selector);
  }

  @Override
  public GroupByVectorColumnSelector makeMultiValueDimensionProcessor(
      final ColumnCapabilities capabilities,
      final MultiValueDimensionVectorSelector selector
  )
  {
    Preconditions.checkArgument(
        capabilities.is(ValueType.STRING),
        "groupBy dimension processors must be STRING typed"
    );
    throw new UnsupportedOperationException(
        "Vectorized groupBys on multi-value dictionary-encoded dimensions are not yet implemented"
    );
  }

  @Override
  public GroupByVectorColumnSelector makeFloatProcessor(
      final ColumnCapabilities capabilities,
      final VectorValueSelector selector
  )
  {
    if (capabilities.hasNulls().isFalse()) {
      return new FloatGroupByVectorColumnSelector(selector);
    }
    return new NullableFloatGroupByVectorColumnSelector(selector);
  }

  @Override
  public GroupByVectorColumnSelector makeDoubleProcessor(
      final ColumnCapabilities capabilities,
      final VectorValueSelector selector
  )
  {
    if (capabilities.hasNulls().isFalse()) {
      return new DoubleGroupByVectorColumnSelector(selector);
    }
    return new NullableDoubleGroupByVectorColumnSelector(selector);
  }

  @Override
  public GroupByVectorColumnSelector makeLongProcessor(
      final ColumnCapabilities capabilities,
      final VectorValueSelector selector
  )
  {
    if (capabilities.hasNulls().isFalse()) {
      return new LongGroupByVectorColumnSelector(selector);
    }
    return new NullableLongGroupByVectorColumnSelector(selector);
  }

  @Override
  public GroupByVectorColumnSelector makeObjectProcessor(
      final ColumnCapabilities capabilities,
      final VectorObjectSelector selector
  )
  {
    if (capabilities.is(ValueType.STRING)) {
      return new DictionaryBuildingSingleValueStringGroupByVectorColumnSelector(selector);
    }
    return NilGroupByVectorColumnSelector.INSTANCE;
  }

  /**
   * The group by engine vector processor has a more relaxed approach to choosing to use a dictionary encoded string
   * selector over an object selector than some of the other {@link VectorColumnProcessorFactory} implementations.
   *
   * Basically, if a valid dictionary exists, we will use it to group on dictionary ids (so that we can use
   * {@link SingleValueStringGroupByVectorColumnSelector} whenever possible instead of
   * {@link DictionaryBuildingSingleValueStringGroupByVectorColumnSelector}).
   *
   * We do this even for things like virtual columns that have a single string input, because it allows deferring
   * accessing any of the actual string values, which involves at minimum reading utf8 byte values and converting
   * them to string form (if not already cached), and in the case of expressions, computing the expression output for
   * the string input.
   */
  @Override
  public boolean useDictionaryEncodedSelector(ColumnCapabilities capabilities)
  {
    Preconditions.checkArgument(capabilities != null, "Capabilities must not be null");
    Preconditions.checkArgument(capabilities.is(ValueType.STRING), "Must only be called on a STRING column");
    return capabilities.isDictionaryEncoded().isTrue();
  }
}
