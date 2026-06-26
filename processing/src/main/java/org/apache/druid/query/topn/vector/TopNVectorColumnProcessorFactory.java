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

package org.apache.druid.query.topn.vector;

import com.google.common.base.Preconditions;
import org.apache.druid.segment.VectorColumnProcessorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

/**
 * Creates {@link TopNVectorColumnSelector} instances appropriate for each column type.
 *
 * @see org.apache.druid.query.groupby.epinephelinae.vector.GroupByVectorColumnProcessorFactory the groupBy equivalent
 */
public class TopNVectorColumnProcessorFactory implements VectorColumnProcessorFactory<TopNVectorColumnSelector>
{
  private static final TopNVectorColumnProcessorFactory INSTANCE = new TopNVectorColumnProcessorFactory();

  private TopNVectorColumnProcessorFactory()
  {
    // Singleton.
  }

  public static TopNVectorColumnProcessorFactory instance()
  {
    return INSTANCE;
  }

  @Override
  public TopNVectorColumnSelector makeSingleValueDimensionProcessor(
      final ColumnCapabilities capabilities,
      final SingleValueDimensionVectorSelector selector
  )
  {
    Preconditions.checkArgument(
        capabilities.is(ValueType.STRING),
        "topN dimension processors must be STRING typed"
    );
    return new SingleValueStringTopNVectorColumnSelector(selector);
  }

  @Override
  public TopNVectorColumnSelector makeMultiValueDimensionProcessor(
      final ColumnCapabilities capabilities,
      final MultiValueDimensionVectorSelector selector
  )
  {
    throw new UnsupportedOperationException(
        "Vectorized topN on multi-value dictionary-encoded dimensions is not supported"
    );
  }

  @Override
  public TopNVectorColumnSelector makeFloatProcessor(
      final ColumnCapabilities capabilities,
      final VectorValueSelector selector
  )
  {
    if (capabilities.hasNulls().isFalse()) {
      return new FloatTopNVectorColumnSelector(selector);
    }
    return new NullableFloatTopNVectorColumnSelector(selector);
  }

  @Override
  public TopNVectorColumnSelector makeDoubleProcessor(
      final ColumnCapabilities capabilities,
      final VectorValueSelector selector
  )
  {
    if (capabilities.hasNulls().isFalse()) {
      return new DoubleTopNVectorColumnSelector(selector);
    }
    return new NullableDoubleTopNVectorColumnSelector(selector);
  }

  @Override
  public TopNVectorColumnSelector makeLongProcessor(
      final ColumnCapabilities capabilities,
      final VectorValueSelector selector
  )
  {
    if (capabilities.hasNulls().isFalse()) {
      return new LongTopNVectorColumnSelector(selector);
    }
    return new NullableLongTopNVectorColumnSelector(selector);
  }

  @Override
  public TopNVectorColumnSelector makeArrayProcessor(
      final ColumnCapabilities capabilities,
      final VectorObjectSelector selector
  )
  {
    throw new UnsupportedOperationException(
        "Vectorized topN on ARRAY columns is not supported"
    );
  }

  @Override
  public TopNVectorColumnSelector makeObjectProcessor(
      final ColumnCapabilities capabilities,
      final VectorObjectSelector selector
  )
  {
    if (capabilities.is(ValueType.STRING)) {
      if (capabilities.hasMultipleValues().isTrue()) {
        throw new UnsupportedOperationException(
            "Vectorized topN on multi-value dimensions is not supported"
        );
      }
      return new DictionaryBuildingSingleValueStringTopNVectorColumnSelector(selector);
    }

    throw new UnsupportedOperationException(
        "Vectorized topN is not supported for column type: " + capabilities.asTypeString()
    );
  }

  /**
   * Prefer dictionary-encoded selectors over object selectors when a valid, unique dictionary exists —
   * avoids the cost of resolving the string and building a local dictionary. Non-unique dictionaries
   * (two dict IDs mapping to the same string) route through the object path so
   * {@link DictionaryBuildingSingleValueStringTopNVectorColumnSelector} can collapse aliased IDs by
   * string value, matching {@link org.apache.druid.query.topn.types.StringTopNColumnAggregatesProcessor}'s
   * behavior on the non-vectorized path.
   */
  @Override
  public boolean useDictionaryEncodedSelector(final ColumnCapabilities capabilities)
  {
    Preconditions.checkArgument(capabilities != null, "Capabilities must not be null");
    Preconditions.checkArgument(capabilities.is(ValueType.STRING), "Must only be called on a STRING column");
    return capabilities.isDictionaryEncoded().isTrue() && capabilities.areDictionaryValuesUnique().isTrue();
  }
}
