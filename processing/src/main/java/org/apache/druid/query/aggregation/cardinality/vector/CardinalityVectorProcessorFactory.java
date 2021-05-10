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

package org.apache.druid.query.aggregation.cardinality.vector;

import org.apache.druid.segment.VectorColumnProcessorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

public class CardinalityVectorProcessorFactory implements VectorColumnProcessorFactory<CardinalityVectorProcessor>
{
  public static final CardinalityVectorProcessorFactory INSTANCE = new CardinalityVectorProcessorFactory();

  @Override
  public CardinalityVectorProcessor makeSingleValueDimensionProcessor(
      ColumnCapabilities capabilities,
      SingleValueDimensionVectorSelector selector
  )
  {
    return new SingleValueStringCardinalityVectorProcessor(selector);
  }

  @Override
  public CardinalityVectorProcessor makeMultiValueDimensionProcessor(
      ColumnCapabilities capabilities,
      MultiValueDimensionVectorSelector selector
  )
  {
    return new MultiValueStringCardinalityVectorProcessor(selector);
  }

  @Override
  public CardinalityVectorProcessor makeFloatProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
  {
    return new FloatCardinalityVectorProcessor(selector);
  }

  @Override
  public CardinalityVectorProcessor makeDoubleProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
  {
    return new DoubleCardinalityVectorProcessor(selector);
  }

  @Override
  public CardinalityVectorProcessor makeLongProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
  {
    return new LongCardinalityVectorProcessor(selector);
  }

  @Override
  public CardinalityVectorProcessor makeObjectProcessor(ColumnCapabilities capabilities, VectorObjectSelector selector)
  {
    // Handles string-as-object and complex types.
    return new StringObjectCardinalityVectorProcessor(selector);
  }
}
