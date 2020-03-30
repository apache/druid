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

import org.apache.druid.segment.VectorColumnProcessorFactory;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
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
  public GroupByVectorColumnSelector makeSingleValueDimensionProcessor(final SingleValueDimensionVectorSelector selector)
  {
    return new SingleValueStringGroupByVectorColumnSelector(selector);
  }

  @Override
  public GroupByVectorColumnSelector makeMultiValueDimensionProcessor(final MultiValueDimensionVectorSelector selector)
  {
    throw new UnsupportedOperationException("Multi-value dimensions not yet implemented for vectorized groupBys");
  }

  @Override
  public GroupByVectorColumnSelector makeFloatProcessor(final VectorValueSelector selector)
  {
    return new FloatGroupByVectorColumnSelector(selector);
  }

  @Override
  public GroupByVectorColumnSelector makeDoubleProcessor(final VectorValueSelector selector)
  {
    return new DoubleGroupByVectorColumnSelector(selector);
  }

  @Override
  public GroupByVectorColumnSelector makeLongProcessor(final VectorValueSelector selector)
  {
    return new LongGroupByVectorColumnSelector(selector);
  }
}
