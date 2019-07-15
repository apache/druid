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

package org.apache.druid.query.filter.vector;

import org.apache.druid.query.dimension.VectorColumnStrategizer;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

public class VectorValueMatcherColumnStrategizer implements VectorColumnStrategizer<VectorValueMatcherFactory>
{
  private static final VectorValueMatcherColumnStrategizer INSTANCE = new VectorValueMatcherColumnStrategizer();

  private VectorValueMatcherColumnStrategizer()
  {
    // Singleton.
  }

  public static VectorValueMatcherColumnStrategizer instance()
  {
    return INSTANCE;
  }

  @Override
  public VectorValueMatcherFactory makeSingleValueDimensionStrategy(
      final SingleValueDimensionVectorSelector selector
  )
  {
    return new SingleValueStringVectorValueMatcher(selector);
  }

  @Override
  public VectorValueMatcherFactory makeMultiValueDimensionStrategy(
      final MultiValueDimensionVectorSelector selector
  )
  {
    return new MultiValueStringVectorValueMatcher(selector);
  }

  @Override
  public VectorValueMatcherFactory makeFloatStrategy(final VectorValueSelector selector)
  {
    return new FloatVectorValueMatcher(selector);
  }

  @Override
  public VectorValueMatcherFactory makeDoubleStrategy(final VectorValueSelector selector)
  {
    return new DoubleVectorValueMatcher(selector);
  }

  @Override
  public VectorValueMatcherFactory makeLongStrategy(final VectorValueSelector selector)
  {
    return new LongVectorValueMatcher(selector);
  }
}
