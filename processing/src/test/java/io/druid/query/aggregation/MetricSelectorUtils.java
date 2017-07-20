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

package io.druid.query.aggregation;

import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;

public class MetricSelectorUtils
{
  public static ObjectColumnSelector<Float> wrap(final FloatColumnSelector selector)
  {
    return new ObjectColumnSelector<Float>()
    {
      @Override
      public Class<Float> classOfObject()
      {
        return Float.class;
      }

      @Override
      public Float get()
      {
        return selector.get();
      }
    };
  }

  public static ObjectColumnSelector<Double> wrap(final DoubleColumnSelector selector)
  {
    return new ObjectColumnSelector<Double>()
    {
      @Override
      public Class<Double> classOfObject()
      {
        return Double.class;
      }

      @Override
      public Double get()
      {
        return selector.get();
      }
    };
  }
}
