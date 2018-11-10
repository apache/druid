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

package org.apache.druid.query.aggregation;

/**
 */
public class MetricManipulatorFns
{
  private static final MetricManipulationFn IDENTITY_INSTANCE = new MetricManipulationFn()
  {
    @Override
    public Object manipulate(final AggregatorFactory factory, final Object object)
    {
      return object;
    }
  };

  public static MetricManipulationFn identity()
  {
    return IDENTITY_INSTANCE;
  }

  private static final MetricManipulationFn FINALIZING_INSTANCE = new MetricManipulationFn()
  {
    @Override
    public Object manipulate(final AggregatorFactory factory, final Object object)
    {
      return factory.finalizeComputation(object);
    }
  };

  public static MetricManipulationFn finalizing()
  {
    return FINALIZING_INSTANCE;
  }

  private static final MetricManipulationFn DESERIALIZING_INSTANCE = new MetricManipulationFn()
  {
    @Override
    public Object manipulate(final AggregatorFactory factory, final Object object)
    {
      return factory.deserialize(object);
    }
  };

  public static MetricManipulationFn deserializing()
  {
    return DESERIALIZING_INSTANCE;
  }
}
